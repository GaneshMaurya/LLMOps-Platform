#!/usr/bin/env python3
import json
import logging
import threading
import time
from typing import Dict, Any
from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import os


# Configure logging with detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s - [%(filename)s:%(lineno)d]',
    handlers=[
        logging.FileHandler("controller.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("deployment-controller")



app = Flask(__name__)
CORS(app)  # This will enable CORS for all routes

# Configuration - can be overridden through environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
METRICS_TOPIC = os.getenv('METRICS_TOPIC', 'system-metrics')
DEPLOYMENT_ENDPOINT = os.getenv('DEPLOYMENT_ENDPOINT', '/create-vm')
HEALTH_CHECK_TIMEOUT = int(os.getenv('HEALTH_CHECK_TIMEOUT', '60'))  # Reduced timeout for faster checks
SKIP_CONNECTIVITY_TEST = os.getenv('SKIP_CONNECTIVITY_TEST', 'False').lower() == 'true'
MAX_METRIC_AGE_SECONDS = int(os.getenv('MAX_METRIC_AGE_SECONDS', '300'))  # 5 minutes by default
SKIP_CONNECTIVITY_TEST=True

logger.info(f"Starting with: SKIP_CONNECTIVITY_TEST={SKIP_CONNECTIVITY_TEST}, HEALTH_CHECK_TIMEOUT={HEALTH_CHECK_TIMEOUT}s")

class DeploymentController:
    def __init__(self):
        logger.info(f"Initializing deployment controller with: KAFKA={KAFKA_BOOTSTRAP_SERVERS}, TOPIC={METRICS_TOPIC}")
        
        self.laptop_metrics = {}  # Stores system metrics for each laptop
        self.deployment_registry = {}  # Basic registry of deployments (no activity tracking)
        self.lock = threading.Lock()  # Lock for thread safety
        self.laptop_health_cache = {}  # Cache health check results for 30 seconds
        
        # Create Kafka topic if it doesn't exist
        self.create_kafka_topic()
        
        # Configure Kafka consumer
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'deployment-controller-group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
            'fetch.max.bytes': 10485760,    # Match broker max message size
            'max.partition.fetch.bytes': 1048576,  # 1MB per partition
            'session.timeout.ms': 30000,    # 30 seconds
            'heartbeat.interval.ms': 10000  # 10 seconds
        }
        
        # Initialize Kafka consumer and subscribe to metrics topic
        self.metrics_consumer = Consumer(self.consumer_config)
        self.metrics_consumer.subscribe([METRICS_TOPIC])
        logger.info(f"Subscribed to Kafka topic: {METRICS_TOPIC}")
        
        # Start metrics consumer thread
        self.metrics_thread = threading.Thread(target=self.consume_metrics)
        self.metrics_thread.daemon = True
        self.metrics_thread.start()
        logger.info("Started metrics consumer thread")
        
        logger.info("Deployment controller initialization complete")
    
    def create_kafka_topic(self):
        """Create Kafka topic if it doesn't exist"""
        try:
            logger.info(f"Attempting to create Kafka topic: {METRICS_TOPIC}")
            admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
            topic_list = [NewTopic(METRICS_TOPIC, num_partitions=1, replication_factor=1)]
            admin_client.create_topics(topic_list)
            logger.info(f"Successfully created Kafka topic: {METRICS_TOPIC}")
        except Exception as e:
            logger.warning(f"Could not create topic {METRICS_TOPIC}: {str(e)}")
    
    def consume_metrics(self):
        """Consume metrics from Kafka"""
        logger.info("Starting metrics consumer loop")
        while True:
            try:
                # Poll for messages
                msg = self.metrics_consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Error polling Kafka: {msg.error()}")
                    continue
                
                try:
                    # Parse and process the message
                    metric_data = json.loads(msg.value().decode('utf-8'))
                    laptop_id = metric_data.get('laptop_id')
                    
                    if not laptop_id:
                        logger.warning("Received metrics without laptop_id")
                        continue
                    
                    with self.lock:
                        # Extract basic laptop information
                        ip = metric_data.get('ip')
                        port = metric_data.get('port')
                        
                        if not ip or not port:
                            logger.warning(f"Metrics for laptop {laptop_id} missing IP or port")
                            continue
                        
                        # Update laptop metrics - only system-level metrics
                        self.laptop_metrics[laptop_id] = {
                            'ip': ip,
                            'port': port,
                            'cpu': metric_data.get('cpu', {}),
                            'memory': metric_data.get('memory', {}),
                            'disk': metric_data.get('disk', {}),
                            'network': metric_data.get('network', {}),
                            'system': metric_data.get('system', {}),
                            'last_updated': time.time()
                        }
                        
                        # Log a summary of the metrics received
                        cpu_percent = metric_data.get('cpu', {}).get('percent', 0)
                        memory_percent = metric_data.get('memory', {}).get('percent', 0)
                        logger.info(f"Updated metrics for laptop {laptop_id}: CPU: {cpu_percent:.1f}%, Memory: {memory_percent:.1f}%")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding metrics JSON: {str(e)}")
                except Exception as e:
                    logger.error(f"Error processing metrics: {str(e)}", exc_info=True)
            
            except Exception as e:
                logger.error(f"Error in consumer loop: {str(e)}", exc_info=True)
                time.sleep(1)  # Brief pause on error
    
    def _test_connectivity(self, laptop_id, ip, port):
        """Test if the laptop is reachable before selecting it for deployment - with caching"""
        current_time = time.time()
        
        # Check cache first to avoid repeated health checks
        cache_key = f"{laptop_id}:{ip}:{port}"
        if cache_key in self.laptop_health_cache:
            cache_entry = self.laptop_health_cache[cache_key]
            # Cache health check results for 30 seconds
            if current_time - cache_entry['timestamp'] < 30:
                is_healthy = cache_entry['status']
                logger.info(f"Using cached health status for {laptop_id}: {'healthy' if is_healthy else 'unhealthy'}")
                return is_healthy
        
        # If not in cache or cache expired, perform a health check
        try:
            # Use a simple health check endpoint to test connectivity
            health_url = f"http://{ip}:{port}/health"
            logger.info(f"Testing connectivity to laptop {laptop_id} at {health_url}")
            
            response = requests.get(health_url, timeout=HEALTH_CHECK_TIMEOUT)
            
            is_healthy = response.status_code == 200
            
            if is_healthy:
                logger.info(f"Successfully connected to laptop {laptop_id}")
            else:
                logger.warning(f"Received non-200 response from laptop {laptop_id}: {response.status_code}")
            
            # Cache the result
            self.laptop_health_cache[cache_key] = {
                'status': is_healthy,
                'timestamp': current_time
            }
            
            return is_healthy
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to connect to laptop {laptop_id} at {ip}:{port}: {str(e)}")
            
            # Cache the negative result
            self.laptop_health_cache[cache_key] = {
                'status': False,
                'timestamp': current_time
            }
            
            return False
    
    def select_laptop(self, model_id: str, version: str) -> Dict[str, Any]:
        """Select the best laptop for deployment based on system metrics only"""
        logger.info(f"Selecting laptop for model {model_id} version {version}")
        
        with self.lock:
            if not self.laptop_metrics:
                logger.warning("No laptop metrics available for deployment decision")
                return None

            current_time = time.time()
            
            # Filter to only active laptops (metrics received recently)
            active_laptops = {
                laptop_id: metrics for laptop_id, metrics in self.laptop_metrics.items()
                if current_time - metrics.get('last_updated', 0) < MAX_METRIC_AGE_SECONDS
            }
            
            if not active_laptops:
                logger.warning("No laptops with recent metrics available")
                return None
            
            # Skip connectivity test if configured to do so
            if SKIP_CONNECTIVITY_TEST:
                logger.info("Skipping connectivity tests as per configuration")
                reachable_laptops = active_laptops
            else:
                # Filter laptops to only those we can actually connect to
                reachable_laptops = {}
                for laptop_id, metrics in active_laptops.items():
                    ip = metrics.get('ip')
                    port = metrics.get('port')
                    if self._test_connectivity(laptop_id, ip, port):
                        reachable_laptops[laptop_id] = metrics
                    else:
                        logger.warning(f"Excluding laptop {laptop_id} due to connectivity issues")
            
            if not reachable_laptops:
                logger.warning("No reachable laptops available")
                return None
            
            # Log available laptops for selection
            logger.info(f"Available laptops for selection: {len(reachable_laptops)}")
            for laptop_id, metrics in reachable_laptops.items():
                cpu = metrics.get('cpu', {}).get('percent', 0)
                memory = metrics.get('memory', {}).get('percent', 0)
                ip = metrics.get('ip', 'unknown')
                port = metrics.get('port', 0)
                logger.info(f"  - Laptop {laptop_id}: CPU {cpu:.1f}%, Memory {memory:.1f}%, IP:{ip}, Port:{port}")
            
            # Find the best laptop (lowest weighted score of CPU and memory usage)
            best_laptop_id = None
            best_score = float('inf')
            
            for laptop_id, metrics in reachable_laptops.items():
                cpu_percent = metrics.get('cpu', {}).get('percent', 100)
                memory_percent = metrics.get('memory', {}).get('percent', 100)
                
                # Calculate deployment score (lower is better)
                # 70% weight to CPU, 30% to memory
                score = (0.7 * cpu_percent) + (0.3 * memory_percent)
                
                if score < best_score:
                    best_score = score
                    best_laptop_id = laptop_id
            
            if not best_laptop_id:
                logger.warning("Failed to select best laptop")
                return None
                
            logger.info(f"Selected laptop {best_laptop_id} with score {best_score:.2f} for model {model_id}")
            return {
                'laptop_id': best_laptop_id,
                'ip': reachable_laptops[best_laptop_id]['ip'],
                'port': reachable_laptops[best_laptop_id]['port'],
                'score': best_score
            }
    
    def deploy_model(self, model_id: str, version: str) -> Dict[str, Any]:
        """Deploy model to the best available laptop"""
        start_time = time.time()
        logger.info(f"STEP 1: Starting deployment for model {model_id} version {version}")
        
        # Select the best laptop for deployment
        logger.info(f"STEP 2: Selecting best laptop")
        selected_laptop = self.select_laptop(model_id, version)
        if not selected_laptop:
            logger.error(f"STEP 2 FAILED: No suitable laptop found for model {model_id} version {version}")
            return {
                'success': False,
                'error': 'No suitable deployment target found'
            }
        
        laptop_id = selected_laptop['laptop_id']
        ip = selected_laptop['ip']
        port = selected_laptop['port']
        
        logger.info(f"STEP 3: Selected laptop {laptop_id} ({ip}:{port}) for deployment")
        
        try:
            # Call the agent's deployment endpoint
            deployment_url = f"http://{ip}:{port}{DEPLOYMENT_ENDPOINT}"
            logger.info(f"STEP 4: Sending deployment request to {deployment_url}")
            
            response = requests.post(
                deployment_url,
                json={'model_id': model_id, 'version': version},
                timeout=60  # Reduced timeout for faster response
            )
            
            # Process the response
            if response.status_code == 200:
                response_data = response.json()
                deployment_id = response_data.get('deployment_id', 'unknown')
                
                logger.info(f"STEP 5: Deployment successful. Model {model_id} version {version} deployed to {laptop_id} with ID {deployment_id}")
                
                # Register the deployment (basic info only)
                with self.lock:
                    self.deployment_registry[deployment_id] = {
                        'laptop_id': laptop_id,
                        'model_id': model_id,
                        'version': version,
                        'deployment_time': time.time()
                    }
                
                # Include access_url in the response
                end_time = time.time()
                logger.info(f"STEP 6: Deployment completed. Total time: {end_time - start_time:.2f} seconds")
                
                return {
                    'success': True,
                    'laptop_id': laptop_id,
                    'deployment_id': deployment_id,
                    'access_url': response_data.get('access_url'),
                    'deploy_time_seconds': end_time - start_time
                }
            else:
                logger.error(f"STEP 5 FAILED: Deployment returned HTTP {response.status_code}: {response.text}")
                return {
                    'success': False,
                    'error': f"Deployment failed: {response.status_code}"
                }
                
        except requests.exceptions.RequestException as e:
            logger.error(f"STEP 4 FAILED: Network error deploying model: {str(e)}")
            return {
                'success': False,
                'error': f"Network error: {str(e)}"
            }
        except Exception as e:
            logger.error(f"Unexpected error deploying model: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }
    
    def stop_deployment(self, deployment_id):
        """Stop a deployment by ID"""
        logger.info(f"STEP 1: Stopping deployment {deployment_id}")
        
        with self.lock:
            if deployment_id not in self.deployment_registry:
                logger.warning(f"STEP 1 FAILED: Deployment {deployment_id} not found")
                return False, "Deployment not found"
            
            deployment_info = self.deployment_registry[deployment_id]
            laptop_id = deployment_info.get('laptop_id')
            
            if not laptop_id or laptop_id not in self.laptop_metrics:
                logger.error(f"STEP 2 FAILED: Laptop {laptop_id} not found in metrics")
                return False, f"Laptop {laptop_id} not found"
            
            ip = self.laptop_metrics[laptop_id]['ip']
            port = self.laptop_metrics[laptop_id]['port']
            
            logger.info(f"STEP 2: Found deployment on laptop {laptop_id} ({ip}:{port})")
        
        # Request termination via agent API
        try:
            termination_url = f"http://{ip}:{port}/stop-vm/{deployment_id}"
            logger.info(f"STEP 3: Sending stop request to {termination_url}")
            
            response = requests.post(
                termination_url,
                timeout=30  # Reduced timeout
            )
            
            if response.status_code == 200:
                logger.info(f"STEP 4: Successfully stopped deployment {deployment_id}")
                
                # Remove from registry
                with self.lock:
                    if deployment_id in self.deployment_registry:
                        del self.deployment_registry[deployment_id]
                
                return True, f"Deployment {deployment_id} stopped successfully"
            else:
                error_msg = f"STEP 4 FAILED: Received HTTP {response.status_code}: {response.text}"
                logger.error(error_msg)
                return False, error_msg
                
        except requests.exceptions.RequestException as e:
            error_msg = f"STEP 3 FAILED: Network error: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
    
    def get_deployments(self):
        """Get a list of all deployments"""
        with self.lock:
            current_time = time.time()
            deployments = {
                deployment_id: {
                    'model_id': info.get('model_id', 'unknown'),
                    'version': info.get('version', 'unknown'),
                    'laptop_id': info.get('laptop_id'),
                    'deployment_time': info.get('deployment_time'),
                    'uptime': current_time - info.get('deployment_time', current_time)
                }
                for deployment_id, info in self.deployment_registry.items()
            }
            return deployments

# Create controller instance
controller = DeploymentController()

@app.route('/controller/deploy', methods=['POST'])
def deploy_model():
    """Endpoint for deploying a model"""
    logger.info(f"Received deployment request: {request.json}")
    
    data = request.json
    
    if not data or 'model_id' not in data:
        logger.warning("Received deploy request without model_id")
        return jsonify({
            'success': False,
            'error': 'Missing model_id in request'
        }), 400
    
    model_id = data['model_id']
    version = data.get('version', 'latest')
    logger.info(f"Processing deployment request for model {model_id} version {version}")
    
    result = controller.deploy_model(model_id, version)
    
    if result.get('success', False):
        logger.info(f"Deployment successful: {result}")
        return jsonify(result), 200
    else:
        logger.error(f"Deployment failed: {result}")
        return jsonify(result), 500

@app.route('/controller/stop', methods=['POST'])
def stop_deployment():
    """Endpoint for manually stopping a deployment"""
    logger.info(f"Received stop request: {request.json}")
    
    data = request.json
    
    if not data or 'deployment_id' not in data:
        logger.warning("Received stop request without deployment_id")
        return jsonify({
            'success': False,
            'error': 'Missing deployment_id in request'
        }), 400
    
    deployment_id = data['deployment_id']
    success, message = controller.stop_deployment(deployment_id)
    
    if success:
        logger.info(f"Deployment stop successful: {deployment_id}")
        return jsonify({
            'success': True,
            'message': message
        }), 200
    else:
        logger.error(f"Deployment stop failed: {deployment_id}")
        return jsonify({
            'success': False,
            'error': message
        }), 500

@app.route('/controller/status', methods=['GET'])
def get_status():
    """Endpoint for getting controller status"""
    logger.debug("Received status request")
    
    with controller.lock:
        active_laptops = {}
        current_time = time.time()
        
        for laptop_id, metrics in controller.laptop_metrics.items():
            if current_time - metrics.get('last_updated', 0) < MAX_METRIC_AGE_SECONDS:
                active_laptops[laptop_id] = {
                    'cpu_percent': metrics.get('cpu', {}).get('percent', 0),
                    'memory_percent': metrics.get('memory', {}).get('percent', 0),
                    'last_updated': metrics.get('last_updated', 0),
                    'ip': metrics.get('ip', 'unknown'),
                    'port': metrics.get('port', 0)
                }
        
        status = {
            'active_laptops': len(active_laptops),
            'laptops': active_laptops,
            'ready': len(active_laptops) > 0,
            'time': current_time
        }
        
        logger.info(f"Status response: {len(active_laptops)} active laptops, ready={len(active_laptops) > 0}")
        return jsonify(status), 200

@app.route('/controller/deployments', methods=['GET'])
def get_deployments():
    """Endpoint for getting information about all deployments"""
    logger.debug("Received deployments listing request")
    
    deployments = controller.get_deployments()
    
    response = {
        'deployment_count': len(deployments),
        'deployments': deployments,
        'time': time.time()
    }
    
    logger.info(f"Deployments response: {len(deployments)} deployments")
    return jsonify(response), 200

# Option to skip health checks via HTTP request
@app.route('/controller/config', methods=['POST'])
def update_config():
    """Update controller configuration"""
    global SKIP_CONNECTIVITY_TEST, HEALTH_CHECK_TIMEOUT
    
    data = request.json or {}
    updated = []
    
    if 'skip_connectivity_test' in data:
        SKIP_CONNECTIVITY_TEST = data['skip_connectivity_test']
        updated.append(f"SKIP_CONNECTIVITY_TEST={SKIP_CONNECTIVITY_TEST}")
    
    if 'health_check_timeout' in data:
        HEALTH_CHECK_TIMEOUT = int(data['health_check_timeout'])
        updated.append(f"HEALTH_CHECK_TIMEOUT={HEALTH_CHECK_TIMEOUT}")
        
    logger.info(f"Updated configuration: {', '.join(updated)}")
    
    return jsonify({
        'success': True,
        'config': {
            'skip_connectivity_test': SKIP_CONNECTIVITY_TEST,
            'health_check_timeout': HEALTH_CHECK_TIMEOUT
        },
        'message': f"Configuration updated: {', '.join(updated)}"
    }), 200

if __name__ == '__main__':
    logger.info("Starting controller application")
    app.run(host='0.0.0.0', port=8090)