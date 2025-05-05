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
from dotenv import load_dotenv
import socket

ENV_FILE='/exports/applications/.env'

load_dotenv(ENV_FILE)

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

service_registry_ip=os.environ.get('service_registry_ip','192.168.211.61')
service_registry_port=os.environ.get('service_registry_port','9090')
KAFKA_HOST_IP=os.environ.get('life_cycle_manager_ip','192.168.227.62')

app = Flask(__name__)
CORS(app) 

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', f'{KAFKA_HOST_IP}:29092')
METRICS_TOPIC = os.getenv('METRICS_TOPIC', 'system-metrics')
DEPLOYMENT_ENDPOINT = os.getenv('DEPLOYMENT_ENDPOINT', '/create-vm')
HEALTH_CHECK_TIMEOUT = int(os.getenv('HEALTH_CHECK_TIMEOUT', '60'))
SKIP_CONNECTIVITY_TEST = os.getenv('SKIP_CONNECTIVITY_TEST', 'False').lower() == 'true'
MAX_METRIC_AGE_SECONDS = int(os.getenv('MAX_METRIC_AGE_SECONDS', '300'))
SKIP_CONNECTIVITY_TEST=True
CONTROLLER_PORT='8090'

# Add these environment variables for Caddy integration
CADDY_API_URL = os.getenv('CADDY_API_URL', 'http://localhost:2019')
ENABLE_PUBLIC_URLS = os.getenv('ENABLE_PUBLIC_URLS', 'True').lower() == 'true'
PUBLIC_URL_BASE = os.getenv('PUBLIC_URL_BASE', 'http://localhost')

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
        """Create Kafka topic with proper verification"""
        try:
            logger.info(f"Checking for Kafka topic: {METRICS_TOPIC}")
            admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
            
            # First check if the topic already exists
            metadata = admin_client.list_topics(timeout=10)
            if METRICS_TOPIC in metadata.topics:
                logger.info(f"Topic {METRICS_TOPIC} already exists")
                return True
                
            # Topic doesn't exist, create it
            logger.info(f"Creating Kafka topic: {METRICS_TOPIC}")
            topic_list = [NewTopic(METRICS_TOPIC, num_partitions=1, replication_factor=1)]
            futures = admin_client.create_topics(topic_list)
            
            # Wait for topic creation to complete
            for topic, future in futures.items():
                try:
                    future.result(timeout=30)  # Wait up to 30 seconds for creation
                    logger.info(f"Successfully created Kafka topic: {topic}")
                except Exception as e:
                    logger.warning(f"Error creating topic {topic}: {str(e)}")
                    # Don't return False here - topic might still be created
            
            # Verify the topic exists now
            metadata = admin_client.list_topics(timeout=10)
            if METRICS_TOPIC in metadata.topics:
                logger.info(f"Verified topic {METRICS_TOPIC} exists")
                return True
            else:
                logger.warning(f"Topic {METRICS_TOPIC} not found after creation attempt")
                return False
                
        except Exception as e:
            logger.warning(f"Could not create topic {METRICS_TOPIC}: {str(e)}")
            return False
    
    def consume_metrics(self):
        """Consume metrics from Kafka with robust error handling"""
        logger.info("Starting metrics consumer loop")
        
        # Add initial delay to give time for Kafka to register the topic
        time.sleep(5)
        
        # Track consecutive errors to implement backoff strategy
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        while True:
            try:
                # Check if topic exists before attempting to poll
                if consecutive_errors >= 3:
                    # After 3 consecutive errors, verify topic exists
                    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
                    metadata = admin_client.list_topics(timeout=10)
                    
                    if METRICS_TOPIC not in metadata.topics:
                        logger.warning(f"Topic {METRICS_TOPIC} not found. Attempting to create it.")
                        self.create_kafka_topic()
                        time.sleep(2)  # Give time for topic to register
                
                # Poll for messages
                msg = self.metrics_consumer.poll(1.0)
                
                if msg is None:
                    consecutive_errors = 0  # Reset error counter on successful poll
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                        consecutive_errors = 0  # This is normal behavior
                    else:
                        logger.error(f"Error polling Kafka: {msg.error()}")
                        consecutive_errors += 1
                        
                        # If we get a topic error, try to recreate it
                        if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                            logger.warning("Topic not found. Attempting to create it.")
                            self.create_kafka_topic()
                            
                            # Resubscribe to the topic
                            logger.info("Resubscribing to the topic")
                            self.metrics_consumer.unsubscribe()
                            time.sleep(1)
                            self.metrics_consumer.subscribe([METRICS_TOPIC])
                            time.sleep(2)  # Give time for subscription to take effect
                    
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error(f"Too many consecutive errors ({consecutive_errors}). Sleeping before retry.")
                        time.sleep(30)  # Longer sleep after many errors
                        consecutive_errors = 0  # Reset after sleep
                    
                    continue
                
                # Process the message (successful case)
                try:
                    # Reset error counter on successful processing
                    consecutive_errors = 0
                    
                    # Parse and process the message
                    metric_data = json.loads(msg.value().decode('utf-8'))
                    system_data= metric_data.get('system',{})
                    laptop_id=system_data.get('hostname',{})
                    
                    if not laptop_id:
                        logger.warning("Received metrics without laptop_id")
                        continue
                    
                    with self.lock:
                        # Extract basic laptop information
                        ip = metric_data.get('ip')
                        port = metric_data.get('port',8091)
                        
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
                consecutive_errors += 1
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
    
    def update_caddy_route(self, deployment_id, internal_url, action="add"):
        """
        Directly update Caddy's configuration to add or remove a route
        
        Args:
            deployment_id (str): Unique identifier for the deployment
            internal_url (str): Internal URL where the model is running (ip:port)
            action (str): Either "add" or "remove"
            
        Returns:
            tuple: (success (bool), result (str))
        """
        
        route_id = f"route-{deployment_id}"
        
        try:
            if action == "add":
                logger.info(f"Adding Caddy route for deployment {deployment_id} to {internal_url}")
                
                # Prepare the route configuration
                config_patch = {
                    "@id": route_id,
                    "handle": [
                        {
                            "handler": "reverse_proxy",
                            "upstreams": [{"dial": internal_url}]
                        }
                    ],
                    "match": [{"path": [f"/{deployment_id}/*", f"/{deployment_id}"]}]
                }
                
                # Use Caddy's API to add the route
                config_url = f"{CADDY_API_URL}/config/apps/http/servers/srv0/routes"
                response = requests.post(config_url, json=config_patch, timeout=10)
                
                if response.status_code in (200, 201, 202):
                    return True, f"Route added for deployment {deployment_id}"
                else:
                    logger.error(f"Failed to add Caddy route: {response.status_code} - {response.text}")
                    return False, f"Failed to add route: {response.text}"
                    
            elif action == "remove":
                logger.info(f"Removing Caddy route for deployment {deployment_id}")
                
                # Use Caddy's API to remove the route
                config_url = f"{CADDY_API_URL}/config/apps/http/servers/srv0/routes/{route_id}"
                response = requests.delete(config_url, timeout=10)
                
                if response.status_code in (200, 204):
                    return True, f"Route removed for deployment {deployment_id}"
                else:
                    logger.error(f"Failed to remove Caddy route: {response.status_code} - {response.text}")
                    return False, f"Failed to remove route: {response.text}"
                    
            else:
                return False, f"Invalid action: {action}"
                
        except Exception as e:
            logger.error(f"Error updating Caddy route: {str(e)}", exc_info=True)
            return False, f"Error: {str(e)}"
    
    def deploy_model(self, model_id: str, version: str) -> Dict[str, Any]:
        """Deploy model to the best available laptop and create public URL"""
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
                timeout=660  # Reduced timeout for faster response
            )
            
            # Process the response
            if response.status_code == 200:
                response_data = response.json()
                deployment_id = response_data.get('deployment_id', 'unknown')
                model_access_url = response_data.get('access_url')
                public_url = None
                
                logger.info(f"STEP 5: Deployment successful. Model {model_id} deployed with ID {deployment_id}")
                
                # Register the deployment
                with self.lock:
                    self.deployment_registry[deployment_id] = {
                        'laptop_id': laptop_id,
                        'model_id': model_id,
                        'version': version,
                        'deployment_time': time.time(),
                        'internal_url': model_access_url
                    }
                
                # STEP 6: Create public URL via Caddy if enabled
                if ENABLE_PUBLIC_URLS and model_access_url:
                    try:
                        logger.info(f"STEP 6: Creating public URL for deployment {deployment_id}")
                        
                        success, message = self.update_caddy_route(deployment_id, model_access_url, "add")
                        if success:
                            public_url = f"{PUBLIC_URL_BASE}/{deployment_id}"
                            
                            # Update deployment registry with public URL
                            with self.lock:
                                if deployment_id in self.deployment_registry:
                                    self.deployment_registry[deployment_id]['public_url'] = public_url
                            
                            logger.info(f"STEP 6: Public URL created: {public_url}")
                        else:
                            logger.warning(f"STEP 6: Failed to create public URL: {message}")
                    except Exception as e:
                        logger.error(f"STEP 6: Error creating public URL: {str(e)}")
                else:
                    logger.info("STEP 6: Public URL creation disabled or no model access URL available")
                
                end_time = time.time()
                logger.info(f"Deployment completed. Total time: {end_time - start_time:.2f} seconds")
                
                return {
                    'success': True,
                    'laptop_id': laptop_id,
                    'deployment_id': deployment_id,
                    'access_url': model_access_url,
                    'public_url': public_url,
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
        """Stop a deployment by ID and remove its public URL"""
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
                timeout=30
            )
            
            if response.status_code == 200:
                logger.info(f"STEP 4: Successfully stopped deployment {deployment_id}")
                
                # STEP 5: Remove public URL if enabled
                if ENABLE_PUBLIC_URLS:
                    try:
                        logger.info(f"STEP 5: Removing public URL for deployment {deployment_id}")
                        
                        success, message = self.update_caddy_route(deployment_id, "", "remove")
                        if success:
                            logger.info(f"STEP 5: Public URL removed for deployment {deployment_id}")
                        else:
                            logger.warning(f"STEP 5: Failed to remove public URL: {message}")
                    except Exception as e:
                        logger.error(f"STEP 5: Error removing public URL: {str(e)}")
                
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
                    'uptime': current_time - info.get('deployment_time', current_time),
                    'internal_url': info.get('internal_url'),
                    'public_url': info.get('public_url')  # Include public URL in deployments info
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

@app.route('/health',methods=['GET'])
def health():
    return jsonify({"status":"healthy"}), 200

# Option to skip health checks via HTTP request
@app.route('/controller/config', methods=['POST'])
def update_config():
    """Update controller configuration"""
    global SKIP_CONNECTIVITY_TEST, HEALTH_CHECK_TIMEOUT, ENABLE_PUBLIC_URLS
    
    data = request.json or {}
    updated = []
    
    if 'skip_connectivity_test' in data:
        SKIP_CONNECTIVITY_TEST = data['skip_connectivity_test']
        updated.append(f"SKIP_CONNECTIVITY_TEST={SKIP_CONNECTIVITY_TEST}")
    
    if 'health_check_timeout' in data:
        HEALTH_CHECK_TIMEOUT = int(data['health_check_timeout'])
        updated.append(f"HEALTH_CHECK_TIMEOUT={HEALTH_CHECK_TIMEOUT}")
    
    if 'enable_public_urls' in data:
        ENABLE_PUBLIC_URLS = data['enable_public_urls']
        updated.append(f"ENABLE_PUBLIC_URLS={ENABLE_PUBLIC_URLS}")
        
    logger.info(f"Updated configuration: {', '.join(updated)}")
    
    return jsonify({
        'success': True,
        'config': {
            'skip_connectivity_test': SKIP_CONNECTIVITY_TEST,
            'health_check_timeout': HEALTH_CHECK_TIMEOUT,
            'enable_public_urls': ENABLE_PUBLIC_URLS
        },
        'message': f"Configuration updated: {', '.join(updated)}"
    }), 200

# New endpoint to test Caddy connectivity
@app.route('/controller/test-caddy', methods=['GET'])
def test_caddy():
    """Test Caddy connectivity and configuration"""
    try:
        # Try to connect to Caddy Admin API
        test_url = f"{CADDY_API_URL}/config/"
        response = requests.get(test_url, timeout=5)
        
        if response.status_code == 200:
            return jsonify({
                'success': True,
                'caddy_status': 'connected',
                'message': 'Successfully connected to Caddy Admin API',
                'caddy_api_url': CADDY_API_URL,
                'public_url_base': PUBLIC_URL_BASE,
                'enable_public_urls': ENABLE_PUBLIC_URLS
            }), 200
        else:
            return jsonify({
                'success': False,
                'caddy_status': 'error',
                'message': f'Caddy responded with status code {response.status_code}',
                'response': response.text
            }), 500
    except Exception as e:
        return jsonify({
            'success': False,
            'caddy_status': 'error',
            'message': f'Error connecting to Caddy: {str(e)}',
            'caddy_api_url': CADDY_API_URL
        }), 500

def getMyIP():
    local_ip=None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
    except:
        s.close()
    return local_ip

def registerService():
    logger.info("Registering With Service Registry")
    ControllerIP=getMyIP()
    requestURL=f'http://{service_registry_ip}:{service_registry_port}/service-registry/register'
    body={'name':'controller','ip':ControllerIP,'port':f'{CONTROLLER_PORT}'}
    logger.info(f"Sending registeration request at {requestURL} with body {body}")
    response=requests.post(requestURL, json=body)
    if response.status_code==400:
        logger.error("Service Registration Failed")
        exit()
    else:
        logger.info("Registration Successful")
        return
    
if __name__ == '__main__':
    logger.info("Starting controller application")
    registerService()
    app.run(host='0.0.0.0', port=CONTROLLER_PORT)
