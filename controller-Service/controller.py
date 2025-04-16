#!/usr/bin/env python3
import json
import logging
import threading
import time
from typing import Dict, Any
from flask import Flask, request, jsonify
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

# Configuration - can be overridden through environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
METRICS_TOPIC = os.getenv('METRICS_TOPIC', 'system-metrics')
DEPLOYMENT_ENDPOINT = os.getenv('DEPLOYMENT_ENDPOINT', '/create-vm')
CONTAINER_INACTIVITY_THRESHOLD = int(os.getenv('CONTAINER_INACTIVITY_THRESHOLD', '120'))  # 2 minutes
CONTAINER_HEALTH_CHECK_INTERVAL = int(os.getenv('CONTAINER_HEALTH_CHECK_INTERVAL', '30'))  # 30 seconds

class DeploymentController:
    def __init__(self):
        logger.info(f"Initializing deployment controller with: KAFKA={KAFKA_BOOTSTRAP_SERVERS}, TOPIC={METRICS_TOPIC}")
        
        self.laptop_metrics = {}  # Stores system metrics for each laptop
        self.deployment_registry = {}  # Tracks all container deployments
        self.request_rates = {}  # Tracks request activity for each container
        self.lock = threading.Lock()  # Lock for thread safety
        
        # Create Kafka topic if it doesn't exist
        self.create_kafka_topic()
        
        # Configure Kafka consumer
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'deployment-controller-group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000
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
        
        # Start container health monitoring thread
        self.health_thread = threading.Thread(target=self.monitor_container_health)
        self.health_thread.daemon = True
        self.health_thread.start()
        logger.info("Started container health monitoring thread")
        
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
                        
                        # Update laptop metrics
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
                        print(self.laptop_metrics[laptop_id])
                        
                        # Process container metrics
                        containers = metric_data.get('containers', {})
                        current_time = time.time()
                        
                        for deployment_id, container_metrics in containers.items():
                            # Update container registry
                            if deployment_id not in self.deployment_registry:
                                logger.info(f"New container detected: {deployment_id} on laptop {laptop_id}")
                                self.deployment_registry[deployment_id] = {
                                    'laptop_id': laptop_id,
                                    'model_id': container_metrics.get('model_id', 'unknown'),
                                    'status': container_metrics.get('status', 'running'),
                                    'last_update': current_time,
                                    'first_seen': current_time
                                }
                            else:
                                self.deployment_registry[deployment_id].update({
                                    'laptop_id': laptop_id,  # Update in case container moved
                                    'status': container_metrics.get('status', 'running'),
                                    'last_update': current_time
                                })
                            
                            # Track container activity and request rates
                            request_rate = container_metrics.get('request_rate', 0)
                            total_requests = container_metrics.get('total_requests', 0)
                            last_request_time = container_metrics.get('last_request_time')
                            
                            if deployment_id not in self.request_rates:
                                self.request_rates[deployment_id] = {
                                    'request_count': total_requests,
                                    'current_rate': request_rate,
                                    'last_request_time': last_request_time or (current_time if request_rate > 0 else None),
                                    'first_tracked': current_time
                                }
                            else:
                                # Update request rate data
                                existing_data = self.request_rates[deployment_id]
                                
                                # Only update total request count if it's higher than what we have
                                if total_requests > existing_data.get('request_count', 0):
                                    self.request_rates[deployment_id]['request_count'] = total_requests
                                
                                self.request_rates[deployment_id]['current_rate'] = request_rate
                                
                                # Update last request time if we have activity
                                if request_rate > 0:
                                    self.request_rates[deployment_id]['last_request_time'] = current_time
                                elif last_request_time and (existing_data.get('last_request_time') is None or 
                                                      last_request_time > existing_data.get('last_request_time', 0)):
                                    self.request_rates[deployment_id]['last_request_time'] = last_request_time
                            
                            # Log container activity for containers with significant inactivity
                            if deployment_id in self.request_rates and self.request_rates[deployment_id]['last_request_time']:
                                inactive_time = current_time - self.request_rates[deployment_id]['last_request_time']
                                if inactive_time > 60:  # Log if inactive more than 1 minute
                                    logger.info(f"Container {deployment_id} on {laptop_id} has been inactive for {inactive_time:.1f} seconds")
                        
                        # Log a summary of the metrics received
                        cpu_percent = metric_data.get('cpu', {}).get('percent', 0)
                        memory_percent = metric_data.get('memory', {}).get('percent', 0)
                        logger.info(f"Updated metrics for laptop {laptop_id}: CPU: {cpu_percent:.1f}%, Memory: {memory_percent:.1f}%, Containers: {len(containers)}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding metrics JSON: {str(e)}")
                except Exception as e:
                    logger.error(f"Error processing metrics: {str(e)}", exc_info=True)
            
            except Exception as e:
                logger.error(f"Error in consumer loop: {str(e)}", exc_info=True)
                time.sleep(1)  # Brief pause on error
    
    def monitor_container_health(self):
        """Monitor container health and manage container lifecycle"""
        logger.info(f"Starting container health monitoring thread with inactivity threshold: {CONTAINER_INACTIVITY_THRESHOLD}s")
        while True:
            try:
                current_time = time.time()
                containers_to_check = []
                
                with self.lock:
                    # Build list of containers to check for inactivity
                    for deployment_id, deployment_info in list(self.deployment_registry.items()):
                        laptop_id = deployment_info.get('laptop_id')
                        if not laptop_id or laptop_id not in self.laptop_metrics:
                            continue
                            
                        # Check container activity
                        if deployment_id in self.request_rates:
                            last_request_time = self.request_rates[deployment_id].get('last_request_time')
                            
                            # Only consider containers that have been tracked for a minimum period
                            first_tracked = self.request_rates[deployment_id].get('first_tracked', current_time)
                            tracking_duration = current_time - first_tracked
                            
                            if last_request_time and (current_time - last_request_time) > CONTAINER_INACTIVITY_THRESHOLD:
                                logger.info(f"Container {deployment_id} marked for inactivity check: inactive for {current_time - last_request_time:.1f} seconds")
                                containers_to_check.append({
                                    'deployment_id': deployment_id,
                                    'laptop_id': laptop_id,
                                    'inactive_time': current_time - last_request_time,
                                    'model_id': deployment_info.get('model_id', 'unknown')
                                })
                
                # Process containers outside of lock to avoid holding it too long
                for container in containers_to_check:
                    logger.warning(f"Container {container['deployment_id']} has been inactive for {container['inactive_time']:.1f} seconds. Terminating...")
                    self._terminate_container(container['deployment_id'], container['laptop_id'])
                
                # Sleep before next check
                time.sleep(CONTAINER_HEALTH_CHECK_INTERVAL)
            except Exception as e:
                logger.error(f"Error in container health monitoring: {str(e)}", exc_info=True)
                time.sleep(CONTAINER_HEALTH_CHECK_INTERVAL)
    
    def _terminate_container(self, deployment_id, laptop_id):
        """Terminate an inactive container"""
        try:
            logger.info(f"Attempting to terminate container {deployment_id} on laptop {laptop_id}")
            
            # Verify laptop is still available
            with self.lock:
                if laptop_id not in self.laptop_metrics:
                    logger.error(f"Cannot terminate container {deployment_id}: laptop {laptop_id} not found in metrics")
                    return False
                
                ip = self.laptop_metrics[laptop_id]['ip']
                port = self.laptop_metrics[laptop_id]['port']
            
            # Request termination via agent API
            termination_url = f"http://{ip}:{port}/stop-vm/{deployment_id}"
            logger.info(f"Calling termination endpoint at {termination_url}")
            
            response = requests.post(
                termination_url,
                timeout=120
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully terminated inactive container {deployment_id}")
                
                # Remove from tracking
                with self.lock:
                    if deployment_id in self.deployment_registry:
                        del self.deployment_registry[deployment_id]
                    
                    if deployment_id in self.request_rates:
                        del self.request_rates[deployment_id]
                
                return True
            else:
                logger.error(f"Failed to terminate container {deployment_id}: {response.status_code} - {response.text}")
                return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error terminating container {deployment_id}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error terminating container {deployment_id}: {str(e)}", exc_info=True)
            return False
    
    def select_laptop(self, model_id: str,version: str) -> Dict[str, Any]:
        """Select the best laptop for deployment based on metrics"""
        logger.info(f"Selecting laptop for model {model_id}-{version}")
        
        with self.lock:
            if not self.laptop_metrics:
                logger.warning("No laptop metrics available for deployment decision")
                return None

            current_time = time.time()
            
            # Filter to only active laptops (metrics received in the last minute)
            active_laptops = {
                laptop_id: metrics for laptop_id, metrics in self.laptop_metrics.items()
                # if current_time - metrics.get('last_updated', 0) < 60
            }
            
            if not active_laptops:
                logger.warning("No laptops with recent metrics available")
                return None
            
            # Log available laptops for selection
            logger.info(f"Available laptops for selection: {len(active_laptops)}")
            for laptop_id, metrics in active_laptops.items():
                cpu = metrics.get('cpu', {}).get('percent', 0)
                memory = metrics.get('memory', {}).get('percent', 0)
                logger.info(f"  - Laptop {laptop_id}: CPU {cpu:.1f}%, Memory {memory:.1f}%")
            
            # Find the best laptop (lowest weighted score of CPU and memory usage)
            best_laptop_id = None
            best_score = float('inf')
            
            for laptop_id, metrics in active_laptops.items():
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
                'ip': self.laptop_metrics[best_laptop_id]['ip'],
                'port': self.laptop_metrics[best_laptop_id]['port'],
                'score': best_score
            }
    
    def deploy_model(self, model_id: str,version: str) -> Dict[str, Any]:
        """Deploy model to the best available laptop"""
        logger.info(f"Received deployment request for model {model_id}")
        
        # Select the best laptop for deployment
        selected_laptop = self.select_laptop(model_id,version)
        if not selected_laptop:
            logger.error(f"No suitable laptop found for model {model_id}")
            return {
                'success': False,
                'error': 'No suitable deployment target found'
            }
        
        laptop_id = selected_laptop['laptop_id']
        ip = selected_laptop['ip']
        port = selected_laptop['port']
        
        try:
            # Call the agent's deployment endpoint
            deployment_url = f"http://{ip}:{port}{DEPLOYMENT_ENDPOINT}"
            logger.info(f"Deploying model {model_id}-{version} to laptop {laptop_id} at {deployment_url}")
            
            response = requests.post(
                deployment_url,
                json={'model_id': model_id,'version':version},
                timeout=120
            )
            
            # Process the response
            if response.status_code == 200:
                response_data = response.json()
                deployment_id = response_data.get('deployment_id', 'unknown')
                
                logger.info(f"Successfully deployed model {model_id} to laptop {laptop_id} with deployment ID {deployment_id}")
                
                # Register the deployment
                with self.lock:
                    self.deployment_registry[deployment_id] = {
                        'laptop_id': laptop_id,
                        'model_id': model_id,
                        'status': 'RUNNING',
                        'last_update': time.time(),
                        'deployment_time': time.time()
                    }
                    
                    # Initialize request rate tracking
                    self.request_rates[deployment_id] = {
                        'request_count': 0,
                        'last_request_time': None,
                        'current_rate': 0,
                        'first_tracked': time.time()
                    }
                
                # Include access_url in the response
                return {
                    'success': True,
                    'laptop_id': laptop_id,
                    'deployment_id': deployment_id,
                    'access_url': response_data.get('access_url')
                }
            else:
                logger.error(f"Failed to deploy model {model_id}: {response.status_code} - {response.text}")
                return {
                    'success': False,
                    'error': f"Deployment failed: {response.status_code}"
                }
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error deploying model {model_id}: {str(e)}")
            return {
                'success': False,
                'error': f"Network error: {str(e)}"
            }
        except Exception as e:
            logger.error(f"Error deploying model {model_id} to laptop {laptop_id}: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_container_stats(self):
        """Get statistics for all tracked containers"""
        logger.debug("Retrieving container statistics")
        
        with self.lock:
            current_time = time.time()
            stats = {
                deployment_id: {
                    'model_id': info.get('model_id', 'unknown'),
                    'laptop_id': info.get('laptop_id'),
                    'status': info.get('status', 'unknown'),
                    'last_update': info.get('last_update'),
                    'uptime': current_time - info.get('deployment_time', current_time),
                    'request_rate': self.request_rates.get(deployment_id, {}).get('current_rate', 0),
                    'total_requests': self.request_rates.get(deployment_id, {}).get('request_count', 0),
                    'last_request_time': self.request_rates.get(deployment_id, {}).get('last_request_time'),
                    'inactive_time': (current_time - self.request_rates.get(deployment_id, {}).get('last_request_time', current_time)) 
                        if self.request_rates.get(deployment_id, {}).get('last_request_time') else None
                }
                for deployment_id, info in self.deployment_registry.items()
            }
            return stats

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
    version=data['version']
    logger.info(f"Processing deployment request for model {model_id}")
    
    result = controller.deploy_model(model_id,version)
    
    if result['success']:
        logger.info(f"Deployment successful: {result}")
        return jsonify(result), 200
    else:
        logger.error(f"Deployment failed: {result}")
        return jsonify(result), 500

@app.route('/controller/stop', methods=['POST'])
def stop_container():
    """Endpoint for manually stopping a container"""
    logger.info(f"Received stop request: {request.json}")
    
    data = request.json
    
    if not data or 'deployment_id' not in data:
        logger.warning("Received stop request without deployment_id")
        return jsonify({
            'success': False,
            'error': 'Missing deployment_id in request'
        }), 400
    
    deployment_id = data['deployment_id']
    
    with controller.lock:
        if deployment_id not in controller.deployment_registry:
            logger.warning(f"Container {deployment_id} not found for stop request")
            return jsonify({
                'success': False,
                'error': f'Container {deployment_id} not found'
            }), 404
        
        laptop_id = controller.deployment_registry[deployment_id]['laptop_id']
    
    logger.info(f"Processing stop request for container {deployment_id} on laptop {laptop_id}")
    result = controller._terminate_container(deployment_id, laptop_id)
    
    if result:
        logger.info(f"Container stop successful: {deployment_id}")
        return jsonify({
            'success': True,
            'message': f'Container {deployment_id} stopped successfully'
        }), 200
    else:
        logger.error(f"Container stop failed: {deployment_id}")
        return jsonify({
            'success': False,
            'error': f'Failed to stop container {deployment_id}'
        }), 500

@app.route('/controller/status', methods=['GET'])
def get_status():
    """Endpoint for getting controller status"""
    logger.debug("Received status request")
    
    with controller.lock:
        active_laptops = {}
        current_time = time.time()
        
        for laptop_id, metrics in controller.laptop_metrics.items():
            if current_time - metrics.get('last_updated', 0) < 60:
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

@app.route('/controller/containers', methods=['GET'])
def get_containers():
    """Endpoint for getting information about all tracked containers"""
    logger.debug("Received containers listing request")
    
    container_stats = controller.get_container_stats()
    
    response = {
        'container_count': len(container_stats),
        'containers': container_stats,
        'time': time.time()
    }
    
    logger.info(f"Containers response: {len(container_stats)} containers tracked")
    return jsonify(response), 200

if __name__ == '__main__':
    logger.info("Starting controller application")
    app.run(host='0.0.0.0', port=8090)