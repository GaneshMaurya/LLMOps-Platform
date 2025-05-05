#!/usr/bin/env python3
import os
import json
import time
import uuid
import logging
import threading
import socket
import requests
import docker
import psutil
from flask import Flask, request, jsonify
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import socket
from dotenv import load_dotenv
import subprocess

ENV_FILE_PATH = "/exports/applications/.env"  # Update this path as needed

if os.path.exists(ENV_FILE_PATH):
    load_dotenv(ENV_FILE_PATH)
    print(f"Environment variables loaded from {ENV_FILE_PATH}")
else:
    print(f"Error: .env file not found at {ENV_FILE_PATH}")
    exit(1)

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return '127.0.0.1'

# Function to get the host machine's IP address
def get_host_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))  # Connect to a public DNS server
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception as e:
        logger.error(f"Failed to retrieve host IP address: {str(e)}")
        return "127.0.0.1"  # Fallback to localhost if IP retrieval fails

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', f"{os.getenv('life_cycle_manager_ip')}:29092")
METRICS_TOPIC = os.getenv('METRICS_TOPIC', 'system-metrics')
LAPTOP_ID = os.getenv('LAPTOP_ID', socket.gethostname())
AGENT_PORT = int(os.getenv('AGENT_PORT', '8091'))
LAPTOP_ID = os.getenv('LAPTOP_ID', socket.gethostname())[:10]
AGENT_IP = os.getenv('AGENT_IP', get_local_ip())
METRICS_INTERVAL = int(os.getenv('METRICS_INTERVAL', '10'))
DOCKER_IMAGE = os.getenv('DOCKER_IMAGE', 'yaswanth2503/t1:latest')
APP_MOUNT_PATH = os.getenv('APP_MOUNT_PATH', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app'))
MODEL_REGISTRY_URL = os.getenv('MODEL_REGISTRY_URL', f"http://{os.getenv('model_registry_ip', 'localhost')}:8000")

agent_log_file = "/exports/applications/agent-Service/logs/agent-" + LAPTOP_ID + ".log"
os.makedirs(os.path.dirname(agent_log_file), exist_ok=True)

# Setup logging with detailed format
try:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s - [%(filename)s:%(lineno)d]',
        handlers=[
            logging.FileHandler(agent_log_file),
            logging.StreamHandler()
        ]
    )
except PermissionError as e:
    print(f"PermissionError: {e}. Falling back to console logging.")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s - [%(filename)s:%(lineno)d]',
        handlers=[
            logging.StreamHandler()
        ]
    )

logger = logging.getLogger("deployment-agent")

app = Flask(__name__)

def find_free_port():
    """Find a free port on the host machine."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

# Create Kafka topic if it doesn't exist
def create_kafka_topic():
    try:
        logger.info(f"Attempting to create Kafka topic: {METRICS_TOPIC}")
        admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        topic_list = [NewTopic(METRICS_TOPIC, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(topic_list)
        logger.info(f"Successfully created Kafka topic: {METRICS_TOPIC}")
    except Exception as e:
        logger.warning(f"Could not create topic {METRICS_TOPIC}: {str(e)}")

# Initialize Kafka producer
def init_kafka_producer():
    logger.info(f"Initializing Kafka producer with bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': f'deployment-agent-{LAPTOP_ID}',
        'message.max.bytes': 10485760  # 10MB max message size
    }
    return Producer(conf)

class DeploymentAgent:
    def __init__(self):
        logger.info(f"Initializing deployment agent on laptop: {LAPTOP_ID}")
        
        # Initialize Docker client
        # try:
        #     self.docker_client = docker.from_env()
        #     logger.info("Docker client initialized successfully")
        # except Exception as e:
        #     logger.error(f"Failed to initialize Docker client: {str(e)}")
        #     raise

        logger.info("Docker client initialization skipped (using Vagrant for VM management)")
        
        # Initialize Kafka producer
        self.producer = init_kafka_producer()
        
        # Track deployed containers
        self.container_registry = {}  # deployment_id -> VM info
        self.lock = threading.Lock()
        
        # Create app directory if it doesn't exist
        os.makedirs(APP_MOUNT_PATH, exist_ok=True)
        logger.info(f"App mount path: {APP_MOUNT_PATH}")
        
        # Start metrics collection thread
        self.metrics_thread = threading.Thread(target=self.collect_and_send_metrics)
        self.metrics_thread.daemon = True
        self.metrics_thread.start()
        logger.info("Started metrics collection thread")
        
        logger.info(f"Deployment agent initialized with ID: {LAPTOP_ID}, IP: {AGENT_IP}, Port: {AGENT_PORT}")
    
    def collect_metrics(self):
        """Collect system metrics only (no VM activity tracking)"""
        try:
            # System metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            network_io = psutil.net_io_counters()
            
            # Construct metrics object - system-level only
            metrics = {
                'laptop_id': LAPTOP_ID,
                'ip': AGENT_IP,
                'port': AGENT_PORT,
                'timestamp': time.time(),
                'cpu': {
                    'percent': cpu_percent,
                    'count': {
                        'physical': psutil.cpu_count(logical=False) or 1,
                        'logical': psutil.cpu_count(logical=True)
                    },
                    'freq': {
                        'current': psutil.cpu_freq().current if psutil.cpu_freq() else 0
                    }
                },
                'memory': {
                    'total': memory.total,
                    'available': memory.available,
                    'used': memory.used,
                    'percent': memory.percent
                },
                'disk': {
                    'total': disk.total,
                    'used': disk.used,
                    'free': disk.free,
                    'percent': disk.percent
                },
                'network': {
                    'bytes_sent': network_io.bytes_sent,
                    'bytes_recv': network_io.bytes_recv,
                    'packets_sent': network_io.packets_sent,
                    'packets_recv': network_io.packets_recv
                },
                'system': {
                    'system': os.name,
                    'hostname': socket.gethostname()
                }
            }
            
            return metrics
        except Exception as e:
            logger.error(f"Error collecting metrics: {str(e)}", exc_info=True)
            return None
    
    def send_metrics(self, metrics):
        """Send metrics to Kafka"""
        try:
            if metrics:
                self.producer.produce(
                    METRICS_TOPIC,
                    key=LAPTOP_ID,
                    value=json.dumps(metrics).encode('utf-8'),
                    callback=self.delivery_report
                )
                self.producer.flush(timeout=1)
                logger.info(f"Sent metrics to Kafka: CPU {metrics['cpu']['percent']:.1f}%, Memory {metrics['memory']['percent']:.1f}%")
            else:
                logger.warning("No metrics to send")
        except Exception as e:
            logger.error(f"Failed to send metrics to Kafka: {str(e)}", exc_info=True)
    
    def delivery_report(self, err, msg):
        """Callback for Kafka producer"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    def collect_and_send_metrics(self):
        """Continuously collect and send metrics"""
        logger.info("Starting metrics collection thread")
        while True:
            try:
                metrics = self.collect_metrics()
                self.send_metrics(metrics)
                time.sleep(METRICS_INTERVAL)
            except Exception as e:
                logger.error(f"Error in metrics collection thread: {str(e)}", exc_info=True)
                time.sleep(METRICS_INTERVAL)

    def get_vm_ip(self, vagrant_dir):
        """Get the public network IP address (192.x.x.x) of the running VM"""
        try:
            # Specifically look for 192.x.x.x addresses on any interface
            result = subprocess.run(
                ["vagrant", "ssh", "-c", "ip addr show | grep -E 'inet 192\\.' | head -n 1 | awk '{print $2}' | cut -d'/' -f1"],
                cwd=vagrant_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            vm_ip = result.stdout.strip()
            
            if not vm_ip:
                # Try getting all IPs and filter
                result = subprocess.run(
                    ["vagrant", "ssh", "-c", "hostname -I"],
                    cwd=vagrant_dir,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True
                )
                all_ips = result.stdout.strip().split()
                # Filter for 192.x.x.x addresses
                for ip in all_ips:
                    if ip.startswith("192."):
                        vm_ip = ip
                        break
            
            if not vm_ip:
                # As a last resort, get the interface that's connected to the public network
                result = subprocess.run(
                    ["vagrant", "ssh", "-c", "ip route | grep default | awk '{print $5}'"],
                    cwd=vagrant_dir,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True
                )
                default_interface = result.stdout.strip()
                
                if default_interface:
                    result = subprocess.run(
                        ["vagrant", "ssh", "-c", f"ip addr show {default_interface} | grep 'inet ' | awk '{{print $2}}' | cut -d'/' -f1"],
                        cwd=vagrant_dir,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        check=True
                    )
                    vm_ip = result.stdout.strip()
            
            return vm_ip
        except Exception as e:
            logger.error(f"Failed to retrieve VM IP address: {str(e)}")
            raise
    
    def deploy_model(self, model_id, version, redeploy=False, previous_deployment_id=None):
        """Deploy a VM for the model using Vagrant and execute the frontend and backend applications"""
        version = version or "latest"
        logger.info(f"Deploying model {model_id} version {version}" + (" (redeploy)" if redeploy else ""))
        
        try:
            # Generate deployment ID
            deployment_id = f"model-{model_id}-{uuid.uuid4().hex[:8]}"
            
            # Get model details from model registry
            try:
                logger.info(f"Fetching model details from registry for model {model_id}")
                if version and version != "latest":
                    version_param = version[1:] if version.startswith('v') else version
                    registry_response = requests.get(
                        f"{MODEL_REGISTRY_URL}/registry/fetch-model/{model_id}/{version_param}",
                        timeout=10
                    )
                else:
                    registry_response = requests.get(
                        f"{MODEL_REGISTRY_URL}/registry/fetch-model/{model_id}",
                        timeout=10
                    )
                
                if not registry_response.ok:
                    logger.error(f"Failed to get model details: {registry_response.status_code} - {registry_response.text}")
                    return {
                        'success': False,
                        'error': f"Model registry error: {registry_response.status_code}"
                    }
                
                model_details = registry_response.json()
                model_path = model_details.get('path')
                
                if not model_path or not os.path.exists(model_path):
                    logger.error(f"Model path does not exist: {model_path}")
                    return {
                        'success': False,
                        'error': f"Model path not found: {model_path}"
                    }
                    
                logger.info(f"Model path: {model_path}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Network error contacting model registry: {str(e)}")
                return {
                    'success': False,
                    'error': f"Model registry network error: {str(e)}"
                }
            
            # Find free ports for port forwarding - one for backend, one for frontend
            backend_port = find_free_port()
            frontend_port = find_free_port()  # Ensure different port
            logger.info(f"Using ports - Backend: {backend_port}, Frontend: {frontend_port}")

            # Get the host machine's IP address
            vm_ip = get_host_ip()

            # Create and start the VM using Vagrant
            logger.info(f"Creating VM for deployment {deployment_id}")
            vagrant_dir = os.path.join(APP_MOUNT_PATH, deployment_id)
            os.makedirs(vagrant_dir, exist_ok=True)
            
            # Write the Vagrantfile with port forwarding for both services
            vagrantfile_content = f"""
            Vagrant.configure("2") do |config|
                config.vm.box = "ubuntu-ml"

                # Forward ports for both backend and frontend
                config.vm.network "forwarded_port", guest: 5000, host: {backend_port}  # Backend
                config.vm.network "forwarded_port", guest: 8501, host: {frontend_port}  # Frontend
                
                config.vm.synced_folder "{model_path}", "/home/vagrant/model_app", create: true

                config.vm.provider "virtualbox" do |vb|
                    vb.memory = "1024"  # Increased memory for running both services
                    vb.cpus = 2
                end

                config.vm.provision "shell", inline: <<-SHELL
                    # Create and enable a 2GB swap file
                    if ! swapon --show | grep -q /swapfile; then
                        echo "Creating a 2GB swap file..."
                        sudo fallocate -l 2G /swapfile
                        sudo chmod 600 /swapfile
                        sudo mkswap /swapfile
                        sudo swapon /swapfile
                        echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
                        echo "Swap file created and enabled successfully."
                    else
                        echo "Swap file already exists and is enabled."
                    fi

                    # Navigate to the app directory
                    cd /home/vagrant/model_app
                    
                    # Use existing venv if available or create a new one if needed
                    if [ -d "venv" ]; then
                        pip install flask_cors
                        echo "Using existing virtual environment"
                        source venv/bin/activate
                    else
                        echo "Creating new virtual environment"
                        python3 -m venv venv
                        source venv/bin/activate
                        pip install flask flask_cors torch torchvision pillow streamlit requests
                    fi
                    
                    # Start backend service (Flask)
                    echo "Starting backend service (app.py)..."
                    nohup python3 app.py > backend.log 2>&1 &
                    echo $! > backend.pid

                    # Wait for backend to start
                    sleep 5

                    # Start frontend service (Streamlit)
                    echo "Starting frontend service (webapp.py)..."
                    nohup streamlit run webapp.py --server.port=8501 --server.address=0.0.0.0 > frontend.log 2>&1 &
                    echo $! > frontend.pid

                    # Set permissions for log files
                    chmod 666 backend.log frontend.log

                    echo "Both services started. Check logs at backend.log and frontend.log"
                SHELL
            end
            """
            with open(os.path.join(vagrant_dir, "Vagrantfile"), "w") as vf:
                vf.write(vagrantfile_content)
            
            # Start the VM
            logger.info(f"Starting VM for deployment {deployment_id}")
            os.system(f"cd {vagrant_dir} && vagrant up")

            logger.info(f"VM created with port forwarding - Backend: {vm_ip}:{backend_port}, Frontend: {vm_ip}:{frontend_port}")

            # Construct access URLs using the host IP and forwarded ports
            access_urls = {
                'backend': f"http://{vm_ip}:{backend_port}",
                'frontend': f"http://{vm_ip}:{frontend_port}"
            }

            logger.info(f"Deployment successful: {deployment_id}, accessible at {access_urls}")

            # Store VM info
            with self.lock:
                self.container_registry[deployment_id] = {
                    'model_id': model_id,
                    'version': version,
                    'vm_ip': vm_ip,
                    'backend_port': backend_port,
                    'frontend_port': frontend_port,
                    'started_at': time.time(),
                    'model_path': model_path,
                    'access_urls': access_urls
                }

                # If this is a redeploy, clean up the previous VM
                if redeploy and previous_deployment_id in self.container_registry:
                    logger.info(f"Cleaning up previous deployment {previous_deployment_id}")
                    self._cleanup_vm(previous_deployment_id)

            return {
                'success': True,
                'deployment_id': deployment_id,
                'vm_ip': vm_ip,
                'backend_port': backend_port,
                'frontend_port': frontend_port,
                'access_urls': access_urls,
                'model_id': model_id,
                'version': version
            }
            
        except Exception as e:
            logger.error(f"Error deploying model {model_id}: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }

    def _get_available_port(self, start_port, end_port):
        """Find an available port in the specified range"""
        for port in range(start_port, end_port):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(0.1)
            try:
                sock.bind(('', port))
                sock.close()
                return port
            except:
                sock.close()
        raise RuntimeError(f"No available ports in range {start_port}-{end_port}")
    
    def stop_container(self, deployment_id):
        """Stop and remove a container"""
        logger.info(f"Stopping VM for deployment {deployment_id}")
        
        with self.lock:
            if deployment_id not in self.container_registry:
                logger.warning(f"Deployment {deployment_id} not found")
                return False
            
            result = self._cleanup_container(deployment_id)
            
            if result:
                del self.container_registry[deployment_id]
                logger.info(f"Removed deployment {deployment_id} from registry")
            
            return result
    
    def _cleanup_container(self, deployment_id):
        """Helper to clean up a VM and its files"""
        try:
            container_info = self.container_registry.get(deployment_id)
            if not container_info:
                return False
            
            container_id = container_info.get('container_id')
            if container_id:
                try:
                    # Stop and remove the container
                    container = self.docker_client.containers.get(container_id)
                    container.stop(timeout=2)
                    container.remove()
                    logger.info(f"VM {container_id} stopped and removed")
                except docker.errors.NotFound:
                    logger.warning(f"VM {container_id} already removed")
                except Exception as e:
                    logger.error(f"Error removing VM {container_id}: {str(e)}", exc_info=True)
            
            # Clean up the model directory
            model_dir = os.path.join(APP_MOUNT_PATH, deployment_id)
            if os.path.exists(model_dir):
                try:
                    import shutil
                    shutil.rmtree(model_dir)
                    logger.info(f"Removed model directory {model_dir}")
                except Exception as e:
                    logger.error(f"Error removing model directory {model_dir}: {str(e)}", exc_info=True)
            
            return True
        except Exception as e:
            logger.error(f"Error cleaning up deployment {deployment_id}: {str(e)}", exc_info=True)
            return False

# Initialize agent
deployment_agent = DeploymentAgent()

@app.route('/create-vm', methods=['POST'])
def create_vm():
    """Endpoint for VM creation"""
    logger.info(f"Received deployment request: {request.json}")
    
    data = request.json
    
    if not data or 'model_id' not in data:
        logger.warning("Missing model_id in request")
        return jsonify({
            'success': False,
            'error': 'Missing model_id in request'
        }), 400
    
    model_id = data['model_id']
    redeploy = data.get('redeploy', False)
    version = data.get('version', None)
    previous_deployment_id = data.get('previous_deployment_id')
    
    result = deployment_agent.deploy_model(model_id, version, redeploy, previous_deployment_id)
    
    if result.get('success', False):
        logger.info(f"Deployment successful: {result}")
        return jsonify(result), 200
    else:
        logger.error(f"Deployment failed: {result}")
        return jsonify(result), 500

@app.route('/stop-vm/<deployment_id>', methods=['POST'])
def stop_vm(deployment_id):
    """Endpoint for VM stopping"""
    logger.info(f"Received stop request for deployment {deployment_id}")
    
    if deployment_agent.stop_container(deployment_id):
        logger.info(f"Successfully stopped VM {deployment_id}")
        return jsonify({
            'success': True,
            'message': f"VM {deployment_id} stopped"
        }), 200
    else:
        logger.error(f"Failed to stop VM {deployment_id}")
        return jsonify({
            'success': False,
            'error': f"Failed to stop VM {deployment_id}"
        }), 404

@app.route('/status', methods=['GET'])
def get_status():
    """Endpoint for getting agent status"""
    logger.debug("Received status request")
    
    with deployment_agent.lock:
        current_time = time.time()
        
        # Get VM status (without activity metrics)
        containers = {}
        for deployment_id, info in deployment_agent.container_registry.items():
            # Check if VM is still running
            container_id = info.get('container_id')
            status = 'unknown'
            
            if container_id:
                try:
                    container = deployment_agent.docker_client.containers.get(container_id)
                    status = container.status
                except:
                    status = 'not_found'
            
            containers[deployment_id] = {
                'model_id': info.get('model_id'),
                'version': info.get('version', 'latest'),
                'status': status,
                'started_at': info.get('started_at'),
                'uptime': current_time - info.get('started_at', current_time) if info.get('started_at') else None,
                'host_port': info.get('host_port'),
                'container_id': container_id,
                'access_url': f"http://{AGENT_IP}:{info.get('host_port')}"
            }
        
        # Get system metrics
        cpu_percent = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        
        status = {
            'laptop_id': LAPTOP_ID,
            'ip': AGENT_IP,
            'port': AGENT_PORT,
            'running_containers': len(containers),
            'containers': containers,
            'system': {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent
            },
            'time': current_time
        }
        
        logger.info(f"Status response: CPU: {cpu_percent}%, Memory: {memory.percent}%, {len(containers)} containers running")
        return jsonify(status), 200

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    logger.debug("Received health check request")
    return jsonify({
        'status': 'healthy',
        'laptop_id': LAPTOP_ID,
        'time': time.time()
    }), 200

if __name__ == '__main__':
    # Create Kafka topic if it doesn't exist
    # create_kafka_topic()
    
    # Start Flask app
    logger.info(f"Starting agent application on port {AGENT_PORT}")
    app.run(host='0.0.0.0', port=AGENT_PORT)
