#!/usr/bin/env python3
import os
import json
import time
import uuid
import logging
import threading
import socket
import requests
import psutil
from flask import Flask, request, jsonify
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import socket
from dotenv import load_dotenv
import subprocess
import re
import random
from pathlib import Path

ENV_FILE_PATH = "/exports/applications/.env"  # Update this path as needed

def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))  # Bind to any available port on localhost
        return s.getsockname()[1]  # Return the port number assigned by the OS


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

def random_mac():
    # Keep the first three bytes as VirtualBox's OUI (08:00:27)
    mac_prefix = [0x08, 0x00, 0x27]
    mac_suffix = [random.randint(0x00, 0xFF) for _ in range(3)]
    mac = mac_prefix + mac_suffix
    return ''.join(f"{octet:02X}" for octet in mac)

# --- Vagrant Template ---
def generate_vagrantfile(bridge_adapter, host_app_path, mac , port_):
    return f'''
Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu-ml"

  config.vm.network "public_network", bridge: "{bridge_adapter}", mac: "{mac}"

  config.vm.synced_folder "{host_app_path}", "/app", create: true

  config.vm.provider "virtualbox" do |vb|
    vb.memory = "2048"
    vb.cpus = 2
  end

  config.vm.provision "shell", inline: <<-SHELL
    cd /app
    pip3 install -r requirements.txt
    nohup python3 -u app.py > app.log 2>&1 &
    nohup streamlit run webapp.py --server.port "{port_}" --server.headless true > webapp.log 2>&1 &
    ip a | grep enp0s8
  SHELL
end
'''

# --- Detect active Ethernet adapter (non-virtual, non-loopback) ---
def get_bridge_adapter():
    try:
        result = subprocess.run(
            ["nmcli", "-t", "-f", "DEVICE,TYPE,STATE", "device"],
            stdout=subprocess.PIPE, text=True, check=True
        )
        for line in result.stdout.strip().split("\n"):
            device, dtype, state = line.split(":")
            if (dtype == "ethernet" or dtype == "wifi") and state == "connected":
                return device
    except Exception:
        # Fallback to `ip link`
        result = subprocess.run(["ip", "-o", "link", "show"], stdout=subprocess.PIPE, text=True)
        for line in result.stdout.splitlines():
            parts = line.split(": ")
            if len(parts) >= 2:
                name = parts[1].split("@")[0]
                if not name.startswith("lo") and "docker" not in name and "virbr" not in name:
                    return name
    return "eth0"  # fallback

# --- Check if box is registered ---
def is_box_registered(box_name):
    try:
        result = subprocess.run(
            ["vagrant", "box", "list"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True
        )
        return any(box_name in line for line in result.stdout.splitlines())
    except subprocess.CalledProcessError as e:
        print("Error while checking box list:", e.stderr)
        return False

# --- Register box if not present ---
def register_box(box_name, box_path):
    try:
        subprocess.run(
            ["vagrant", "box", "add", box_name, box_path],
            check=True
        )
    except subprocess.CalledProcessError as e:
        print("Failed to register box:", e.stderr)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', f"{os.getenv('life_cycle_manager_ip')}:29092")
METRICS_TOPIC = os.getenv('METRICS_TOPIC', 'system-metrics')
LAPTOP_METRICS_TOPIC = os.getenv('LAPTOP_METRICS_TOPIC', 'system-metrics')  # New topic for laptop metrics
LAPTOP_ID = os.getenv('LAPTOP_ID', socket.gethostname())[:10]
AGENT_PORT = int(os.getenv('AGENT_PORT', '8091'))
AGENT_IP = os.getenv('AGENT_IP', get_local_ip())
METRICS_INTERVAL = int(os.getenv('METRICS_INTERVAL', '10'))
APP_MOUNT_PATH = os.getenv('APP_MOUNT_PATH', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app'))
MODEL_REGISTRY_URL = os.getenv('MODEL_REGISTRY_URL', f"http://{os.getenv('model_registry_ip', 'localhost')}:8000")


agent_log_file = "/exports/applications/agent-Service/logs/agent-" + LAPTOP_ID + ".log"
os.makedirs(os.path.dirname(agent_log_file), exist_ok=True)

# Setup logging with detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s - [%(filename)s:%(lineno)d]',
    handlers=[
        logging.FileHandler(agent_log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("deployment-agent")

app = Flask(__name__)

# Create Kafka topic if it doesn't exist
def create_kafka_topic():
    try:
        logger.info(f"Attempting to create Kafka topics: {METRICS_TOPIC}, {LAPTOP_METRICS_TOPIC}")
        admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        topic_list = [
            NewTopic(METRICS_TOPIC, num_partitions=1, replication_factor=1),
            NewTopic(LAPTOP_METRICS_TOPIC, num_partitions=1, replication_factor=1)  # Create laptop metrics topic
        ]
        admin_client.create_topics(topic_list)
        logger.info(f"Successfully created Kafka topics: {METRICS_TOPIC}, {LAPTOP_METRICS_TOPIC}")
    except Exception as e:
        logger.warning(f"Could not create topics: {str(e)}")

# Initialize Kafka producer
def init_kafka_producer():
    logger.info(f"Initializing Kafka producer with bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': f'deployment-agent-{LAPTOP_ID}',
        'message.max.bytes': 10485760  # 10MB max message size
    }
    return Producer(conf)

class LaptopMetricsCollector:
    def __init__(self, producer):
        self.producer = producer
        self.laptop_id = LAPTOP_ID
        self.agent_ip = AGENT_IP
        self.metrics_interval = METRICS_INTERVAL
        
        # Start metrics collection thread
        self.thread = threading.Thread(target=self.collect_and_send_metrics_loop)
        self.thread.daemon = True
        logger.info("Initializing laptop metrics collector")
    
    def start(self):
        self.thread.start()
        logger.info("Started laptop metrics collection thread")
    
    def collect_laptop_metrics(self):
        """Collect detailed laptop metrics"""
        try:
            # System metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_times = psutil.cpu_times_percent(interval=1)
            memory = psutil.virtual_memory()
            swap = psutil.swap_memory()
            disk = psutil.disk_usage('/')
            disk_io = psutil.disk_io_counters()
            network_io = psutil.net_io_counters()
            
            # Battery info (if available)
            battery_info = {}
            if hasattr(psutil, "sensors_battery"):
                battery = psutil.sensors_battery()
                if battery:
                    battery_info = {
                        "percent": battery.percent,
                        "power_plugged": battery.power_plugged,
                        "secsleft": battery.secsleft
                    }
            
            # Temperature sensors (if available)
            temperatures = {}
            if hasattr(psutil, "sensors_temperatures"):
                temps = psutil.sensors_temperatures()
                if temps:
                    for name, entries in temps.items():
                        temperatures[name] = [{"label": entry.label, "current": entry.current} for entry in entries]
            
            # Detailed process info
            process_count = len(psutil.pids())
            
            # Get top 5 CPU consuming processes
            top_processes = []
            for proc in sorted(psutil.process_iter(['pid', 'name', 'username', 'cpu_percent', 'memory_percent']), 
                               key=lambda p: p.info['cpu_percent'] or 0, 
                               reverse=True)[:5]:
                top_processes.append({
                    'pid': proc.info['pid'],
                    'name': proc.info['name'],
                    'username': proc.info['username'],
                    'cpu_percent': proc.info['cpu_percent'],
                    'memory_percent': proc.info['memory_percent']
                })
            
            # Collect Vagrant VM information if available
            vagrant_vms = []
            try:
                deployments_dir = Path("./deployments")
                if deployments_dir.exists():
                    for vm_dir in deployments_dir.iterdir():
                        if vm_dir.is_dir():
                            vagrant_vms.append({
                                'deployment_id': vm_dir.name,
                                'path': str(vm_dir.absolute())
                            })
            except Exception as e:
                logger.error(f"Error collecting Vagrant VM info: {str(e)}")
            
            # Construct metrics object
            metrics = {
                'laptop_id': self.laptop_id,
                'ip': self.agent_ip,
                'timestamp': time.time(),
                'cpu': {
                    'percent': cpu_percent,
                    'times_percent': {
                        'user': cpu_times.user,
                        'system': cpu_times.system,
                        'idle': cpu_times.idle,
                        'iowait': cpu_times.iowait if hasattr(cpu_times, 'iowait') else None
                    },
                    'count': {
                        'physical': psutil.cpu_count(logical=False) or 1,
                        'logical': psutil.cpu_count(logical=True)
                    },
                    'freq': {
                        'current': psutil.cpu_freq().current if psutil.cpu_freq() else 0,
                        'min': psutil.cpu_freq().min if psutil.cpu_freq() and hasattr(psutil.cpu_freq(), 'min') else None,
                        'max': psutil.cpu_freq().max if psutil.cpu_freq() and hasattr(psutil.cpu_freq(), 'max') else None
                    }
                },
                'memory': {
                    'total': memory.total,
                    'available': memory.available,
                    'used': memory.used,
                    'free': memory.free,
                    'percent': memory.percent,
                    'swap': {
                        'total': swap.total,
                        'used': swap.used,
                        'free': swap.free,
                        'percent': swap.percent
                    }
                },
                'disk': {
                    'total': disk.total,
                    'used': disk.used,
                    'free': disk.free,
                    'percent': disk.percent,
                    'io': {
                        'read_count': disk_io.read_count if disk_io else 0,
                        'write_count': disk_io.write_count if disk_io else 0,
                        'read_bytes': disk_io.read_bytes if disk_io else 0,
                        'write_bytes': disk_io.write_bytes if disk_io else 0,
                        'read_time': disk_io.read_time if disk_io and hasattr(disk_io, 'read_time') else 0,
                        'write_time': disk_io.write_time if disk_io and hasattr(disk_io, 'write_time') else 0
                    }
                },
                'network': {
                    'bytes_sent': network_io.bytes_sent,
                    'bytes_recv': network_io.bytes_recv,
                    'packets_sent': network_io.packets_sent,
                    'packets_recv': network_io.packets_recv,
                    'errin': network_io.errin,
                    'errout': network_io.errout,
                    'dropin': network_io.dropin,
                    'dropout': network_io.dropout
                },
                'system': {
                    'boot_time': psutil.boot_time(),
                    'system': os.name,
                    'hostname': socket.gethostname(),
                    'processes': {
                        'count': process_count,
                        'top_cpu': top_processes
                    }
                }
            }
            
            # Add battery info if available
            if battery_info:
                metrics['battery'] = battery_info
            
            # Add temperature sensors if available
            if temperatures:
                metrics['temperatures'] = temperatures
            
            # Add Vagrant VM info if available
            if vagrant_vms:
                metrics['vagrant_vms'] = vagrant_vms
            
            return metrics
        except Exception as e:
            logger.error(f"Error collecting laptop metrics: {str(e)}", exc_info=True)
            return None
    
    def delivery_report(self, err, msg):
        """Callback for Kafka producer"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    def send_metrics(self, metrics):
        """Send metrics to Kafka"""
        try:
            if metrics:
                self.producer.produce(
                    LAPTOP_METRICS_TOPIC,
                    key=self.laptop_id,
                    value=json.dumps(metrics).encode('utf-8'),
                    callback=self.delivery_report
                )
                self.producer.flush(timeout=1)
                logger.info(f"Sent laptop metrics to Kafka: CPU {metrics['cpu']['percent']:.1f}%, Memory {metrics['memory']['percent']:.1f}%")
            else:
                logger.warning("No laptop metrics to send")
        except Exception as e:
            logger.error(f"Failed to send laptop metrics to Kafka: {str(e)}", exc_info=True)
    
    def collect_and_send_metrics_loop(self):
        """Continuously collect and send laptop metrics"""
        logger.info("Starting laptop metrics collection loop")
        while True:
            try:
                metrics = self.collect_laptop_metrics()
                self.send_metrics(metrics)
                time.sleep(self.metrics_interval)
            except Exception as e:
                logger.error(f"Error in laptop metrics collection loop: {str(e)}", exc_info=True)
                time.sleep(self.metrics_interval)

# --- Flask App Factory ---
def create_app():
    app = Flask(__name__)

    box_name = "ubuntu-ml"
    box_path = "/exports/applications/vm-service/ubuntu-ml.box"  # Replace this path

    # Initial Setup
    with app.app_context():
        if not is_box_registered(box_name):
            register_box(box_name, box_path)

    # Create Kafka topic
    create_kafka_topic()
    
    # Initialize Kafka producer
    producer = init_kafka_producer()
    
    # Initialize and start laptop metrics collector
    laptop_metrics_collector = LaptopMetricsCollector(producer)
    laptop_metrics_collector.start()

    @app.route('/')
    def index():
        return "Flask server with Vagrant is up."

    @app.route('/create-vm', methods=['POST'])
    def provision_vm():
        data = request.get_json()
        model_id = data['model_id']
        version = data.get('version', None)

        free_port = get_free_port()

        if version and version != "latest":
            # Strip 'v' prefix if present
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
        host_app_path = model_details.get('path')

        if not host_app_path or not os.path.exists(host_app_path):
            return jsonify({"error": "Invalid host_app_path"}), 400

        deploy_id = str(uuid.uuid4())[:8]
        folder_path = Path(f"./deployments/{deploy_id}")
        folder_path.mkdir(parents=True, exist_ok=True)

        adapter = get_bridge_adapter()
        
        vagrantfile_content = generate_vagrantfile(adapter, host_app_path, random_mac(), free_port)
        (folder_path / "Vagrantfile").write_text(vagrantfile_content)

        try:
            subprocess.run(["vagrant", "up"], cwd=folder_path, check=True)
        except subprocess.CalledProcessError as e:
            return jsonify({"error": "Failed to provision VM", "details": str(e)}), 500

        # Get IP of bridged network inside the VM
        try:
            bridge_ip_cmd = "ip -o -4 addr show | grep enp0s8 | awk '{print $4}' | cut -d'/' -f1"
            result = subprocess.run(
                ["vagrant", "ssh", "-c", bridge_ip_cmd],
                cwd=folder_path,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            vm_ip = result.stdout.strip().splitlines()[0]
        except Exception as e:
            return jsonify({'success': False, "error": str(e)}), 500

        # Return the known hardcoded port
        return jsonify({
            'success': True,
            "deployment_id": deploy_id,
            "container_id" : deploy_id,
            "host_port": free_port,
            "access_url": f"http://{vm_ip}:{free_port}",
            'model_id': model_id,
            'version': version
        })

    @app.route('/stop-vm/<deployment_id>', methods=['POST'])
    def deprovision_vm(deployment_id):
        print(deployment_id)
        if not deployment_id:
            return jsonify({'success': False,"error": "Missing deployment_id"}), 400

        folder_path = Path(f"./deployments/{deployment_id}")
        if not folder_path.exists():
            return jsonify({'success': False,"error": "Deployment not found"}), 404

        try:
            subprocess.run(["vagrant", "destroy", "-f"], cwd=folder_path, check=True)
        except subprocess.CalledProcessError as e:
            return jsonify({'success': False,'error': f"Failed to stop VM {deployment_id}"}), 500

        # Optionally remove the folder (clean-up)
        try:
            for child in folder_path.glob("*"):
                child.unlink()
            folder_path.rmdir()
        except Exception as e:
            return jsonify({'success': True,"message": "VM destroyed, but cleanup failed", "details": str(e)}), 200

        return jsonify({'success': True,'message': f"VM {deployment_id} stopped"})

    @app.route('/status', methods=['GET'])
    def get_status():
        """Endpoint for getting agent status"""
        logger.debug("Received status request")
        
        # Get system metrics
        cpu_percent = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        
        # Get all vagrant VMs
        vagrant_vms = []
        deployments_dir = Path("./deployments")
        if deployments_dir.exists():
            for vm_dir in deployments_dir.iterdir():
                if vm_dir.is_dir():
                    try:
                        # Try to get VM status
                        status_output = subprocess.run(
                            ["vagrant", "status"], 
                            cwd=vm_dir, 
                            stdout=subprocess.PIPE, 
                            stderr=subprocess.PIPE, 
                            text=True
                        )
                        vm_status = "unknown"
                        for line in status_output.stdout.splitlines():
                            if "default" in line:
                                if "running" in line.lower():
                                    vm_status = "running"
                                elif "poweroff" in line.lower() or "stopped" in line.lower():
                                    vm_status = "stopped"
                                break
                        
                        # Try to get VM IP if running
                        vm_ip = None
                        if vm_status == "running":
                            try:
                                bridge_ip_cmd = "ip -o -4 addr show | grep enp0s8 | awk '{print $4}' | cut -d'/' -f1"
                                ip_result = subprocess.run(
                                    ["vagrant", "ssh", "-c", bridge_ip_cmd],
                                    cwd=vm_dir,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    text=True,
                                    check=True
                                )
                                vm_ip = ip_result.stdout.strip().splitlines()[0]
                            except:
                                pass
                        
                        vagrant_vms.append({
                            'deployment_id': vm_dir.name,
                            'status': vm_status,
                            'path': str(vm_dir.absolute()),
                            'ip': vm_ip,
                            'url': f"http://{vm_ip}:8051" if vm_ip else None
                        })
                    except Exception as e:
                        logger.error(f"Error getting status for VM {vm_dir.name}: {str(e)}")
                        vagrant_vms.append({
                            'deployment_id': vm_dir.name,
                            'status': 'error',
                            'path': str(vm_dir.absolute()),
                            'error': str(e)
                        })
        
        status = {
            'laptop_id': LAPTOP_ID,
            'ip': AGENT_IP,
            'port': AGENT_PORT,
            'running_vms': len([vm for vm in vagrant_vms if vm['status'] == 'running']),
            'total_vms': len(vagrant_vms),
            'vms': vagrant_vms,
            'system': {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent
            },
            'time': time.time()
        }
        
        logger.info(f"Status response: CPU: {cpu_percent}%, Memory: {memory.percent}%, VMs: {len(vagrant_vms)}")
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

    return app

# --- Run Server ---
if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=AGENT_PORT)
