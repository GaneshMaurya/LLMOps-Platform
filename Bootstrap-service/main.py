import os
import subprocess
import json
import logging
import threading
import time
import socket
import uuid
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from flask import Flask, jsonify
from pythonjsonlogger import jsonlogger
import yaml
from http import HTTPStatus
import re

# Flask app for health endpoint
app = Flask(__name__)

# Configuration Management: Load config from YAML or environment variables
CONFIG_FILE = os.getenv("BOOTSTRAP_CONFIG", "/etc/bootstrap/config.yaml")
DEFAULT_CONFIG = {
    "kafka_brokers": ["localhost:29092"],
    "vmops_topic": "vmops",
    "available_ips_topic": "available_ips",
    "allocated_ips_topic": "allocated_ips",
    "nfs_server": "192.168.33.124",
    "nfs_share": "/exports",
    "mount_point": "/exports",
    "agent_binary": "/exports/system_services/binaries/servicelifecyclemanager/agent",
    "slcm_binary": "/exports/system_services/binaries/servicelifecyclemanager/serverlifecyclemanager",
    "persist_nfs_mount": True,
    "health_check_interval": 30,
    "health_port": 5000,
    "deployment_timeout": 300  # Align with SLCM's VM_REQUEST_TIMEOUT
}

def load_config():
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r") as f:
                config = yaml.safe_load(f)
        else:
            config = {}
        for key, value in DEFAULT_CONFIG.items():
            config.setdefault(key, value)
        config["kafka_brokers"] = os.getenv("KAFKA_BROKERS", ",".join(config["kafka_brokers"])).split(",")
        config["nfs_server"] = os.getenv("NFS_SERVER", config["nfs_server"])
        config["slcm_binary"] = os.getenv("SLCM_BINARY", config["slcm_binary"])
        config["health_port"] = int(os.getenv("HEALTH_PORT", config["health_port"]))
        return config
    except Exception as e:
        logger.error({"message": f"Failed to load config: {str(e)}"})
        raise

config = load_config()

# Observability: Structured JSON logging
log_handler = logging.StreamHandler()
log_handler.setFormatter(jsonlogger.JsonFormatter(
    fmt="%(asctime)s %(levelname)s %(name)s %(message)s"
))
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers = [log_handler]

# Health Check: Service status
health_status = {
    "slcm_running": False,
    "kafka_consumer_active": False,
    "last_deployment_success": None,
    "last_error": None,
    "slcm_vm_ip": None
}

def create_topics():
    """Create required Kafka topics if they don't exist."""
    admin_client = KafkaAdminClient(bootstrap_servers=config["kafka_brokers"])
    try:
        topics = [
            NewTopic(name=config["available_ips_topic"], num_partitions=3, replication_factor=1),
            NewTopic(name=config["allocated_ips_topic"], num_partitions=3, replication_factor=1)
        ]
        admin_client.create_topics(topics)
        logger.info({"message": "Topics created", "topics": [t.name for t in topics]})
    except TopicAlreadyExistsError:
        logger.info({"message": "Topics already exist", "topics": [t.name for t in topics]})
    except Exception as e:
        logger.error({"message": f"Failed to create topics: {str(e)}"})
        raise
    finally:
        admin_client.close()

def check_slcm_status():
    """Periodically check if SLCM is running in the VM."""
    while True:
        try:
            if not health_status["slcm_vm_ip"]:
                logger.warning({"message": "No SLCM VM IP assigned"})
                health_status["slcm_running"] = False
            else:
                check_command = (
                    f"ssh -o StrictHostKeyChecking=no vagrant@{health_status['slcm_vm_ip']} "
                    f"'pgrep -f {config['slcm_binary']} && echo running'"
                )
                result = subprocess.run(check_command, shell=True, capture_output=True, text=True)
                health_status["slcm_running"] = "running" in result.stdout
                if not health_status["slcm_running"]:
                    logger.warning({"message": f"SLCM not running on {health_status['slcm_vm_ip']}"})
        except Exception as e:
            logger.error({"message": f"SLCM health check failed: {str(e)}"})
            health_status["slcm_running"] = False
        time.sleep(config["health_check_interval"])

@app.route("/health")
def health():
    """Health check endpoint."""
    status = {
        "status": "healthy" if health_status["slcm_running"] and health_status["kafka_consumer_active"] else "unhealthy",
        "slcm_running": health_status["slcm_running"],
        "kafka_consumer_active": health_status["kafka_consumer_active"],
        "last_deployment_success": health_status["last_deployment_success"],
        "last_error": health_status["last_error"],
        "slcm_vm_ip": health_status["slcm_vm_ip"]
    }
    return jsonify(status), HTTPStatus.OK if status["status"] == "healthy" else HTTPStatus.SERVICE_UNAVAILABLE

def start_slcm():
    """Provision a VM and deploy SLCM and agent."""
    static_ip = None
    request_id = str(uuid.uuid4())
    agent_id = str(uuid.uuid4())
    start_time = time.time()
    try:
        # Step 1: Fetch and validate IP
        static_ip = fetch_available_ip()
        if not re.match(r"^192\.168\.56\.\d{1,3}$", static_ip):
            raise ValueError(f"Invalid IP: {static_ip}")
        logger.info({"message": "Fetched available IP for SLCM", "ip": static_ip})
        health_status["slcm_vm_ip"] = static_ip

        # Step 2: Acknowledge IP
        acknowledge_ip(static_ip)
        logger.info({"message": "Acknowledged IP for SLCM", "ip": static_ip})

        # Check timeout
        if time.time() - start_time > config["deployment_timeout"]:
            raise TimeoutError("SLCM deployment timed out")

        # Step 3: Validate VM connectivity
        validate_vm(static_ip)
        logger.info({"message": "VM validated for SLCM", "ip": static_ip})

        # Step 4: Mount NFS
        mount_nfs(static_ip)
        logger.info({"message": "NFS mounted for SLCM", "ip": static_ip})

        # Check timeout
        if time.time() - start_time > config["deployment_timeout"]:
            raise TimeoutError("SLCM deployment timed out")

        # Step 5: Deploy SLCM
        deploy_slcm(static_ip)
        logger.info({"message": "SLCM deployed", "ip": static_ip})

        # Step 6: Deploy agent
        deploy_agent(static_ip)
        logger.info({"message": "Agent deployed for SLCM", "ip": static_ip})

        # Step 7: Validate VM health
        validate_vm_health(static_ip)
        logger.info({"message": "VM health validated for SLCM", "ip": static_ip})

        # Step 8: Publish success status
        publish_deployment_status(request_id, agent_id, static_ip, "slcm_provisioned", None)
        health_status["last_deployment_success"] = True
        health_status["last_error"] = None

    except Exception as e:
        logger.error({"message": f"SLCM deployment failed for IP {static_ip}", "error": str(e)})
        health_status["last_deployment_success"] = False
        health_status["last_error"] = str(e)
        if static_ip:
            cleanup(static_ip, str(e))
        publish_deployment_status(request_id, agent_id, static_ip, "slcm_provision_failed", str(e))
        raise

def deploy_slcm(static_ip):
    """Deploy the SLCM binary from the NFS share."""
    try:
        check_command = (
            f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} "
            f"'pgrep -f {config['slcm_binary']} && echo running'"
        )
        result = subprocess.run(check_command, shell=True, capture_output=True, text=True)
        if "running" in result.stdout:
            logger.info({"message": f"SLCM already running on {static_ip}"})
            return

        # Configure SLCM with Kafka settings
        config_command = (
            f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} "
            f"'echo \"KAFKA_BOOTSTRAP_SERVERS={','.join(config['kafka_brokers'])}\" > /tmp/slcm_config.env && "
            f"echo \"VMOPS_TOPIC={config['vmops_topic']}\" >> /tmp/slcm_config.env'"
        )
        subprocess.run(config_command, shell=True, check=True)

        ssh_command = (
            f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} "
            f"'sudo env $(cat /tmp/slcm_config.env) nohup {config['slcm_binary']} > /tmp/slcm.log 2>&1 &'"
        )
        subprocess.run(ssh_command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        logger.error({"message": f"SLCM deployment failed on {static_ip}", "error": str(e)})
        raise

def check_dependencies():
    """Validate Kafka and NFS server connectivity."""
    try:
        for broker in config["kafka_brokers"]:
            host, port = broker.split(":")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, int(port)))
            sock.close()
            if result != 0:
                raise ConnectionError(f"Cannot connect to Kafka broker {broker}")
        logger.info({"message": "Kafka brokers reachable"})
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((config["nfs_server"], 2049))
        sock.close()
        if result != 0:
            raise ConnectionError(f"Cannot connect to NFS server {config['nfs_server']}:2049")
        logger.info({"message": "NFS server reachable"})
    except Exception as e:
        logger.error({"message": f"Dependency check failed: {str(e)}"})
        raise

def consume_deploy_requests():
    """Consume VM requests from the vmops topic."""
    consumer = KafkaConsumer(
        config["vmops_topic"],
        bootstrap_servers=config["kafka_brokers"],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        group_id="bootstrap_server",
        auto_offset_reset="earliest",
        enable_auto_commit=False
    )
    health_status["kafka_consumer_active"] = True
    try:
        for message in consumer:
            request = message.value
            if request.get("type") == "vm_request":
                logger.info({"message": "Received VM request", "request": request})
                process_deploy_request(request)
                consumer.commit()
    except Exception as e:
        logger.error({"message": f"Kafka consumer failed: {str(e)}"})
        health_status["kafka_consumer_active"] = False
        raise
    finally:
        consumer.close()

def process_deploy_request(vm_request):
    """Process a VM provisioning request."""
    static_ip = None
    request_id = vm_request.get("request_id")
    if not request_id:
        logger.error({"message": "Missing request_id in VM request"})
        return
    agent_id = str(uuid.uuid4())  # Generate agent_id
    start_time = time.time()
    try:
        # Check timeout
        if time.time() - start_time > config["deployment_timeout"]:
            raise TimeoutError("Deployment timed out")

        # Step 1: Fetch and validate IP
        static_ip = fetch_available_ip()
        if not re.match(r"^192\.168\.56\.\d{1,3}$", static_ip):
            raise ValueError(f"Invalid IP: {static_ip}")
        logger.info({"message": "Fetched available IP", "ip": static_ip})

        # Step 2: Acknowledge IP
        acknowledge_ip(static_ip)
        logger.info({"message": "Acknowledged IP", "ip": static_ip})

        # Check timeout
        if time.time() - start_time > config["deployment_timeout"]:
            raise TimeoutError("Deployment timed out")

        # Step 3: Validate VM connectivity
        validate_vm(static_ip)
        logger.info({"message": "VM validated", "ip": static_ip})

        # Step 4: Mount NFS
        mount_nfs(static_ip)
        logger.info({"message": "NFS mounted", "ip": static_ip})

        # Check timeout
        if time.time() - start_time > config["deployment_timeout"]:
            raise TimeoutError("Deployment timed out")

        # Step 5: Deploy agent
        deploy_agent(static_ip)
        logger.info({"message": "Agent deployed", "ip": static_ip})

        # Step 6: Validate VM health
        validate_vm_health(static_ip)
        logger.info({"message": "VM health validated", "ip": static_ip})

        # Step 7: Publish success status
        publish_deployment_status(request_id, agent_id, static_ip, "vm_provisioned", None)
        health_status["last_deployment_success"] = True
        health_status["last_error"] = None

    except Exception as e:
        logger.error({"message": f"Deployment failed for IP {static_ip}", "error": str(e)})
        health_status["last_deployment_success"] = False
        health_status["last_error"] = str(e)
        if static_ip:
            cleanup(static_ip, str(e))
        publish_deployment_status(request_id, agent_id, static_ip, "vm_provision_failed", str(e))
        raise

def fetch_available_ip():
    """Fetch an available static IP from Kafka."""
    consumer = KafkaConsumer(
        config["available_ips_topic"],
        bootstrap_servers=config["kafka_brokers"],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        group_id="bootstrap_server",
        auto_offset_reset="earliest",
        enable_auto_commit=False
    )
    for message in consumer:
        ip = message.value.get("ip")
        consumer.commit()
        consumer.close()
        return ip

def acknowledge_ip(ip):
    """Acknowledge the IP allocation."""
    producer = KafkaProducer(
        bootstrap_servers=config["kafka_brokers"],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send(config["allocated_ips_topic"], {"ip": ip})
    producer.flush()
    producer.close()

def validate_vm(static_ip):
    """Validate VM SSH connectivity."""
    for attempt in range(3):
        try:
            subprocess.run(
                f"ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no vagrant@{static_ip} exit",
                shell=True, check=True
            )
            return
        except subprocess.CalledProcessError:
            logger.warning({"message": f"SSH attempt {attempt+1} failed for {static_ip}"})
            time.sleep(5)
    raise ConnectionError(f"Cannot SSH to VM {static_ip}")

def mount_nfs(static_ip):
    """Mount the NFS share on the VM via SSH."""
    for attempt in range(3):
        try:
            setup_command = (
                f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} "
                f"'sudo apt-get update && sudo apt-get install -y nfs-common && "
                f"sudo mkdir -p {config['mount_point']} && sudo chown vagrant:vagrant {config['mount_point']}'"
            )
            subprocess.run(setup_command, shell=True, check=True)

            check_mount = (
                f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} "
                f"'mountpoint -q {config['mount_point']} && echo mounted'"
            )
            result = subprocess.run(check_check_mount, shell=True, capture_output=True, text=True)
            if "mounted" in result.stdout:
                logger.info({"message": f"Unmounting existing NFS mount on {static_ip}"})
                subprocess.run(
                    f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} 'sudo umount {config['mount_point']}'",
                    shell=True, check=True
                )

            mount_command = (
                f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} "
                f"'sudo mount -t nfs -o vers=4,soft,timeo=100 {config['nfs_server']}:{config['nfs_share']} {config['mount_point']}'"
            )
            subprocess.run(mount_command, shell=True, check=True)

            verify_command = (
                f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} "
                f"'test -f {config['agent_binary']} && test -f {config['slcm_binary']} && echo exists'"
            )
            result = subprocess.run(verify_command, shell=True, capture_output=True, text=True)
            if "exists" not in result.stdout:
                raise FileNotFoundError(f"Binaries not found at {config['agent_binary']} or {config['slcm_binary']}")

            if config["persist_nfs_mount"]:
                persist_command = (
                    f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} "
                    f"'echo \"{config['nfs_server']}:{config['nfs_share']} {config['mount_point']} nfs defaults 0 0\" | sudo tee -a /etc/fstab'"
                )
                subprocess.run(persist_command, shell=True, check=True)
            return
        except subprocess.CalledProcessError as e:
            logger.warning({"message": f"NFS mount attempt {attempt+1} failed for {static_ip}", "error": str(e)})
            time.sleep(5)
    raise RuntimeError(f"Failed to mount NFS on {static_ip}")

def deploy_agent(static_ip):
    """Deploy the agent binary from the NFS share."""
    try:
        check_command = (
            f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} "
            f"'pgrep -f {config['agent_binary']} && echo running'"
        )
        result = subprocess.run(check_command, shell=True, capture_output=True, text=True)
        if "running" in result.stdout:
            logger.info({"message": f"Agent already running on {static_ip}"})
            return

        # Configure agent with Kafka settings
        config_command = (
            f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} "
            f"'echo \"KAFKA_BOOTSTRAP_SERVERS={','.join(config['kafka_brokers'])}\" > /tmp/agent_config.env && "
            f"echo \"DEPLOYMENT_TOPIC=deployment\" >> /tmp/agent_config.env'"
        )
        subprocess.run(config_command, shell=True, check=True)

        ssh_command = (
            f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} "
            f"'sudo env $(cat /tmp/agent_config.env) nohup {config['agent_binary']} > /tmp/agent.log 2>&1 &'"
        )
        subprocess.run(ssh_command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        logger.error({"message": f"Agent deployment failed on {static_ip}", "error": str(e)})
        raise

def validate_vm_health(static_ip):
    """Validate VM health by checking agent and SLCM status."""
    try:
        check_agent = (
            f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} "
            f"'pgrep -f {config['agent_binary']} && echo running'"
        )
        result_agent = subprocess.run(check_agent, shell=True, capture_output=True, text=True)
        if "running" not in result_agent.stdout:
            raise RuntimeError(f"Agent not running on {static_ip}")

        check_slcm = (
            f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} "
            f"'pgrep -f {config['slcm_binary']} && echo running'"
        )
        result_slcm = subprocess.run(check_slcm, shell=True, capture_output=True, text=True)
        if "running" not in result_slcm.stdout:
            raise RuntimeError(f"SLCM not running on {static_ip}")
    except subprocess.CalledProcessError as e:
        logger.error({"message": f"VM health check failed on {static_ip}", "error": str(e)})
        raise

def publish_deployment_status(request_id, agent_id, ip, status_type, error=None):
    """Publish VM provisioning status to vmops topic."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=config["kafka_brokers"],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        message = {
            "type": status_type,
            "request_id": request_id,
            "agent_id": agent_id,
            "vm_ip": ip
        }
        if error:
            message = {
                "type": status_type,
                "request_id": request_id,
                "reason": error
            }
        producer.send(config["vmops_topic"], message)
        producer.flush()
        producer.close()
        logger.info({"message": f"Published {status_type} status", "ip": ip, "request_id": request_id})
    except Exception as e:
        logger.error({"message": f"Failed to publish {status_type} status for {ip}", "error": str(e)})

def cleanup(static_ip, error):
    """Clean up resources on failure."""
    try:
        unmount_command = (
            f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} "
            f"'mountpoint -q {config['mount_point']} && sudo umount {config['mount_point']}'"
        )
        subprocess.run(unmount_command, shell=True, capture_output=True)
        logger.info({"message": f"Unmounted NFS on {static_ip}"})
    except Exception as e:
        logger.warning({"message": f"Failed to unmount NFS on {static_ip}", "error": str(e)})

    try:
        stop_agent = (
            f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} "
            f"'pkill -f {config['agent_binary']}'"
        )
        subprocess.run(stop_agent, shell=True, capture_output=True)
        logger.info({"message": f"Stopped agent on {static_ip}"})
    except Exception as e:
        logger.warning({"message": f"Failed to stop agent on {static_ip}", "error": str(e)})

    try:
        stop_slcm = (
            f"ssh -o StrictHostKeyChecking=no vagrant@{static_ip} "
            f"'pkill -f {config['slcm_binary']}'"
        )
        subprocess.run(stop_slcm, shell=True, capture_output=True)
        logger.info({"message": f"Stopped SLCM on {static_ip}"})
    except Exception as e:
        logger.warning({"message": f"Failed to stop SLCM on {static_ip}", "error": str(e)})

    try:
        producer = KafkaProducer(
            bootstrap_servers=config["kafka_brokers"],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        producer.send(config["available_ips_topic"], {"ip": static_ip, "status": "released", "reason": error})
        producer.flush()
        producer.close()
        logger.info({"message": f"Released IP {static_ip}"})
    except Exception as e:
        logger.error({"message": f"Failed to release IP {static_ip}", "error": str(e)})

if __name__ == "__main__":
    try:
        create_topics()
        check_dependencies()
        # Start SLCM in a VM
        slcm_thread = threading.Thread(target=start_slcm, daemon=True)
        slcm_thread.start()
        # Start health check
        health_thread = threading.Thread(target=check_slcm_status, daemon=True)
        health_thread.start()
        # Start Flask app
        flask_thread = threading.Thread(target=lambda: app.run(host="0.0.0.0", port=config["health_port"]), daemon=True)
        flask_thread.start()
        # Start consuming VM requests
        consume_deploy_requests()
    except Exception as e:
        logger.error({"message": f"Bootstrap Service failed: {str(e)}"})
        raise