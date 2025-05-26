import os
import subprocess
import json
import logging
import threading
import time
import signal
import socket
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from pythonjsonlogger import jsonlogger
import yaml
from flask import Flask, jsonify
from http import HTTPStatus
import re

# Flask app for health endpoint
app = Flask(__name__)

# Configuration Management: Load config from YAML or environment variables
CONFIG_FILE = os.getenv("PROVISIONING_CONFIG", "/etc/provisioning/config.yaml")
DEFAULT_CONFIG = {
    "kafka_brokers": ["localhost:29092"],  # Aligned with Bootstrap and SLCM
    "available_ips_topic": "available_ips",
    "allocated_ips_topic": "allocated_ips",
    "static_ip_pool": [
        "192.168.56.101",
        "192.168.56.102",
        "192.168.56.103",
        "192.168.56.104",
        "192.168.56.105"
    ],
    "publish_interval": 10,
    "health_port": 5001  # Changed to avoid conflict with Bootstrap
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
        config["available_ips_topic"] = os.getenv("AVAILABLE_IPS_TOPIC", config["available_ips_topic"])
        config["allocated_ips_topic"] = os.getenv("ALLOCATED_IPS_TOPIC", config["allocated_ips_topic"])
        config["health_port"] = int(os.getenv("HEALTH_PORT", config["health_port"]))
        for ip in config["static_ip_pool"]:
            if not re.match(r"^192\.168\.56\.\d{1,3}$", ip) or int(ip.split(".")[-1]) < 1 or int(ip.split(".")[-1]) > 255:
                raise ValueError(f"Invalid IP in static_ip_pool: {ip}")
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

# Track allocated IPs and thread safety
allocated_ips = set()
allocated_ips_lock = threading.Lock()

# Health Check: Service status
health_status = {
    "kafka_connected": False,
    "available_ips": len(config["static_ip_pool"]),
    "last_error": None,
    "start_time": time.time()
}

def check_kafka_connectivity():
    """Check connectivity to Kafka brokers."""
    try:
        for broker in config["kafka_brokers"]:
            host, port = broker.split(":")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, int(port)))
            sock.close()
            if result != 0:
                raise ConnectionError(f"Cannot connect to Kafka broker {broker}")
        return True
    except Exception as e:
        logger.error({"message": f"Kafka connectivity check failed: {str(e)}"})
        return False

@app.route("/health")
def health():
    """Health check endpoint."""
    health_status["kafka_connected"] = check_kafka_connectivity()
    health_status["available_ips"] = len(config["static_ip_pool"]) - len(allocated_ips)
    status = {
        "status": "healthy" if health_status["kafka_connected"] else "unhealthy",
        "kafka_connected": health_status["kafka_connected"],
        "available_ips": health_status["available_ips"],
        "uptime_seconds": int(time.time() - health_status["start_time"]),
        "last_error": health_status["last_error"]
    }
    return jsonify(status), HTTPStatus.OK if status["status"] == "healthy" else HTTPStatus.SERVICE_UNAVAILABLE

def create_topic():
    """Create the Kafka topics if they don't exist."""
    admin_client = KafkaAdminClient(bootstrap_servers=config["kafka_brokers"])
    try:
        topics = [
            NewTopic(name=config["available_ips_topic"], num_partitions=1, replication_factor=2),
            NewTopic(name=config["allocated_ips_topic"], num_partitions=1, replication_factor=2)
        ]
        admin_client.create_topics(topics)
        logger.info({"message": f"Topics created", "topics": [config["available_ips_topic"], config["allocated_ips_topic"]]})
    except TopicAlreadyExistsError:
        logger.info({"message": f"Topics already exist", "topics": [config["available_ips_topic"], config["allocated_ips_topic"]]})
    except Exception as e:
        logger.error({"message": f"Failed to create topics: {str(e)}"})
        raise
    finally:
        admin_client.close()

def publish_available_ips():
    """Publish available IPs to the Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers=config["kafka_brokers"],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    try:
        while True:
            available_count = len(config["static_ip_pool"]) - len(allocated_ips)
            if available_count == 0:
                logger.error({"message": "IP pool exhausted"})
            elif available_count < len(config["static_ip_pool"]) * 0.2:
                logger.warning({"message": f"IP pool low", "available_ips": available_count})
            
            for ip in config["static_ip_pool"]:
                with allocated_ips_lock:
                    if ip not in allocated_ips:
                        for attempt in range(3):
                            try:
                                producer.send(config["available_ips_topic"], {"ip": ip})
                                logger.info({"message": "Published IP", "ip": ip, "topic": config["available_ips_topic"]})
                                break
                            except Exception as e:
                                logger.warning({"message": f"Failed to publish IP {ip}, attempt {attempt+1}", "error": str(e)})
                                if attempt == 2:
                                    logger.error({"message": f"Failed to publish IP {ip} after retries", "error": str(e)})
                                    health_status["last_error"] = str(e)
                                time.sleep(5)
            producer.flush()
            time.sleep(config["publish_interval"])
    except (KeyboardInterrupt, SystemExit):
        logger.info({"message": "Stopping IP publisher"})
    except Exception as e:
        logger.error({"message": f"IP publisher failed: {str(e)}"})
        health_status["last_error"] = str(e)
        raise
    finally:
        producer.close()

def consume_allocated_ips():
    """Consume allocated IPs from the Kafka topic and mark them as allocated."""
    for attempt in range(3):
        try:
            consumer = KafkaConsumer(
                config["allocated_ips_topic"],
                bootstrap_servers=config["kafka_brokers"],
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                group_id="provision_server",
                auto_offset_reset="earliest",
                enable_auto_commit=False
            )
            break
        except Exception as e:
            logger.warning({"message": f"Failed to connect to Kafka for allocated IPs, attempt {attempt+1}", "error": str(e)})
            if attempt == 2:
                logger.error({"message": f"Failed to connect to Kafka for allocated IPs after retries", "error": str(e)})
                health_status["last_error"] = str(e)
                raise
            time.sleep(5)
    
    try:
        for message in consumer:
            try:
                ip = message.value.get("ip")
                if ip:
                    mark_ip_as_allocated(ip)
                    consumer.commit()
            except Exception as e:
                logger.error({"message": f"Failed to process allocated IP message", "error": str(e)})
                health_status["last_error"] = str(e)
    except (KeyboardInterrupt, SystemExit):
        logger.info({"message": "Stopping allocated IP consumer"})
    except Exception as e:
        logger.error({"message": f"Allocated IP consumer failed: {str(e)}"})
        health_status["last_error"] = str(e)
        raise
    finally:
        consumer.close()

def consume_released_ips():
    """Consume released IPs from the Kafka topic and mark them as available."""
    for attempt in range(3):
        try:
            consumer = KafkaConsumer(
                config["available_ips_topic"],
                bootstrap_servers=config["kafka_brokers"],
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                group_id="provision_server_released",
                auto_offset_reset="earliest",
                enable_auto_commit=False
            )
            break
        except Exception as e:
            logger.warning({"message": f"Failed to connect to Kafka for released IPs, attempt {attempt+1}", "error": str(e)})
            if attempt == 2:
                logger.error({"message": f"Failed to connect to Kafka for released IPs after retries", "error": str(e)})
                health_status["last_error"] = str(e)
                raise
            time.sleep(5)
    
    try:
        for message in consumer:
            try:
                if message.value.get("status") == "released":
                    ip = message.value.get("ip")
                    if ip:
                        mark_ip_as_available(ip)
                        consumer.commit()
            except Exception as e:
                logger.error({"message": f"Failed to process released IP message", "error": str(e)})
                health_status["last_error"] = str(e)
    except (KeyboardInterrupt, SystemExit):
        logger.info({"message": "Stopping released IP consumer"})
    except Exception as e:
        logger.error({"message": f"Released IP consumer failed: {str(e)}"})
        health_status["last_error"] = str(e)
        raise
    finally:
        consumer.close()

def mark_ip_as_allocated(ip):
    """Mark an IP as allocated."""
    if not re.match(r"^192\.168\.56\.\d{1,3}$", ip) or int(ip.split(".")[-1]) < 1 or int(ip.split(".")[-1]) > 255:
        logger.warning({"message": f"Invalid IP skipped", "ip": ip})
        return
    if ip not in config["static_ip_pool"]:
        logger.warning({"message": f"IP not in pool", "ip": ip})
        return
    with allocated_ips_lock:
        if ip not in allocated_ips:
            allocated_ips.add(ip)
            logger.info({"message": f"IP marked as allocated", "ip": ip})
        else:
            logger.info({"message": f"IP already allocated", "ip": ip})

def mark_ip_as_available(ip):
    """Mark an IP as available."""
    if not re.match(r"^192\.168\.56\.\d{1,3}$", ip) or int(ip.split(".")[-1]) < 1 or int(ip.split(".")[-1]) > 255:
        logger.warning({"message": f"Invalid IP skipped", "ip": ip})
        return
    if ip not in config["static_ip_pool"]:
        logger.warning({"message": f"IP not in pool", "ip": ip})
        return
    with allocated_ips_lock:
        if ip in allocated_ips:
            allocated_ips.remove(ip)
            logger.info({"message": f"IP marked as available", "ip": ip})
        else:
            logger.info({"message": f"IP already available", "ip": ip})

def handle_shutdown(signum, frame):
    """Handle shutdown signals."""
    logger.info({"message": "Received shutdown signal"})
    raise SystemExit

if __name__ == "__main__":
    logger.info({"message": "Starting Provisioning Service"})
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
    
    try:
        create_topic()
        flask_thread = threading.Thread(
            target=lambda: app.run(host="0.0.0.0", port=config["health_port"], debug=False),
            daemon=True
        )
        flask_thread.start()
        
        publisher_thread = threading.Thread(target=publish_available_ips)
        allocated_consumer_thread = threading.Thread(target=consume_allocated_ips)
        released_consumer_thread = threading.Thread(target=consume_released_ips)
        
        publisher_thread.start()
        allocated_consumer_thread.start()
        released_consumer_thread.start()
        
        publisher_thread.join()
        allocated_consumer_thread.join()
        released_consumer_thread.join()
        
    except (SystemExit, KeyboardInterrupt):
        logger.info({"message": "Provisioning Service shutting down"})
    except Exception as e:
        logger.error({"message": f"Provisioning Service failed: {str(e)}"})
        raise