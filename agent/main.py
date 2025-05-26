# import os
# import time
# import json
# import subprocess
# import psutil
# import socket
# import yaml
# from confluent_kafka import Producer, Consumer, KafkaError
# from uuid import uuid4
# from confluent_kafka.admin import AdminClient, NewTopic
# import logging


# # Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# # Kafka configuration

# KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVER','')

# # List of topics to create
# TOPICS = [
#     'metric',
#     'agent-deployment',
#     'modelregistry-request',
#     'modelregistry-response',
#     'deployed-weburls'
# ]

# def create_topics():
#     """Create Kafka topics if they don't exist."""
#     admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

#     # Define topic configurations
#     new_topics = [
#         NewTopic(topic, num_partitions=1, replication_factor=1)
#         for topic in TOPICS
#     ]

#     # Create topics
#     fs = admin_client.create_topics(new_topics)

#     # Wait for each operation to complete
#     for topic, f in fs.items():
#         try:
#             f.result()  # The result() method blocks until the operation is complete
#             logger.info(f"Topic {topic} created successfully")
#         except Exception as e:
#             logger.error(f"Failed to create topic {topic}: {e}")

# # Kafka configuration
# KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
# AGENT_ID = os.getenv('AGENT_ID', f'agent_{uuid4()}')
# AGENT_IP = os.getenv('AGENT_IP', socket.gethostbyname(socket.gethostname()))

# # Kafka topics
# METRIC_TOPIC = 'metric'
# AGENT_DEPLOYMENT_TOPIC = 'agent-deployment'
# MODELREGISTRY_REQUEST_TOPIC = 'modelregistry-request'
# MODELREGISTRY_RESPONSE_TOPIC = 'modelregistry-response'
# DEPLOYED_WEBURLS_TOPIC = 'deployed-weburls'

# # Metrics interval
# METRIC_INTERVAL_SECONDS = 10

# def get_system_metrics():
#     """Collect CPU and memory usage metrics."""
#     return {
#         'agent_id': AGENT_ID,
#         'cpu_percent': psutil.cpu_percent(interval=1),
#         'memory_percent': psutil.virtual_memory().percent,
#         'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
#     }

# def send_metrics(producer):
#     """Send system metrics to Kafka topic."""
#     while True:
#         metrics = get_system_metrics()
#         producer.produce(
#             METRIC_TOPIC,
#             key=AGENT_ID.encode('utf-8'),
#             value=json.dumps(metrics).encode('utf-8')
#         )
#         producer.flush()
#         time.sleep(METRIC_INTERVAL_SECONDS)

# def read_config_yaml(config_path):
#     """Read and parse config.yaml file."""
#     try:
#         with open(config_path, 'r') as f:
#             return yaml.safe_load(f) or {}
#     except Exception as e:
#         print(f"Error reading config.yaml at {config_path}: {e}")
#         return {}

# def launch_executable(executable_path, config):
#     """Launch executable with environment variables from config."""
#     env = os.environ.copy()
#     env.update({k: str(v) for k, v in config.items()})
#     try:
#         process = subprocess.Popen(
#             [executable_path],
#             env=env,
#             stdout=subprocess.PIPE,
#             stderr=subprocess.PIPE,
#             text=True
#         )
#         return process
#     except Exception as e:
#         print(f"Error launching executable {executable_path}: {e}")
#         return None

# def construct_web_url(port):
#     """Construct web URL from AGENT_IP and port."""
#     return f"http://{AGENT_IP}:{port}"

# def main():
#     # Initialize Kafka producer
#     producer_conf = {
#         'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
#     }
#     producer = Producer(producer_conf)

#     # Initialize Kafka consumer
#     consumer_conf = {
#         'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
#         'group.id': f'agent_group_{AGENT_ID}',
#         'auto.offset.reset': 'latest'
#     }
#     consumer = Consumer(consumer_conf)
#     consumer.subscribe([AGENT_DEPLOYMENT_TOPIC, MODELREGISTRY_RESPONSE_TOPIC])

#     # Start metrics loop in background
#     import threading
#     metrics_thread = threading.Thread(target=send_metrics, args=(producer,), daemon=True)
#     metrics_thread.start()

#     # Store the last deployment message for reference
#     last_deployment_message = None

#     # Main loop to process Kafka messages
#     while True:
#         msg = consumer.poll(timeout=1.0)
#         if msg is None:
#             continue
#         if msg.error():
#             if msg.error().code() == KafkaError._PARTITION_EOF:
#                 continue
#             else:
#                 print(f"Kafka error: {msg.error()}")
#                 continue

#         topic = msg.topic()
#         value = json.loads(msg.value().decode('utf-8'))
#         key = msg.key().decode('utf-8') if msg.key() else None

#         if topic == AGENT_DEPLOYMENT_TOPIC:
#             # Store deployment message and check if this agent is assigned
#             last_deployment_message = value
#             if AGENT_ID in value:
#                 client_id = value['client_id']
#                 model_id = value['model_id']

#                 # Send request to model registry
#                 request = {
#                     'agent_id': AGENT_ID,
#                     'model_id': model_id
#                 }
#                 producer.produce(
#                     MODELREGISTRY_REQUEST_TOPIC,
#                     key=AGENT_ID.encode('utf-8'),
#                     value=json.dumps(request).encode('utf-8')
#                 )
#                 producer.flush()

#         elif topic == MODELREGISTRY_RESPONSE_TOPIC and value.get('agent_id') == AGENT_ID:
#             if not last_deployment_message or AGENT_ID not in last_deployment_message:
#                 continue

#             model_id = value['model_id']
#             paths = value['paths']
#             client_id = last_deployment_message['client_id']
#             executables = last_deployment_message[AGENT_ID]['executables']

#             # Process each executable
#             for exe in executables:
#                 executable_id = exe['id']
#                 return_url = exe['return_url']

#                 # Find matching path
#                 for path in paths:
#                     if os.path.basename(path) == executable_id:
#                         # Find config.yaml in the same directory
#                         config_path = os.path.join(os.path.dirname(path), 'config.yaml')
#                         config = read_config_yaml(config_path)

#                         # Launch executable
#                         process = launch_executable(path, config)
#                         if process:
#                             if return_url:
#                                 # Get port from config or default to 8080
#                                 port = config.get('PORT', '8080')
#                                 web_url = construct_web_url(port)

#                                 # Send deployment result
#                                 result = {
#                                     'agent_id': AGENT_ID,
#                                     'executable_id': executable_id,
#                                     'web_url': web_url,
#                                     'client_id': client_id
#                                 }
#                                 producer.produce(
#                                     DEPLOYED_WEBURLS_TOPIC,
#                                     key=client_id.encode('utf-8'),
#                                     value=json.dumps(result).encode('utf-8')
#                                 )
#                                 producer.flush()

# if __name__ == '__main__':
#     create_topics()
#     main()

import os
import time
import json
import subprocess
import psutil
import socket
import yaml
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from uuid import uuid4
import logging
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

load_dotenv('/exports/system-services/')

# Configure logging
log_file = 'kafka_agent.log'
handler = RotatingFileHandler(log_file, maxBytes=10485760, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '')
AGENT_ID = os.getenv('AGENT_ID', f'agent_{uuid4()}')
AGENT_IP = os.getenv('AGENT_IP', socket.gethostbyname(socket.gethostname()))

# Kafka topics
METRIC_TOPIC = 'metric'
AGENT_DEPLOYMENT_TOPIC = 'agent-deployment'
MODELREGISTRY_REQUEST_TOPIC = 'modelregistry-request'
MODELREGISTRY_RESPONSE_TOPIC = 'modelregistry-response'
DEPLOYED_WEBURLS_TOPIC = 'deployed-weburls'

# Metrics interval
METRIC_INTERVAL_SECONDS = 10

def create_topics():
    """Create Kafka topics if they don't exist."""
    logger.info("Starting Kafka topic creation")
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    # Define topic configurations
    new_topics = [
        NewTopic(topic, num_partitions=1, replication_factor=1)
        for topic in [
            'metric',
            'agent-deployment',
            'modelregistry-request',
            'modelregistry-response',
            'deployed-weburls'
        ]
    ]

    # Create topics
    fs = admin_client.create_topics(new_topics)

    # Wait for each operation to complete
    for topic, f in fs.items():
        try:
            f.result()
            logger.info(f"Successfully created topic {topic}")
        except Exception as e:
            logger.error(f"Failed to create topic {topic}: {str(e)}")

def get_system_metrics():
    """Collect CPU and memory usage metrics."""
    logger.debug("Collecting system metrics")
    metrics = {
        'agent_id': AGENT_ID,
        'cpu_percent': psutil.cpu_percent(interval=1),
        'memory_percent': psutil.virtual_memory().percent,
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    }
    logger.debug(f"Collected metrics: {metrics}")
    return metrics

def send_metrics(producer):
    """Send system metrics to Kafka topic."""
    logger.info("Starting metrics collection loop")
    while True:
        try:
            metrics = get_system_metrics()
            producer.produce(
                METRIC_TOPIC,
                key=AGENT_ID.encode('utf-8'),
                value=json.dumps(metrics).encode('utf-8')
            )
            producer.flush()
            logger.info(f"Sent metrics to {METRIC_TOPIC}")
        except Exception as e:
            logger.error(f"Error sending metrics: {str(e)}")
        time.sleep(METRIC_INTERVAL_SECONDS)

def read_config_yaml(config_path):
    """Read and parse config.yaml file."""
    logger.info(f"Reading config file: {config_path}")
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f) or {}
            logger.debug(f"Config content: {config}")
            return config
    except Exception as e:
        logger.error(f"Error reading config.yaml at {config_path}: {str(e)}")
        return {}

def launch_executable(executable_path, config):
    """Launch executable with environment variables from config."""
    logger.info(f"Launching executable: {executable_path}")
    env = os.environ.copy()
    env.update({k: str(v) for k, v in config.items()})
    try:
        process = subprocess.Popen(
            [executable_path],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        logger.info(f"Successfully launched executable: {executable_path}")
        return process
    except Exception as e:
        logger.error(f"Error launching executable {executable_path}: {str(e)}")
        return None

def construct_web_url(port):
    """Construct web URL from AGENT_IP and port."""
    web_url = f"http://{AGENT_IP}:{port}"
    logger.debug(f"Constructed web URL: {web_url}")
    return web_url

def main():
    logger.info("Starting main agent process")
    
    # Initialize Kafka producer
    producer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    }
    producer = Producer(producer_conf)
    logger.info("Kafka producer initialized")

    # Initialize Kafka consumer
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': f'agent_group_{AGENT_ID}',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([AGENT_DEPLOYMENT_TOPIC, MODELREGISTRY_RESPONSE_TOPIC])
    logger.info(f"Subscribed to topics: {AGENT_DEPLOYMENT_TOPIC}, {MODELREGISTRY_RESPONSE_TOPIC}")

    # Start metrics loop in background
    import threading
    metrics_thread = threading.Thread(target=send_metrics, args=(producer,), daemon=True)
    metrics_thread.start()
    logger.info("Started metrics collection thread")

    # Store the last deployment message for reference
    last_deployment_message = None

    # Main loop to process Kafka messages
    logger.info("Entering main message processing loop")
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logger.error(f"Kafka error: {msg.error()}")
                continue

        topic = msg.topic()
        value = json.loads(msg.value().decode('utf-8'))
        key = msg.key().decode('utf-8') if msg.key() else None
        logger.debug(f"Received message from {topic}: {value}")

        if topic == AGENT_DEPLOYMENT_TOPIC:
            logger.info("Processing deployment message")
            last_deployment_message = value
            if AGENT_ID in value:
                client_id = value['client_id']
                model_id = value['model_id']
                logger.info(f"Agent {AGENT_ID} assigned for model {model_id}")

                # Send request to model registry
                request = {
                    'agent_id': AGENT_ID,
                    'model_id': model_id
                }
                try:
                    producer.produce(
                        MODELREGISTRY_REQUEST_TOPIC,
                        key=AGENT_ID.encode('utf-8'),
                        value=json.dumps(request).encode('utf-8')
                    )
                    producer.flush()
                    logger.info(f"Sent model registry request for model {model_id}")
                except Exception as e:
                    logger.error(f"Error sending model registry request: {str(e)}")

        elif topic == MODELREGISTRY_RESPONSE_TOPIC and value.get('agent_id') == AGENT_ID:
            logger.info("Processing model registry response")
            if not last_deployment_message or AGENT_ID not in last_deployment_message:
                logger.warning("No valid deployment message found, skipping")
                continue

            model_id = value['model_id']
            paths = value['paths']
            client_id = last_deployment_message['client_id']
            executables = last_deployment_message[AGENT_ID]['executables']
            logger.debug(f"Processing executables for model {model_id}")

            # Process each executable
            for exe in executables:
                executable_id = exe['id']
                return_url = exe['return_url']
                logger.info(f"Processing executable {executable_id}")

                # Find matching path
                for path in paths:
                    if os.path.basename(path) == executable_id:
                        # Find config.yaml in the same directory
                        config_path = os.path.join(os.path.dirname(path), 'config.yaml')
                        config = read_config_yaml(config_path)

                        # Launch executable
                        process = launch_executable(path, config)
                        if process:
                            if return_url:
                                # Get port from config or default to 8080
                                port = config.get('PORT', '9999')
                                web_url = construct_web_url(port)

                                # Send deployment result
                                result = {
                                    'agent_id': AGENT_ID,
                                    'executable_id': executable_id,
                                    'web_url': web_url,
                                    'client_id': client_id
                                }
                                try:
                                    producer.produce(
                                        DEPLOYED_WEBURLS_TOPIC,
                                        key=client_id.encode('utf-8'),
                                        value=json.dumps(result).encode('utf-8')
                                    )
                                    producer.flush()
                                    logger.info(f"Sent deployment result to {DEPLOYED_WEBURLS_TOPIC}: {web_url}")
                                except Exception as e:
                                    logger.error(f"Error sending deployment result: {str(e)}")

if __name__ == '__main__':
    logger.info("Initializing Kafka agent")
    create_topics()
    main()
