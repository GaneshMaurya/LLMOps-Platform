#!/usr/bin/env python3
"""
Server Life Cycle Manager (SLCM)

This module implements the SLCM component of the MLOps platform, responsible for:
- VM and service provisioning
- Scaling decisions based on metrics
- Handling deployment requests
- Managing service lifecycle
- Load balancing

The SLCM communicates with other components via Kafka and makes decisions about 
where to deploy services based on current load and availability.
"""

import json
import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Set, Tuple, Any

from confluent_kafka import Producer, Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Configure logging
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler = RotatingFileHandler('slcm.log', maxBytes=10485760, backupCount=5)
log_handler.setFormatter(log_formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

logger = logging.getLogger('SLCM')
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)
logger.addHandler(console_handler)

# Constants
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')  # Changed to 29092
METRICS_TOPIC = os.environ.get('METRICS_TOPIC', 'metrics')
VMOPS_TOPIC = os.environ.get('VMOPS_TOPIC', 'vmops')
DEPLOYMENT_TOPIC = os.environ.get('DEPLOYMENT_TOPIC', 'deployment')
REQUEST_TOPIC = os.environ.get('REQUEST_TOPIC', 'frontend_requests')
RESPONSE_TOPIC = os.environ.get('RESPONSE_TOPIC', 'frontend_responses')
METRICS_COLLECTION_INTERVAL = int(os.environ.get('METRICS_COLLECTION_INTERVAL', '30'))  # seconds
SCALING_INTERVAL = int(os.environ.get('SCALING_INTERVAL', '60'))  # seconds
VM_REQUEST_TIMEOUT = int(os.environ.get('VM_REQUEST_TIMEOUT', '300'))  # seconds
DEPLOYMENT_TIMEOUT = int(os.environ.get('DEPLOYMENT_TIMEOUT', '180'))  # seconds
MAX_CPU_THRESHOLD = float(os.environ.get('MAX_CPU_THRESHOLD', '80.0'))  # percentage
MIN_CPU_THRESHOLD = float(os.environ.get('MIN_CPU_THRESHOLD', '20.0'))  # percentage
MAX_MEMORY_THRESHOLD = float(os.environ.get('MAX_MEMORY_THRESHOLD', '80.0'))  # percentage
ACCEPTABLE_RESPONSE_TIME = float(os.environ.get('ACCEPTABLE_RESPONSE_TIME', '500.0'))  # milliseconds

class ServiceState(Enum):
    """States for service lifecycle management"""
    PENDING = "pending"  # Service is scheduled for deployment
    DEPLOYING = "deploying"  # Service is being deployed
    RUNNING = "running"  # Service is active and healthy
    SCALING = "scaling"  # Service is being scaled
    UNHEALTHY = "unhealthy"  # Service is running but not responding correctly
    FAILED = "failed"  # Service deployment or execution failed
    TERMINATING = "terminating"  # Service is being shut down
    TERMINATED = "terminated"  # Service has been terminated

class AgentState(Enum):
    """States for agent lifecycle management"""
    PROVISIONING = "provisioning"  # Agent is being set up
    READY = "ready"  # Agent is ready to accept deployments
    BUSY = "busy"  # Agent is at capacity
    UNHEALTHY = "unhealthy"  # Agent is not responding correctly
    DECOMMISSIONING = "decommissioning"  # Agent is being shut down

@dataclass
class ServiceMetrics:
    """Metrics collected from a service instance"""
    service_id: str
    instance_id: str
    agent_id: str
    cpu_usage: float  # percentage
    memory_usage: float  # percentage
    network_in: float  # bytes/sec
    network_out: float  # bytes/sec
    request_count: int
    error_count: int
    avg_response_time: float  # milliseconds
    last_updated: datetime = field(default_factory=datetime.now)

    def is_healthy(self) -> bool:
        """Check if service metrics indicate a healthy state"""
        if self.cpu_usage > MAX_CPU_THRESHOLD:
            return False
        if self.memory_usage > MAX_MEMORY_THRESHOLD:
            return False
        if self.error_count > 0:  # Simple check; could be more sophisticated
            return False
        if self.avg_response_time > ACCEPTABLE_RESPONSE_TIME:
            return False
        # Check if metrics are fresh (updated within the last 2 collection intervals)
        if (datetime.now() - self.last_updated).total_seconds() > (2 * METRICS_COLLECTION_INTERVAL):
            return False
        return True

@dataclass
class AgentMetrics:
    """Metrics collected from an agent"""
    agent_id: str
    vm_ip: str
    cpu_usage: float  # percentage
    memory_usage: float  # percentage
    disk_usage: float  # percentage
    network_in: float  # bytes/sec
    network_out: float  # bytes/sec
    service_count: int
    last_updated: datetime = field(default_factory=datetime.now)

    def is_healthy(self) -> bool:
        """Check if agent metrics indicate a healthy state"""
        if self.cpu_usage > MAX_CPU_THRESHOLD:
            return False
        if self.memory_usage > MAX_MEMORY_THRESHOLD:
            return False
        if self.disk_usage > 90.0:  # 90% disk usage is concerning
            return False
        # Check if metrics are fresh (updated within the last 2 collection intervals)
        if (datetime.now() - self.last_updated).total_seconds() > (2 * METRICS_COLLECTION_INTERVAL):
            return False
        return True

    def can_accept_deployment(self) -> bool:
        """Check if agent can accept a new deployment"""
        return (self.cpu_usage < 70.0 and  # Leave some headroom
                self.memory_usage < 70.0 and
                self.disk_usage < 80.0 and
                self.is_healthy())

@dataclass
class ServiceInstance:
    """Represents a running service instance"""
    service_id: str
    instance_id: str
    agent_id: str
    vm_ip: str
    port: int
    state: ServiceState
    binary_path: str
    service_type: str  # e.g., "inference_api", "web_ui", "microservice"
    model_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)
    endpoint_url: Optional[str] = None
    metrics: Optional[ServiceMetrics] = None
    
    def get_endpoint(self) -> str:
        """Get service endpoint URL"""
        if self.endpoint_url:
            return self.endpoint_url
        return f"http://{self.vm_ip}:{self.port}"
    
    def is_healthy(self) -> bool:
        """Check if service instance is healthy"""
        if self.state != ServiceState.RUNNING:
            return False
        if not self.metrics:
            return False
        return self.metrics.is_healthy()

@dataclass
class Agent:
    """Represents an agent running on a VM"""
    agent_id: str
    vm_ip: str
    state: AgentState
    created_at: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)
    service_instances: Dict[str, str] = field(default_factory=dict)  # instance_id: service_id
    metrics: Optional[AgentMetrics] = None
    
    def is_healthy(self) -> bool:
        """Check if agent is healthy"""
        if self.state not in [AgentState.READY, AgentState.BUSY]:
            return False
        if not self.metrics:
            return False
        return self.metrics.is_healthy()
    
    def can_accept_deployment(self) -> bool:
        """Check if agent can accept a new deployment"""
        if not self.is_healthy() or self.state != AgentState.READY:
            return False
        if not self.metrics:
            return False
        return self.metrics.can_accept_deployment()

@dataclass
class ModelInfo:
    """Information about a model registered in the system"""
    model_id: str
    name: str
    version: str
    inference_api_binary: str
    web_ui_binary: str
    microservices: Dict[str, str]  # service_name: binary_path
    created_at: datetime = field(default_factory=datetime.now)

@dataclass
class DeploymentRequest:
    """Request to deploy a model or service"""
    request_id: str
    model_id: str
    user_id: str
    requested_at: datetime = field(default_factory=datetime.now)
    components: List[str] = field(default_factory=list)  # List of components to deploy

class ServerLifeCycleManager:
    """
    Server Life Cycle Manager (SLCM) for the MLOps platform
    
    Responsible for:
    - VM and service provisioning
    - Scaling decisions based on metrics
    - Handling deployment requests
    - Managing service lifecycle
    - Load balancing
    """
    
    def __init__(self):
        """Initialize SLCM instance"""
        logger.info("Initializing Server Life Cycle Manager")
        
        # Internal state
        self.agents: Dict[str, Agent] = {}  # agent_id: Agent
        self.service_instances: Dict[str, ServiceInstance] = {}  # instance_id: ServiceInstance
        self.models: Dict[str, ModelInfo] = {}  # model_id: ModelInfo
        self.pending_vm_requests: Dict[str, datetime] = {}  # request_id: request_time
        self.pending_deployments: Dict[str, DeploymentRequest] = {}  # request_id: DeploymentRequest
        self.active_models: Dict[str, Set[str]] = {}  # model_id: Set[instance_id]
        self.deployment_lock = threading.Lock()
        self.scaling_lock = threading.Lock()
        
        # Kafka configuration
        self.producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
        }
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'auto.offset.reset': 'earliest'
        }
        
        # Create Kafka admin client for topic management
        self.admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        
        # Create Kafka producer
        self.kafka_producer = Producer(self.producer_config)
        
        # Create Kafka consumers
        self.metrics_consumer = Consumer({
            **self.consumer_config,
            'group.id': 'slcm-metrics'
        })
        self.vmops_consumer = Consumer({
            **self.consumer_config,
            'group.id': 'slcm-vmops'
        })
        self.deployment_consumer = Consumer({
            **self.consumer_config,
            'group.id': 'slcm-deployment'
        })
        self.request_consumer = Consumer({
            **self.consumer_config,
            'group.id': 'slcm-requests'
        })
        
        # Ensure all required topics exist
        self._ensure_topics([
            METRICS_TOPIC,
            VMOPS_TOPIC,
            DEPLOYMENT_TOPIC,
            REQUEST_TOPIC,
            RESPONSE_TOPIC
        ])
        
        # Subscribe consumers to their respective topics
        self.metrics_consumer.subscribe([METRICS_TOPIC])
        self.vmops_consumer.subscribe([VMOPS_TOPIC])
        self.deployment_consumer.subscribe([DEPLOYMENT_TOPIC])
        self.request_consumer.subscribe([REQUEST_TOPIC])
        
        # Prometheus metrics
        self.active_agents = Gauge('slcm_active_agents', 'Number of active agents')
        self.active_services = Gauge('slcm_active_services', 'Number of active service instances')
        self.pending_deployments_gauge = Gauge('slcm_pending_deployments', 'Number of pending deployments')
        self.deployment_duration = Histogram('slcm_deployment_duration_seconds', 'Time taken to deploy a service')
        self.deployment_successes = Counter('slcm_deployment_successes', 'Number of successful deployments')
        self.deployment_failures = Counter('slcm_deployment_failures', 'Number of failed deployments')
        self.vm_provision_duration = Histogram('slcm_vm_provision_duration_seconds', 'Time taken to provision a VM')
        
        # Start Prometheus metrics server
        start_http_server(8000)
        
        # Start background threads
        self.stop_event = threading.Event()
        self.threads = []
    
    def _ensure_topics(self, topics: List[str]):
        """Ensure all specified Kafka topics exist, creating them if necessary"""
        try:
            # Get current topics
            metadata = self.admin_client.list_topics(timeout=10)
            existing_topics = set(metadata.topics.keys())
            
            # Identify topics that need to be created
            topics_to_create = [topic for topic in topics if topic not in existing_topics]
            
            if topics_to_create:
                logger.info(f"Creating Kafka topics: {topics_to_create}")
                
                # Create new topic configurations
                new_topics = [
                    NewTopic(
                        topic=topic,
                        num_partitions=3,  # Default partitions
                        replication_factor=1,  # Default replication
                        config={'retention.ms': '604800000'}  # 7 days retention
                    ) for topic in topics_to_create
                ]
                
                # Create topics
                fs = self.admin_client.create_topics(new_topics)
                
                # Wait for topic creation to complete
                for topic, f in fs.items():
                    try:
                        f.result()  # Wait for completion
                        logger.info(f"Topic {topic} created successfully")
                    except Exception as e:
                        logger.error(f"Failed to create topic {topic}: {e}")
                        raise
                        
        except Exception as e:
            logger.error(f"Error ensuring Kafka topics: {e}")
            raise
    
    def start(self):
        """Start all background threads and services"""
        logger.info("Starting Server Life Cycle Manager services")
        
        # Start consumer threads
        metrics_thread = threading.Thread(target=self._metrics_consumer_loop)
        metrics_thread.daemon = True
        metrics_thread.start()
        self.threads.append(metrics_thread)
        
        vmops_thread = threading.Thread(target=self._vmops_consumer_loop)
        vmops_thread.daemon = True
        vmops_thread.start()
        self.threads.append(vmops_thread)
        
        deployment_thread = threading.Thread(target=self._deployment_consumer_loop)
        deployment_thread.daemon = True
        deployment_thread.start()
        self.threads.append(deployment_thread)
        
        request_thread = threading.Thread(target=self._request_consumer_loop)
        request_thread.daemon = True
        request_thread.start()
        self.threads.append(request_thread)
        
        # Start scaling and cleanup thread
        scaling_thread = threading.Thread(target=self._scaling_loop)
        scaling_thread.daemon = True
        scaling_thread.start()
        self.threads.append(scaling_thread)
        
        cleanup_thread = threading.Thread(target=self._cleanup_loop)
        cleanup_thread.daemon = True
        cleanup_thread.start()
        self.threads.append(cleanup_thread)
        
        logger.info("All SLCM services started successfully")
    
    def stop(self):
        """Stop all background threads and services"""
        logger.info("Stopping Server Life Cycle Manager")
        self.stop_event.set()
        
        # Close Kafka consumers
        for consumer in [self.metrics_consumer, self.vmops_consumer, 
                        self.deployment_consumer, self.request_consumer]:
            try:
                consumer.close()
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")
        
        # Flush producer
        try:
            self.kafka_producer.flush(timeout=5)
        except Exception as e:
            logger.error(f"Error flushing producer: {e}")
        
        for thread in self.threads:
            thread.join(timeout=5)
        
        logger.info("All SLCM services stopped")
    
    def _metrics_consumer_loop(self):
        """Process metrics from services and agents"""
        logger.info("Starting metrics consumer loop")
        
        while not self.stop_event.is_set():
            try:
                msg = self.metrics_consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Metrics consumer error: {msg.error()}")
                    continue
                
                try:
                    metric_data = json.loads(msg.value().decode('utf-8'))
                    self._process_metric(metric_data)
                except Exception as e:
                    logger.error(f"Error processing metric: {e}", exc_info=True)
            
            except Exception as e:
                logger.error(f"Error in metrics consumer loop: {e}", exc_info=True)
                time.sleep(1)  # Avoid tight loop in case of repeated errors
    
    def _process_metric(self, metric_data: Dict[str, Any]):
        """Process a single metric message"""
        metric_type = metric_data.get('type')
        
        if metric_type == 'service':
            self._update_service_metrics(metric_data)
        elif metric_type == 'agent':
            self._update_agent_metrics(metric_data)
        else:
            logger.warning(f"Unknown metric type: {metric_type}")
    
    def _update_service_metrics(self, metric_data: Dict[str, Any]):
        """Update metrics for a service instance"""
        instance_id = metric_data.get('instance_id')
        if not instance_id or instance_id not in self.service_instances:
            logger.warning(f"Received metrics for unknown service instance: {instance_id}")
            return
        
        service_instance = self.service_instances[instance_id]
        
        metrics = ServiceMetrics(
            service_id=metric_data.get('service_id'),
            instance_id=instance_id,
            agent_id=metric_data.get('agent_id'),
            cpu_usage=metric_data.get('cpu_usage', 0.0),
            memory_usage=metric_data.get('memory_usage', 0.0),
            network_in=metric_data.get('network_in', 0.0),
            network_out=metric_data.get('network_out', 0.0),
            request_count=metric_data.get('request_count', 0),
            error_count=metric_data.get('error_count', 0),
            avg_response_time=metric_data.get('avg_response_time', 0.0),
            last_updated=datetime.now()
        )
        
        service_instance.metrics = metrics
        service_instance.last_updated = datetime.now()
        
        # Update service state based on metrics
        if not metrics.is_healthy() and service_instance.state == ServiceState.RUNNING:
            logger.warning(f"Service instance {instance_id} is unhealthy")
            service_instance.state = ServiceState.UNHEALTHY
        elif metrics.is_healthy() and service_instance.state == ServiceState.UNHEALTHY:
            logger.info(f"Service instance {instance_id} is healthy again")
            service_instance.state = ServiceState.RUNNING
    
    def _update_agent_metrics(self, metric_data: Dict[str, Any]):
        """Update metrics for an agent"""
        agent_id = metric_data.get('agent_id')
        if not agent_id:
            logger.warning(f"Received agent metrics without agent_id")
            return
        
        # Create agent if it doesn't exist (auto-registration)
        if agent_id not in self.agents:
            logger.info(f"Auto-registering new agent: {agent_id}")
            agent = Agent(
                agent_id=agent_id,
                vm_ip=metric_data.get('vm_ip', 'unknown'),
                state=AgentState.READY
            )
            self.agents[agent_id] = agent
            self.active_agents.inc()
        
        agent = self.agents[agent_id]
        
        metrics = AgentMetrics(
            agent_id=agent_id,
            vm_ip=metric_data.get('vm_ip', agent.vm_ip),
            cpu_usage=metric_data.get('cpu_usage', 0.0),
            memory_usage=metric_data.get('memory_usage', 0.0),
            disk_usage=metric_data.get('disk_usage', 0.0),
            network_in=metric_data.get('network_in', 0.0),
            network_out=metric_data.get('network_out', 0.0),
            service_count=metric_data.get('service_count', len(agent.service_instances)),
            last_updated=datetime.now()
        )
        
        agent.metrics = metrics
        agent.last_updated = datetime.now()
        
        # Update agent state based on metrics
        previous_state = agent.state
        if not metrics.is_healthy() and agent.state in [AgentState.READY, AgentState.BUSY]:
            logger.warning(f"Agent {agent_id} is unhealthy")
            agent.state = AgentState.UNHEALTHY
        elif metrics.is_healthy():
            if agent.state == AgentState.UNHEALTHY:
                logger.info(f"Agent {agent_id} is healthy again")
                # Determine if agent is busy or ready based on resource usage
                if metrics.can_accept_deployment():
                    agent.state = AgentState.READY
                else:
                    agent.state = AgentState.BUSY
            elif agent.state == AgentState.READY and not metrics.can_accept_deployment():
                agent.state = AgentState.BUSY
            elif agent.state == AgentState.BUSY and metrics.can_accept_deployment():
                agent.state = AgentState.READY
        
        if previous_state != agent.state:
            logger.info(f"Agent {agent_id} state changed from {previous_state} to {agent.state}")
    
    def _vmops_consumer_loop(self):
        """Process VM operations messages"""
        logger.info("Starting VM operations consumer loop")
        
        while not self.stop_event.is_set():
            try:
                msg = self.vmops_consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"VM ops consumer error: {msg.error()}")
                    continue
                
                try:
                    message = json.loads(msg.value().decode('utf-8'))
                    self._process_vmops_message(message)
                except Exception as e:
                    logger.error(f"Error processing VM ops message: {e}", exc_info=True)
            
            except Exception as e:
                logger.error(f"Error in VM ops consumer loop: {e}", exc_info=True)
                time.sleep(1)
    
    def _process_vmops_message(self, message: Dict[str, Any]):
        """Process a VM operations message"""
        message_type = message.get('type')
        
        if message_type == 'vm_provisioned':
            # A new VM has been provisioned by the bootstrap server
            request_id = message.get('request_id')
            agent_id = message.get('agent_id')
            vm_ip = message.get('vm_ip')
            
            if request_id in self.pending_vm_requests:
                # Calculate provisioning duration
                request_time = self.pending_vm_requests.pop(request_id)
                duration = (datetime.now() - request_time).total_seconds()
                self.vm_provision_duration.observe(duration)
                
                logger.info(f"VM provisioned: agent_id={agent_id}, vm_ip={vm_ip}, duration={duration:.2f}s")
                
                # Register the new agent
                agent = Agent(
                    agent_id=agent_id,
                    vm_ip=vm_ip,
                    state=AgentState.READY
                )
                self.agents[agent_id] = agent
                self.active_agents.inc()
                
                # Process any pending deployments that were waiting for a VM
                self._process_pending_deployments()
            else:
                logger.warning(f"Received VM provisioned message for unknown request: {request_id}")
        
        elif message_type == 'vm_provision_failed':
            # VM provisioning failed
            request_id = message.get('request_id')
            reason = message.get('reason', 'Unknown reason')
            
            if request_id in self.pending_vm_requests:
                self.pending_vm_requests.pop(request_id)
                logger.error(f"VM provisioning failed for request {request_id}: {reason}")
                
                # Mark related deployments as failed
                self._handle_vm_provision_failure(request_id, reason)
            else:
                logger.warning(f"Received VM provision failure for unknown request: {request_id}")
        
        elif message_type == 'agent_heartbeat':
            # Agent heartbeat message
            agent_id = message.get('agent_id')
            vm_ip = message.get('vm_ip')
            
            if agent_id in self.agents:
                agent = self.agents[agent_id]
                agent.last_updated = datetime.now()
                
                # Update IP if changed
                if agent.vm_ip != vm_ip:
                    logger.info(f"Agent {agent_id} IP changed from {agent.vm_ip} to {vm_ip}")
                    agent.vm_ip = vm_ip
            else:
                # Auto-register new agent
                logger.info(f"Auto-registering new agent from heartbeat: {agent_id}")
                agent = Agent(
                    agent_id=agent_id,
                    vm_ip=vm_ip,
                    state=AgentState.READY
                )
                self.agents[agent_id] = agent
                self.active_agents.inc()
        
        else:
            logger.warning(f"Unknown VM ops message type: {message_type}")
    
    def _deployment_consumer_loop(self):
        """Process deployment messages from agents"""
        logger.info("Starting deployment consumer loop")
        
        while not self.stop_event.is_set():
            try:
                msg = self.deployment_consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Deployment consumer error: {msg.error()}")
                    continue
                
                try:
                    message = json.loads(msg.value().decode('utf-8'))
                    self._process_deployment_message(message)
                except Exception as e:
                    logger.error(f"Error processing deployment message: {e}", exc_info=True)
            
            except Exception as e:
                logger.error(f"Error in deployment consumer loop: {e}", exc_info=True)
                time.sleep(1)
    
    def _process_deployment_message(self, message: Dict[str, Any]):
        """Process a deployment message from an agent"""
        message_type = message.get('type')
        
        if message_type == 'deployment_success':
            # Service deployment succeeded
            instance_id = message.get('instance_id')
            agent_id = message.get('agent_id')
            port = message.get('port')
            
            if instance_id in self.service_instances:
                service = self.service_instances[instance_id]
                service.state = ServiceState.RUNNING
                service.port = port
                service.last_updated = datetime.now()
                
                # Update endpoint URL
                service.endpoint_url = f"http://{service.vm_ip}:{port}"
                
                logger.info(f"Service {instance_id} deployed successfully on agent {agent_id}")
                self.deployment_successes.inc()
                
                # Update parent deployment request if this was part of one
                self._check_deployment_request_completion(service)
            else:
                logger.warning(f"Received deployment success for unknown service: {instance_id}")
        
        elif message_type == 'deployment_failed':
            # Service deployment failed
            instance_id = message.get('instance_id')
            agent_id = message.get('agent_id')
            reason = message.get('reason', 'Unknown reason')
            
            if instance_id in self.service_instances:
                service = self.service_instances[instance_id]
                service.state = ServiceState.FAILED
                service.last_updated = datetime.now()
                
                logger.error(f"Service {instance_id} deployment failed on agent {agent_id}: {reason}")
                self.deployment_failures.inc()
                
                # Update parent deployment request if this was part of one
                self._check_deployment_request_completion(service)
            else:
                logger.warning(f"Received deployment failure for unknown service: {instance_id}")
        
        elif message_type == 'service_terminated':
            # Service was terminated
            instance_id = message.get('instance_id')
            agent_id = message.get('agent_id')
            
            if instance_id in self.service_instances:
                service = self.service_instances[instance_id]
                service.state = ServiceState.TERMINATED
                service.last_updated = datetime.now()
                
                logger.info(f"Service {instance_id} terminated on agent {agent_id}")
                
                # Remove service from agent's list
                if agent_id in self.agents:
                    agent = self.agents[agent_id]
                    if instance_id in agent.service_instances:
                        del agent.service_instances[instance_id]
                
                # Remove from active models list
                if service.model_id and service.model_id in self.active_models:
                    if instance_id in self.active_models[service.model_id]:
                        self.active_models[service.model_id].remove(instance_id)
                
                # Will be cleaned up later by the cleanup thread
            else:
                logger.warning(f"Received termination for unknown service: {instance_id}")
        
        else:
            logger.warning(f"Unknown deployment message type: {message_type}")
    
    def _request_consumer_loop(self):
        """Process frontend requests"""
        logger.info("Starting frontend request consumer loop")
        
        while not self.stop_event.is_set():
            try:
                msg = self.request_consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Request consumer error: {msg.error()}")
                    continue
                
                try:
                    request = json.loads(msg.value().decode('utf-8'))
                    self._process_frontend_request(request)
                except Exception as e:
                    logger.error(f"Error processing frontend request: {e}", exc_info=True)
            
            except Exception as e:
                logger.error(f"Error in frontend request consumer loop: {e}", exc_info=True)
                time.sleep(1)
    
    def _process_frontend_request(self, request: Dict[str, Any]):
        """Process a request from the frontend"""
        request_type = request.get('type')
        request_id = request.get('request_id', str(uuid.uuid4()))
        
        if request_type == 'model_inference':
            # Request to perform inference on a model
            model_id = request.get('model_id')
            user_id = request.get('user_id')
            
            if not model_id:
                self._send_frontend_response(request_id, user_id, False, "Model ID is required", None)
                return
            
            if model_id not in self.models:
                self._send_frontend_response(request_id, user_id, False, f"Model {model_id} not found", None)
                return
            
            logger.info(f"Received inference request for model {model_id} from user {user_id}")
            
            # Check if model is already deployed and has capacity
            if self._has_available_deployment(model_id):
                # Find best instance to handle this request
                instance = self._get_best_instance(model_id)
                
                if instance:
                    logger.info(f"Using existing instance {instance.instance_id} for model {model_id}")
                    self._send_frontend_response(
                        request_id, 
                        user_id, 
                        True, 
                        "Model is available", 
                        {"endpoint": instance.get_endpoint()}
                    )
                    return
            
            # Need to deploy model
            with self.deployment_lock:
                # Check again in case another thread deployed it while we were waiting
                if self._has_available_deployment(model_id):
                    instance = self._get_best_instance(model_id)
                    if instance:
                        logger.info(f"Using existing instance {instance.instance_id} for model {model_id}")
                        self._send_frontend_response(
                            request_id, 
                            user_id, 
                            True, 
                            "Model is available", 
                            {"endpoint": instance.get_endpoint()}
                        )
                        return
                
                # Create deployment request
                deployment_req = DeploymentRequest(
                    request_id=request_id,
                    model_id=model_id,
                    user_id=user_id,
                    components=["inference_api", "web_ui"]  # Default to both components
                )
                
                self.pending_deployments[request_id] = deployment_req
                self.pending_deployments_gauge.inc()
                
                logger.info(f"Created deployment request {request_id} for model {model_id}")
                
                # Respond to user that deployment is in progress
                self._send_frontend_response(
                    request_id,
                    user_id,
                    True,
                    "Model deployment in progress",
                    {"status": "deploying"}
                )
                
                # Start deployment process
                self._process_deployment_request(deployment_req)
        
        else:
            logger.warning(f"Unknown frontend request type: {request_type}")
            self._send_frontend_response(
                request_id,
                request.get('user_id', 'unknown'),
                False,
                f"Unknown request type: {request_type}",
                None
            )
    
    def _send_frontend_response(self, request_id: str, user_id: str, success: bool, message: str, data: Optional[Dict[str, Any]]):
        """Send response back to frontend"""
        response = {
            'request_id': request_id,
            'user_id': user_id,
            'success': success,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'data': data
        }
        
        try:
            self.kafka_producer.produce(
                RESPONSE_TOPIC,
                value=json.dumps(response).encode('utf-8')
            )
            logger.debug(f"Sent response for request {request_id}: {message}")
        except Exception as e:
            logger.error(f"Error producing response to {RESPONSE_TOPIC}: {e}")
    
    def _has_available_deployment(self, model_id: str) -> bool:
        """Check if a model has an available deployment"""
        if model_id not in self.active_models:
            return False
        
        # Check for healthy instances
        for instance_id in self.active_models[model_id]:
            if instance_id in self.service_instances:
                instance = self.service_instances[instance_id]
                if instance.is_healthy() and instance.state == ServiceState.RUNNING:
                    # Check if the instance has capacity
                    if instance.metrics and instance.metrics.avg_response_time < ACCEPTABLE_RESPONSE_TIME:
                        return True
        
        return False
    
    def _get_best_instance(self, model_id: str) -> Optional[ServiceInstance]:
        """Get the best instance for a model based on load and response time"""
        if model_id not in self.active_models:
            return None
        
        best_instance = None
        best_score = float('inf')  # Lower is better
        
        for instance_id in self.active_models[model_id]:
            if instance_id in self.service_instances:
                instance = self.service_instances[instance_id]
                
                if instance.is_healthy() and instance.state == ServiceState.RUNNING:
                    if instance.metrics:
                        # Calculate a score based on CPU usage and response time
                        # This is a simple heuristic that could be improved
                        score = (instance.metrics.cpu_usage / 100.0) * instance.metrics.avg_response_time
                        
                        if score < best_score:
                            best_score = score
                            best_instance = instance
        
        return best_instance
    
    def _process_deployment_request(self, request: DeploymentRequest):
        """Process a model deployment request"""
        model_id = request.model_id
        request_id = request.request_id
        
        if model_id not in self.models:
            logger.error(f"Cannot deploy unknown model: {model_id}")
            self._send_frontend_response(
                request_id,
                request.user_id,
                False,
                f"Model {model_id} not found",
                None
            )
            return
        
        model_info = self.models[model_id]
        start_time = datetime.now()
        
        logger.info(f"Processing deployment request {request_id} for model {model_id}")
        
        # Determine required components
        components = request.components
        if not components:
            components = ["inference_api", "web_ui"]  # Default to both
        
        # Find suitable agents for each component
        deployment_plan = {}
        needed_vms = []
        
        for component in components:
            agent = self._find_suitable_agent(component, model_id)
            
            if agent:
                deployment_plan[component] = agent.agent_id
            else:
                needed_vms.append(component)
        
        # Request new VMs if needed
        if needed_vms:
            vm_request_id = str(uuid.uuid4())
            self.pending_vm_requests[vm_request_id] = datetime.now()
            
            logger.info(f"Requesting new VM for components: {needed_vms}, request_id: {vm_request_id}")
            
            # Send VM request message to bootstrap server
            try:
                self.kafka_producer.produce(
                    VMOPS_TOPIC,
                    value=json.dumps({
                        'type': 'vm_request',
                        'request_id': vm_request_id,
                        'deployment_request_id': request_id,
                        'needed_for': needed_vms
                    }).encode('utf-8')
                )
            except Exception as e:
                logger.error(f"Error producing VM request to {VMOPS_TOPIC}: {e}")
            
            # This request will be picked up when VM is provisioned
            return
        
        # Deploy to available agents
        for component, agent_id in deployment_plan.items():
            self._deploy_component(request_id, model_id, component, agent_id, request.user_id)
        
        # Record deployment time
        duration = (datetime.now() - start_time).total_seconds()
        self.deployment_duration.observe(duration)
        
        logger.info(f"Deployment plan created for request {request_id}, duration: {duration:.2f}s")
    
    def _find_suitable_agent(self, component: str, model_id: str) -> Optional[Agent]:
        """Find a suitable agent for a component deployment"""
        best_agent = None
        best_score = float('inf')  # Lower is better
        
        for agent_id, agent in self.agents.items():
            if agent.state != AgentState.READY or not agent.is_healthy():
                continue
            
            if not agent.metrics or not agent.metrics.can_accept_deployment():
                continue
            
            # Calculate a score based on resource usage
            # Lower score is better
            cpu_score = agent.metrics.cpu_usage
            memory_score = agent.metrics.memory_usage
            service_count_score = agent.metrics.service_count * 10  # Weight service count higher
            
            score = cpu_score + memory_score + service_count_score
            
            if score < best_score:
                best_score = score
                best_agent = agent
        
        return best_agent
    
    def _deploy_component(self, request_id: str, model_id: str, component: str, agent_id: str, user_id: str):
        """Deploy a model component to an agent"""
        if agent_id not in self.agents:
            logger.error(f"Cannot deploy to unknown agent: {agent_id}")
            return
        
        agent = self.agents[agent_id]
        model_info = self.models[model_id]
        
        # Get binary path
        binary_path = None
        if component == "inference_api":
            binary_path = model_info.inference_api_binary
        elif component == "web_ui":
            binary_path = model_info.web_ui_binary
        elif component in model_info.microservices:
            binary_path = model_info.microservices[component]
        else:
            logger.error(f"Unknown component: {component}")
            return
        
        # Create service instance
        instance_id = str(uuid.uuid4())
        service = ServiceInstance(
            service_id=f"{model_id}-{component}",
            instance_id=instance_id,
            agent_id=agent_id,
            vm_ip=agent.vm_ip,
            port=0,  # Will be set when deployment completes
            state=ServiceState.DEPLOYING,
            binary_path=binary_path,
            service_type=component,
            model_id=model_id
        )
        
        self.service_instances[instance_id] = service
        
        # Add to agent's service list
        agent.service_instances[instance_id] = service.service_id
        
        # Add to active models list
        if model_id not in self.active_models:
            self.active_models[model_id] = set()
        self.active_models[model_id].add(instance_id)
        
        # Update metrics
        self.active_services.inc()
        
        logger.info(f"Deploying component {component} of model {model_id} to agent {agent_id}, instance_id: {instance_id}")
        
        # Send deployment request to agent
        try:
            self.kafka_producer.produce(
                DEPLOYMENT_TOPIC,
                value=json.dumps({
                    'type': 'deploy_service',
                    'agent_id': agent_id,
                    'instance_id': instance_id,
                    'service_id': service.service_id,
                    'binary_path': binary_path,
                    'service_type': component,
                    'model_id': model_id,
                    'request_id': request_id,
                    'user_id': user_id
                }).encode('utf-8')
            )
        except Exception as e:
            logger.error(f"Error producing deployment request to {DEPLOYMENT_TOPIC}: {e}")
    
    def _check_deployment_request_completion(self, service: ServiceInstance):
        """Check if a deployment request is complete and notify the user"""
        # Find the parent deployment request
        for request_id, request in list(self.pending_deployments.items()):
            if request.model_id == service.model_id:
                # Check if all components are deployed
                model_id = request.model_id
                user_id = request.user_id
                
                # Count successful web_ui deployments
                web_ui_instances = [
                    inst for inst_id, inst in self.service_instances.items()
                    if inst.model_id == model_id and 
                    inst.service_type == "web_ui" and 
                    inst.state == ServiceState.RUNNING
                ]
                
                if web_ui_instances:
                    # We have at least one web UI instance, consider the deployment complete
                    web_ui = web_ui_instances[0]  # Take the first one
                    
                    # Notify the user
                    self._send_frontend_response(
                        request_id,
                        user_id,
                        True,
                        "Model deployed successfully",
                        {"endpoint": web_ui.get_endpoint()}
                    )
                    
                    # Remove from pending
                    self.pending_deployments.pop(request_id, None)
                    self.pending_deployments_gauge.dec()
                    
                    logger.info(f"Deployment request {request_id} completed successfully")
                
                elif any(inst.state == ServiceState.FAILED 
                         for inst_id, inst in self.service_instances.items()
                         if inst.model_id == model_id):
                    # Some component failed
                    self._send_frontend_response(
                        request_id,
                        user_id,
                        False,
                        "Model deployment failed",
                        {"status": "failed"}
                    )
                    
                    # Remove from pending
                    self.pending_deployments.pop(request_id, None)
                    self.pending_deployments_gauge.dec()
                    
                    logger.error(f"Deployment request {request_id} failed")
    
    def _handle_vm_provision_failure(self, vm_request_id: str, reason: str):
        """Handle VM provisioning failure"""
        # Find related deployment requests
        affected_deployments = []
        
        for request_id, request in list(self.pending_deployments.items()):
            # This is a simplified relationship between VM requests and deployments
            # In a real system, you would track this relationship more explicitly
            affected_deployments.append(request_id)
        
        for request_id in affected_deployments:
            if request_id in self.pending_deployments:
                request = self.pending_deployments[request_id]
                
                self._send_frontend_response(
                    request_id,
                    request.user_id,
                    False,
                    f"Deployment failed: Could not provision VM: {reason}",
                    {"status": "failed"}
                )
                
                self.pending_deployments.pop(request_id, None)
                self.pending_deployments_gauge.dec()
    
    def _process_pending_deployments(self):
        """Process pending deployments that were waiting for VMs"""
        with self.deployment_lock:
            for request_id, request in list(self.pending_deployments.items()):
                self._process_deployment_request(request)
    
    def _scaling_loop(self):
        """Periodically check for scaling needs"""
        logger.info("Starting scaling loop")
        
        while not self.stop_event.is_set():
            try:
                with self.scaling_lock:
                    self._check_scaling_needs()
            except Exception as e:
                logger.error(f"Error in scaling loop: {e}", exc_info=True)
            
            # Wait for next scaling check
            time.sleep(SCALING_INTERVAL)
    
    def _check_scaling_needs(self):
        """Check if any services need scaling up or down"""
        # Group services by model and type
        services_by_model = {}
        
        for service in self.service_instances.values():
            if service.state != ServiceState.RUNNING or not service.metrics:
                continue
            
            model_id = service.model_id
            service_type = service.service_type
            
            if model_id not in services_by_model:
                services_by_model[model_id] = {}
            
            if service_type not in services_by_model[model_id]:
                services_by_model[model_id][service_type] = []
            
            services_by_model[model_id][service_type].append(service)
        
        # Check each model and service type for scaling needs
        for model_id, service_types in services_by_model.items():
            for service_type, services in service_types.items():
                self._check_service_type_scaling(model_id, service_type, services)
    
    def _check_service_type_scaling(self, model_id: str, service_type: str, services: List[ServiceInstance]):
        """Check if a specific service type for a model needs scaling"""
        # Calculate average metrics
        total_cpu = 0
        total_memory = 0
        total_response_time = 0
        unhealthy_count = 0
        
        for service in services:
            if service.metrics:
                total_cpu += service.metrics.cpu_usage
                total_memory += service.metrics.memory_usage
                total_response_time += service.metrics.avg_response_time
                
                if not service.is_healthy():
                    unhealthy_count += 1
        
        count = len(services)
        
        if count > 0:
            avg_cpu = total_cpu / count
            avg_memory = total_memory / count
            avg_response_time = total_response_time / count
            
            # Scale up if needed
            should_scale_up = (
                avg_cpu > MAX_CPU_THRESHOLD or 
                avg_memory > MAX_MEMORY_THRESHOLD or 
                avg_response_time > ACCEPTABLE_RESPONSE_TIME or
                unhealthy_count > 0  # Replace unhealthy instances
            )
            
            if should_scale_up:
                logger.info(f"Scaling up {service_type} for model {model_id}: "
                           f"CPU={avg_cpu:.1f}%, Memory={avg_memory:.1f}%, "
                           f"Resp={avg_response_time:.1f}ms, Unhealthy={unhealthy_count}")
                
                # Deploy a new instance
                self._scale_up_service(model_id, service_type)
            
            # Scale down if needed (only if we have more than one instance)
            elif count > 1:
                should_scale_down = (
                    avg_cpu < MIN_CPU_THRESHOLD and
                    avg_memory < MIN_CPU_THRESHOLD and
                    avg_response_time < (ACCEPTABLE_RESPONSE_TIME / 2)
                )
                
                if should_scale_down:
                    logger.info(f"Scaling down {service_type} for model {model_id}: "
                               f"CPU={avg_cpu:.1f}%, Memory={avg_memory:.1f}%, "
                               f"Resp={avg_response_time:.1f}ms")
                    
                    # Terminate the instance with the highest resource usage
                    self._scale_down_service(services)
    
    def _scale_up_service(self, model_id: str, service_type: str):
        """Scale up a service by deploying a new instance"""
        if model_id not in self.models:
            logger.error(f"Cannot scale unknown model: {model_id}")
            return
        
        # Find a suitable agent
        agent = self._find_suitable_agent(service_type, model_id)
        
        if agent:
            # Deploy to this agent
            self._deploy_component(
                request_id=str(uuid.uuid4()),
                model_id=model_id,
                component=service_type,
                agent_id=agent.agent_id,
                user_id="system_scaling"
            )
        else:
            # Need a new VM
            vm_request_id = str(uuid.uuid4())
            self.pending_vm_requests[vm_request_id] = datetime.now()
            
            logger.info(f"Requesting new VM for scaling {service_type} of model {model_id}")
            
            # Send VM request message
            try:
                self.kafka_producer.produce(
                    VMOPS_TOPIC,
                    value=json.dumps({
                        'type': 'vm_request',
                        'request_id': vm_request_id,
                        'needed_for': [service_type],
                        'model_id': model_id,
                        'scaling': True
                    }).encode('utf-8')
                )
            except Exception as e:
                logger.error(f"Error producing VM request to {VMOPS_TOPIC}: {e}")
    
    def _scale_down_service(self, services: List[ServiceInstance]):
        """Scale down by terminating the service with highest resource usage"""
        # Find the instance with the highest resource usage
        target = None
        highest_usage = -1
        
        for service in services:
            if service.metrics:
                usage = service.metrics.cpu_usage + service.metrics.memory_usage
                if usage > highest_usage:
                    highest_usage = usage
                    target = service
        
        if target:
            logger.info(f"Scaling down: terminating service {target.instance_id}")
            
            # Mark for termination
            target.state = ServiceState.TERMINATING
            
            # Send termination request to agent
            try:
                self.kafka_producer.produce(
                    DEPLOYMENT_TOPIC,
                    value=json.dumps({
                        'type': 'terminate_service',
                        'agent_id': target.agent_id,
                        'instance_id': target.instance_id
                    }).encode('utf-8')
                )
            except Exception as e:
                logger.error(f"Error producing termination request to {DEPLOYMENT_TOPIC}: {e}")
    
    def _cleanup_loop(self):
        """Periodically clean up terminated services and check for timed out operations"""
        logger.info("Starting cleanup loop")
        
        while not self.stop_event.is_set():
            try:
                self._cleanup_terminated_services()
                self._check_timed_out_operations()
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}", exc_info=True)
            
            # Wait before next cleanup
            time.sleep(60)  # Run every minute
    
    def _cleanup_terminated_services(self):
        """Clean up terminated service instances"""
        now = datetime.now()
        to_remove = []
        
        for instance_id, service in self.service_instances.items():
            if service.state == ServiceState.TERMINATED:
                # Keep terminated services for a while for debugging
                if (now - service.last_updated).total_seconds() > 3600:  # 1 hour
                    to_remove.append(instance_id)
        
        for instance_id in to_remove:
            service = self.service_instances.pop(instance_id)
            logger.info(f"Cleaned up terminated service {instance_id}")
            
            # Update metrics
            self.active_services.dec()
            
            # Remove from model's active instances if present
            if service.model_id and service.model_id in self.active_models:
                if instance_id in self.active_models[service.model_id]:
                    self.active_models[service.model_id].remove(instance_id)
    
    def _check_timed_out_operations(self):
        """Check for timed out operations"""
        now = datetime.now()
        
        # Check VM requests
        timed_out_vms = []
        for request_id, request_time in self.pending_vm_requests.items():
            if (now - request_time).total_seconds() > VM_REQUEST_TIMEOUT:
                timed_out_vms.append(request_id)
        
        for request_id in timed_out_vms:
            logger.warning(f"VM request {request_id} timed out")
            self.pending_vm_requests.pop(request_id)
            self._handle_vm_provision_failure(request_id, "Request timed out")
        
        # Check deployment requests
        timed_out_deploys = []
        for request_id, request in self.pending_deployments.items():
            if (now - request.requested_at).total_seconds() > DEPLOYMENT_TIMEOUT:
                timed_out_deploys.append(request_id)
        
        for request_id in timed_out_deploys:
            request = self.pending_deployments.pop(request_id)
            self.pending_deployments_gauge.dec()
            
            logger.warning(f"Deployment request {request_id} for model {request.model_id} timed out")
            
            self._send_frontend_response(
                request_id,
                request.user_id,
                False,
                "Deployment timed out",
                {"status": "timeout"}
            )
    
    def register_model(self, model_info: ModelInfo):
        """Register a new model in the system"""
        self.models[model_info.model_id] = model_info
        logger.info(f"Registered model {model_info.model_id}: {model_info.name} v{model_info.version}")
    
    def deregister_model(self, model_id: str):
        """Deregister a model from the system"""
        if model_id in self.models:
            self.models.pop(model_id)
            logger.info(f"Deregistered model {model_id}")
            
            # Terminate all instances of this model
            for instance_id, service in list(self.service_instances.items()):
                if service.model_id == model_id:
                    service.state = ServiceState.TERMINATING
                    
                    # Send termination request
                    try:
                        self.kafka_producer.produce(
                            DEPLOYMENT_TOPIC,
                            value=json.dumps({
                                'type': 'terminate_service',
                                'agent_id': service.agent_id,
                                'instance_id': instance_id
                            }).encode('utf-8')
                        )
                    except Exception as e:
                        logger.error(f"Error producing termination request to {DEPLOYMENT_TOPIC}: {e}")
            
            # Remove from active models
            if model_id in self.active_models:
                del self.active_models[model_id]
        else:
            logger.warning(f"Attempted to deregister unknown model: {model_id}")

def main():
    """Main entry point"""
    logger.info("Starting SLCM")
    
    # Create and start SLCM
    slcm = ServerLifeCycleManager()
    
    try:
        slcm.start()
        
        # Register some test models
        # In a real system, these would come from the Model Registry
        test_model = ModelInfo(
            model_id="test-model-1",
            name="Test Model",
            version="1.0.0",
            inference_api_binary="/models/test-model-1/inference_api",
            web_ui_binary="/models/test-model-1/web_ui",
            microservices={
                "feature_extractor": "/models/test-model-1/feature_extractor"
            }
        )
        slcm.register_model(test_model)
        
        # Keep running until interrupted
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Shutting down SLCM")
    finally:
        slcm.stop()

if __name__ == "__main__":
    main()