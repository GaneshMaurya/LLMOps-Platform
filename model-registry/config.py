import os

NFS_BASE_DIR = os.path.abspath("/exports/user_applications/")
MODELS_BASE_DIR = os.path.abspath("/exports/models/")
DB_PATH = os.path.abspath("/exports/system_services/model-registry/model_registry.db")
ENV_PATH = os.path.abspath("/exports/system_services/.env")
KAFKA_BOOTSTRAP_SERVERS = "192.168.33.61:29092"