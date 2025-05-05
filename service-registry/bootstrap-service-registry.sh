#!/bin/bash

# filepath: /exports/applications/service-registry/bootstrap-service-registry.sh

# Define the absolute path to the .env file
ENV_FILE="/exports/applications/.env"

# Load environment variables from the .env file
if [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' "$ENV_FILE" | xargs)
    echo "Environment variables loaded from $ENV_FILE"
else
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi

# Define the absolute paths
SERVICE_REGISTRY_SCRIPT="/exports/applications/service-registry/registry.py"
VENV_DIR="/exports/applications/service-registry/venv"
REQUIREMENTS_FILE="/exports/applications/service-registry/requirements.txt"

# Ensure service-registry.log has 777 permissions
LOG_FILE="/exports/applications/service-registry/service-registry.log"

if [ -f "$LOG_FILE" ]; then
    CURRENT_PERM=$(stat -c "%a" "$LOG_FILE")
    if [ "$CURRENT_PERM" -ne 777 ]; then
        chmod 777 "$LOG_FILE"
        echo "Permissions for $LOG_FILE updated to 777."
    else
        echo "Permissions for $LOG_FILE are already 777."
    fi
else
    echo "Warning: Log file $LOG_FILE not found."
fi

# Check if the registry.py script exists
if [ ! -f "$SERVICE_REGISTRY_SCRIPT" ]; then
    echo "Error: registry.py not found at $SERVICE_REGISTRY_SCRIPT"
    exit 1
fi

# Create a virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment at $VENV_DIR..."
    python3 -m venv "$VENV_DIR"
    if [ $? -ne 0 ]; then
        echo "Error: Failed to create virtual environment."
        exit 1
    fi
fi

# Activate the virtual environment
source "$VENV_DIR/bin/activate"

# Install required libraries
if [ -f "$REQUIREMENTS_FILE" ]; then
    echo "Installing required libraries from $REQUIREMENTS_FILE..."
    pip install --upgrade pip
    pip install -r "$REQUIREMENTS_FILE"
    if [ $? -ne 0 ]; then
        echo "Error: Failed to install required libraries."
        deactivate
        exit 1
    fi
else
    echo "Warning: requirements.txt not found at $REQUIREMENTS_FILE. Skipping library installation."
fi

# Free the port if it is in use
SERVICE_REGISTRY_PORT=${service_registry_port:-9090}
echo "Checking if port $SERVICE_REGISTRY_PORT is in use..."
PORT_IN_USE=$(lsof -t -i:$SERVICE_REGISTRY_PORT)
if [ -n "$PORT_IN_USE" ]; then
    echo "Port $SERVICE_REGISTRY_PORT is in use by process $PORT_IN_USE. Terminating the process..."
    kill -9 $PORT_IN_USE
    if [ $? -eq 0 ]; then
        echo "Successfully freed port $SERVICE_REGISTRY_PORT."
    else
        echo "Error: Failed to free port $SERVICE_REGISTRY_PORT."
        deactivate
        exit 1
    fi
else
    echo "Port $SERVICE_REGISTRY_PORT is not in use."
fi

# Start the registry.py script
echo "Starting registry.py on port $SERVICE_REGISTRY_PORT..."
nohup python3 "$SERVICE_REGISTRY_SCRIPT" > /exports/applications/service-registry/service-registry.log 2>&1 &

# Get the PID of the last background process
SERVICE_REGISTRY_PID=$!

# Confirm the service registry has started
sleep 5  # Allow some time for the service to start
if ps -p $SERVICE_REGISTRY_PID > /dev/null; then
    echo "Service Registry started successfully with PID $SERVICE_REGISTRY_PID. Logs are being written to /exports/applications/service-registry/service-registry.log."
else
    echo "Error: Failed to start registry.py"
    deactivate
    exit 1
fi

# Deactivate the virtual environment
deactivate