#!/bin/bash

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
MODEL_REGISTRY_SCRIPT="/exports/applications/model-registry/model-registry.py"
VENV_DIR="/exports/applications/model-registry/venv"
REQUIREMENTS_FILE="/exports/applications/model-registry/requirements.txt"

# Check if the model-registry.py script exists
if [ ! -f "$MODEL_REGISTRY_SCRIPT" ]; then
    echo "Error: model-registry.py not found at $MODEL_REGISTRY_SCRIPT"
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
echo "Checking if port $model_registry_port is in use..."
PORT_IN_USE=$(lsof -t -i:$model_registry_port)
if [ -n "$PORT_IN_USE" ]; then
    echo "Port $model_registry_port is in use by process $PORT_IN_USE. Terminating the process..."
    kill -9 $PORT_IN_USE
    if [ $? -eq 0 ]; then
        echo "Successfully freed port $model_registry_port."
    else
        echo "Error: Failed to free port $model_registry_port."
        deactivate
        exit 1
    fi
else
    echo "Port $model_registry_port is not in use."
fi

# Start the model-registry.py script
echo "Starting model-registry.py on port $model_registry_port..."
nohup python3 "$MODEL_REGISTRY_SCRIPT" > /exports/applications/model-registry/model-registry.log 2>&1 &

# Get the PID of the last background process
MODEL_REGISTRY_PID=$!

# Confirm the model registry has started
sleep 5  # Allow some time for the service to start
if ps -p $MODEL_REGISTRY_PID > /dev/null; then
    echo "Model Registry started successfully with PID $MODEL_REGISTRY_PID. Logs are being written to /exports/applications/model-registry/model-registry.log."
else
    echo "Error: Failed to start model-registry.py"
    deactivate
    exit 1
fi

# Deactivate the virtual environment
deactivate