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
CONTROLLER_SCRIPT="/exports/applications/controller-Service/controller.py"
VENV_DIR="/exports/applications/controller-Service/venv"
REQUIREMENTS_FILE="/exports/applications/controller-Service/requirements.txt"

# Check if the controller.py script exists
if [ ! -f "$CONTROLLER_SCRIPT" ]; then
    echo "Error: controller.py not found at $CONTROLLER_SCRIPT"
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
echo "Checking if port $life_cycle_manager_port is in use..."
PORT_IN_USE=$(lsof -t -i:$life_cycle_manager_port)
if [ -n "$PORT_IN_USE" ]; then
    echo "Port $life_cycle_manager_port is in use by process $PORT_IN_USE. Terminating the process..."
    kill -9 $PORT_IN_USE
    if [ $? -eq 0 ]; then
        echo "Successfully freed port $life_cycle_manager_port."
    else
        echo "Error: Failed to free port $life_cycle_manager_port."
        deactivate
        exit 1
    fi
else
    echo "Port $life_cycle_manager_port is not in use."
fi

# Start the controller.py script
echo "Starting controller.py on port $life_cycle_manager_port..."
nohup python3 "$CONTROLLER_SCRIPT" > /exports/applications/controller-Service/controller.log 2>&1 &

# Get the PID of the last background process
CONTROLLER_PID=$!

# Confirm the controller has started
sleep 5  # Allow some time for the service to start
if ps -p $CONTROLLER_PID > /dev/null; then
    echo "Controller started successfully with PID $CONTROLLER_PID. Logs are being written to /exports/applications/controller-Service/controller.log."
else
    echo "Error: Failed to start controller.py"
    deactivate
    exit 1
fi

# Deactivate the virtual environment
deactivate