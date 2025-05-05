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
AGENT_SCRIPT="/exports/applications/vm-service/agent.py"
VENV_DIR="/exports/applications/vm-service/venv"
REQUIREMENTS_FILE="/exports/applications/vm-service/requirements.txt"
LOG_DIR="/exports/applications/vm-service/logs"

# Get agent-specific configurations from environment variables
AGENT_PORT=${AGENT_PORT:-8091}  # Default to 8091 if not set
AGENT_HOSTNAME=${AGENT_HOSTNAME:-$(hostname)}  # Use hostname if AGENT_HOSTNAME is not set
LOG_FILE="$LOG_DIR/agent-$AGENT_HOSTNAME.log"

# Ensure the log directory exists and has proper permissions
if [ ! -d "$LOG_DIR" ]; then
    echo "Creating log directory at $LOG_DIR..."
    sudo mkdir -p "$LOG_DIR"
    sudo chmod -R 755 "$LOG_DIR"
    sudo chown -R $USER:$USER "$LOG_DIR"
fi

# Check if the agent.py script exists
if [ ! -f "$AGENT_SCRIPT" ]; then
    echo "Error: agent.py not found at $AGENT_SCRIPT"
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
if [ -f "$VENV_DIR/bin/activate" ]; then
    source "$VENV_DIR/bin/activate"
else
    echo "Error: Virtual environment activation script not found at $VENV_DIR/bin/activate"
    exit 1
fi

Install required libraries
if [ -f "$REQUIREMENTS_FILE" ]; then
    echo "Installing required libraries from $REQUIREMENTS_FILE..."
    pip install --upgrade pip --break-system-packages
    pip install -r "$REQUIREMENTS_FILE" --break-system-packages
    if [ $? -ne 0 ]; then
        echo "Error: Failed to install required libraries."
        deactivate
        exit 1
    fi
else
    echo "Warning: requirements.txt not found at $REQUIREMENTS_FILE. Skipping library installation."
fi

# Validate AGENT_PORT
if [ -z "$AGENT_PORT" ]; then
    echo "Error: AGENT_PORT is not set or empty."
    deactivate
    exit 1
fi

# Free the port if it is in use
echo "Checking if port $AGENT_PORT is in use..."
PORT_IN_USE=$(lsof -t -i:$AGENT_PORT 2>/dev/null)
if [ -n "$PORT_IN_USE" ]; then
    echo "Port $AGENT_PORT is in use by process $PORT_IN_USE. Terminating the process..."
    kill -9 $PORT_IN_USE
    if [ $? -eq 0 ]; then
        echo "Successfully freed port $AGENT_PORT."
    else
        echo "Error: Failed to free port $AGENT_PORT."
        deactivate
        exit 1
    fi
else
    echo "Port $AGENT_PORT is not in use."
fi

# Start the agent.py script
echo "Starting agent.py on port $AGENT_PORT with hostname $AGENT_HOSTNAME..."
nohup python3 "$AGENT_SCRIPT" > "$LOG_FILE" 2>&1 &

# Get the PID of the last background process
AGENT_PID=$!

# Confirm the agent has started
if [ $? -eq 0 ]; then
    echo "Agent started successfully with PID $AGENT_PID on port $AGENT_PORT"
    echo "Logs are being written to $LOG_FILE"
else
    echo "Error: Failed to start agent.py"
    deactivate
    exit 1
fi

# Deactivate the virtual environment
deactivate