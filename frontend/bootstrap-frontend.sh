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
APP_DIR="/exports/applications/frontend"
APP_SCRIPT="$APP_DIR/app.py"
VENV_DIR="$APP_DIR/venv"
REQUIREMENTS_FILE="$APP_DIR/requirements.txt"
LOG_DIR="$APP_DIR"

# Get frontend-specific configurations
FLASK_PORT=${FLASK_PORT:-5000}  # Default to 5000 if not set
LOG_FILE="$LOG_DIR/frontend.log"

# Set directory permissions
echo "Setting directory permissions..."
chmod -R 777 "$APP_DIR"
echo "Directory permissions set for $APP_DIR"

# Create requirements.txt if it doesn't exist
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "Creating requirements.txt..."
    cat > "$REQUIREMENTS_FILE" << EOF
flask==3.0.0
werkzeug==3.0.1
flask-wtf==1.2.1
requests==2.31.0
python-dotenv==1.0.0
SQLAlchemy==2.0.25
Jinja2==3.1.2
itsdangerous==2.1.2
click==8.1.7
markupsafe==2.1.3
urllib3==2.1.0
certifi==2023.11.17
charset-normalizer==3.3.2
idna==3.6
blinker==1.7.0
EOF
    chmod 777 "$REQUIREMENTS_FILE"
fi

# Check if app.py exists
if [ ! -f "$APP_SCRIPT" ]; then
    echo "Error: app.py not found at $APP_SCRIPT"
    exit 1
fi

# Create and activate virtual environment
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment at $VENV_DIR..."
    python3 -m venv "$VENV_DIR"
    if [ $? -ne 0 ]; then
        echo "Error: Failed to create virtual environment."
        exit 1
    fi
    # Set permissions for venv directory
    chmod -R 777 "$VENV_DIR"
    echo "Virtual environment permissions set"
fi

# Activate virtual environment
source "$VENV_DIR/bin/activate"
if [ $? -ne 0 ]; then
    echo "Error: Failed to activate virtual environment."
    exit 1
fi

# Install required packages
echo "Installing required packages..."
pip install --upgrade pip
pip install -r "$REQUIREMENTS_FILE"
if [ $? -ne 0 ]; then
    echo "Error: Failed to install required packages."
    deactivate
    exit 1
fi

# Free the port if in use
echo "Checking if port $FLASK_PORT is in use..."
PORT_IN_USE=$(lsof -t -i:$FLASK_PORT 2>/dev/null)
if [ -n "$PORT_IN_USE" ]; then
    echo "Port $FLASK_PORT is in use. Terminating process..."
    kill -9 $PORT_IN_USE
    sleep 2
fi

# Set Flask environment variables
export FLASK_ENV=development
export FLASK_APP="$APP_SCRIPT"
export FLASK_PORT="$FLASK_PORT"

# Create and set permissions for log file
echo "Setting up log file..."
touch "$LOG_FILE"
chmod 777 "$LOG_FILE"
echo "Log file permissions set"

# Start the Flask application
echo "Starting Flask application on port $FLASK_PORT..."
nohup python3 "$APP_SCRIPT" > "$LOG_FILE" 2>&1 &

# Get the PID of the Flask process
FLASK_PID=$!

# Check if application started successfully
sleep 2
if ps -p $FLASK_PID > /dev/null; then
    echo "Flask application started successfully with PID $FLASK_PID"
    echo "Logs are being written to $LOG_FILE"
    # Store PID for future reference
    echo $FLASK_PID > "$APP_DIR/.pid"
    chmod 777 "$APP_DIR/.pid"
else
    echo "Error: Failed to start Flask application"
    deactivate
    exit 1
fi

# Deactivate virtual environment
deactivate

echo "Bootstrap completed successfully!"