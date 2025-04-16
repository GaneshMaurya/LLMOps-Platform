#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# Upgrade pip and install dependencies
pip install --upgrade pip
pip install -r /exports/applications/deployer-service/requirements.txt

# Run the python server
python3 /exports/applications/deployer-service/server.py