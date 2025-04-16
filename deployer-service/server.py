from flask import Flask, request, jsonify
import subprocess
import os
import signal
import socket
import json
import psutil
import requests
import zipfile
import shutil
import threading
import time
import requests
from flask_cors import CORS
from dotenv import load_dotenv

app = Flask(__name__)
CORS(app)

ENV_FILE_PATH = "/exports/applications/.env"
load_dotenv(ENV_FILE_PATH)

# Configuration
MODEL_REGISTRY_IP = os.environ.get("model_registry_ip")
MODEL_REGISTRY_PORT = os.environ.get("model_registry_port")
FILE_HANDLER_URL = f"http://{MODEL_REGISTRY_IP}:{MODEL_REGISTRY_PORT}" 

# Store active servers { (ip, port): process }
active_servers = {}

def get_available_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]

def fetch_model(model_id, user_name, version=None):
    """
    Fetch model from file-handler service and unzip it
    
    Returns:
        dict: Model information including the unzipped directory path
    """
    try:
        if version:
            # Fetch specific version
            response = requests.get(
                f"{FILE_HANDLER_URL}/fetch-model/{model_id}/{version}?user_name={user_name}"
            )
        else:
            # Fetch latest version
            response = requests.get(
                f"{FILE_HANDLER_URL}/fetch-model/{model_id}?user_name={user_name}"
            )
        if response.status_code == 200:
            model_info = response.json()
            nfs_path = model_info.get('nfs_path')
            print(model_info)
            if nfs_path and os.path.exists(nfs_path):
                # Create a temporary extraction directory
                temp_extract_dir = os.path.join(os.path.dirname(nfs_path), "temp_extract")
                
                # Remove directory if it already exists
                if os.path.exists(temp_extract_dir):
                    shutil.rmtree(temp_extract_dir)
                
                # Create the directory
                os.makedirs(temp_extract_dir, exist_ok=True)
                
                # Unzip the model file
                with zipfile.ZipFile(nfs_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_extract_dir)
                
                # Determine the final destination directory
                final_dir = os.path.join(os.path.dirname(nfs_path),model_id)
                
                # Remove the final directory if it already exists
                if os.path.exists(final_dir):
                    shutil.rmtree(final_dir)
                
                # Rename the extracted folder to model_id
                shutil.move(temp_extract_dir+"/", final_dir)
                
                # Update the model info with the renamed directory path
                model_info['extracted_path'] = final_dir
                return model_info
            else:
                print(f"NFS path not found or invalid: {nfs_path}")
                return None
        else:
            print(f"Error response from file handler: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Error fetching model: {e}")
        return None
    except zipfile.BadZipFile as e:
        print(f"Error extracting zip file: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def start_web_app(service_name, service_path, port):
    web_app_path = os.path.join(service_path, service_name)
    # print(web_app_path)
    if not os.path.exists(web_app_path):
        raise FileNotFoundError("Web app folder not found")
    
    meta_file = os.path.join(web_app_path, "meta.json")
    if not os.path.exists(meta_file):
        raise FileNotFoundError("meta.json file not found")
    
    with open(meta_file, "r") as f:
        meta_data = json.load(f)
    
    setup_commands = meta_data.get("setup_commands", [])
    start_command = meta_data.get("start_command")
    
    # print(web_app_path)

    os.chdir(web_app_path)
    subprocess.run("python3 -m venv venv", shell=True)
    subprocess.run("source venv/bin/activate", shell=True)
    
    for cmd in setup_commands:
        subprocess.run(cmd, shell=True)
    
    if start_command:
        env = os.environ.copy()
        env["PORT"] = str(port)
        process = subprocess.Popen(start_command, env=env, shell=True)
        return process
    else:
        raise ValueError("Start command not specified in meta.json")

@app.route('/provision', methods=['POST'])
def provision():
    data = request.json
    service_name = data.get('service_name')
    model_id = data.get('model_id')  # Add model_id parameter
    user_name = data.get('user_name')  # Add user_name parameter
    model_version = data.get('model_version')  # Optional - specific version

    if not service_name or not model_id or not user_name:
        return jsonify({'error': 'Missing required parameters'}), 400
    
    # Fetch model from file-handler service and unzip it
    model_info = fetch_model(model_id, user_name, model_version)
    
    if not model_info:
        return jsonify({'error': f'Model {model_id} not found or inaccessible'}), 404
    
    # Get the extracted path from the model information
    extracted_path = model_info.get('extracted_path')
    if not extracted_path:
        return jsonify({'error': 'Failed to extract model files'}), 500
    
    # Use the extracted path for the service
    port = get_available_port()
    
    try:
        # service_path = setup_nfs(service_name, service_path)
        # extracted_path = extracted_path + "Web_app"
        process = start_web_app(service_name, extracted_path, str(port))
        service_id = extracted_path + "@" + service_name
        active_servers[(service_id, port)] = process
        
        return jsonify({
            'message': 'Server provisioned', 
            'service_id': service_id, 
            'port': port,
            'model_info': {
                'model_id': model_id,
                'version': model_info.get('version'),
                'nfs_path': model_info.get('nfs_path'),
                'extracted_path': extracted_path
            }
        })
    except Exception as e:
        return jsonify({'error': f'Failed to provision server: {str(e)}'}), 500

@app.route('/release', methods=['POST'])
def release():
    data = request.json
    service_name = data.get('service_name')
    service_path = data.get('service_path')
    service_id = service_path + "@" + service_name
    port = data.get('port')
    
    if (service_id, port) not in active_servers:
        return jsonify({'error': 'Server not found'}), 404
    
    process = active_servers.pop((service_id, port))
    os.kill(process.pid, signal.SIGTERM)
    return jsonify({'message': 'Server stopped', 'service_id': service_id, 'port': port})


@app.route('/status', methods=['GET'])
def status():
    service_name = request.args.get('service_name')
    service_path = request.args.get('service_path')
    port = request.args.get('port', type=int)

    if not service_name or not service_path or port is None:
        return jsonify({'error': 'Missing required parameters'}), 400

    service_id = service_path + "@" + service_name
    process_key = (service_id, port)

    if process_key not in active_servers:
        return jsonify({'error': 'Server not found'}), 404

    process = active_servers[process_key]
    ps_process = psutil.Process(process.pid)

    resource_usage = {
        'cpu_percent': ps_process.cpu_percent(interval=1),  # CPU usage percentage
        'memory_info': ps_process.memory_info()._asdict(),  # Memory details
        'status': ps_process.status(),  # Process status
        'pid': process.pid
    }

    return jsonify({'service_id': service_id, 'port': port, 'resource_usage': resource_usage})


@app.route('/system_status', methods=['GET'])
def system_status():
    system_info = {
        "cpu_percent": psutil.cpu_percent(interval=1),  # Total CPU usage percentage
        "memory": psutil.virtual_memory()._asdict(),  # Total memory stats
        "disk": psutil.disk_usage('/')._asdict(),  # Disk usage for root partition
        "available_cpu_cores": psutil.cpu_count(logical=False),  # Available physical cores
        "total_cpu_cores": psutil.cpu_count(logical=True)  # Total logical cores
    }

    return jsonify(system_info)

# Service registry details
REGISTRY_IP = os.environ.get("service_registry_ip")
REGISTRY_PORT = os.environ.get("service_registry_port")
REGISTRY_URL = f"http://{REGISTRY_IP}:{REGISTRY_PORT}"
SERVICE_NAME = "deployer_service"
SERVICE_HOST = os.environ.get("deployer_registry_ip")
SERVICE_PORT = os.environ.get("deployer_registry_port")
SERVICE_ID = f"{SERVICE_NAME}-{SERVICE_HOST}-{SERVICE_PORT}"

def send_heartbeat():
    """
    Periodically send a heartbeat to the service registry.
    """
    heartbeat_url = f"{REGISTRY_URL}/heartbeat/{SERVICE_ID}"
    while True:
        try:
            response = requests.post(heartbeat_url)
            if response.ok:
                print("Heartbeat sent successfully.")
            else:
                print("Heartbeat failed:", response.status_code, response.text)
        except Exception as e:
            print("Error sending heartbeat:", e)
        time.sleep(30)  # Send heartbeat every 30 seconds

def start_heartbeat():
    """
    Start heartbeat in a separate thread.
    """
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()

@app.route("/health", methods=["GET"])
def health_check():
    """
    Health check endpoint.
    """
    return jsonify({"status": "ok", "message": "Service is running."})

def register_service():
    register_url = f"{REGISTRY_URL}/register"
    payload = {
        "service_name": SERVICE_NAME,
        "host": SERVICE_HOST,
        "port": SERVICE_PORT,
        "metadata": {"description": "Deployer service"}
    }   
    try:
        response = requests.post(register_url, json=payload)
        if response.ok:
            print("Service registered successfully:", response.json())
        else:
            print("Failed to register service:", response.status_code, response.text)
    except Exception as e:
        print("Error registering service:", e)

if __name__ == '__main__':

    # Register service on startup
    register_service()
    start_heartbeat()
    app.run(host='0.0.0.0', port=SERVICE_PORT)
