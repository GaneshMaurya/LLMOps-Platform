from flask import Flask, request, jsonify
import threading
import json
import time
import os
from datetime import datetime, timezone
from flask_cors import CORS
from dotenv import load_dotenv

app = Flask(__name__)
CORS(app)

ENV_FILE_PATH = "/exports/applications/.env"
load_dotenv(ENV_FILE_PATH)

config = {
    # "apiKey": os.environ.get("REGISTRY_API_KEY", "change-me-in-production"),
    # "storageFile": os.environ.get("REGISTRY_STORAGE", "registry_data.json"),
    # "saveInterval": int(os.environ.get("REGISTRY_SAVE_INTERVAL", 60)),
    # "unhealthyThreshold": int(os.environ.get("REGISTRY_UNHEALTHY_THRESHOLD", 60)),
    # "port": int(os.environ.get("REGISTRY_PORT", 5100))
    "apiKey": "change-me-in-production",
    "storageFile": "registry_data.json",
    "saveInterval": 60,
    "unhealthyThreshold": 60,
    "port": os.getenv("service_registry_port")
}

services = {}
servicesLock = threading.Lock()

def getCurrentTime():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def verifyApiKey():
    apiKey = request.headers.get("X-API-Key")
    return apiKey == config["apiKey"]

def generateServiceId(serviceName, host, port):
    return f"{serviceName}-{host}-{port}"

def saveRegistryData():
    while True:
        time.sleep(config["saveInterval"])
        with servicesLock:
            dataToSave = {k: v for k, v in services.items()}
        try:
            with open(config["storageFile"], 'w') as f:
                json.dump(dataToSave, f)
            print(f"Registry data saved to {config['storageFile']}")
        except Exception as e:
            print(f"Error saving registry data: {str(e)}")

def loadRegistryData():
    try:
        if os.path.exists(config["storageFile"]):
            with open(config["storageFile"], 'r') as f:
                data = json.load(f)
                with servicesLock:
                    services.update(data)
            print(f"Loaded {len(data)} services from {config['storageFile']}")
    except Exception as e:
        print(f"Error loading registry data: {str(e)}")

@app.route('/register', methods=['POST'])
def registerService():
    # if not verifyApiKey():
    #     return jsonify({"error": "Unauthorized"}), 401
    data = request.json
    requiredFields = ['service_name', 'host', 'port']
    for field in requiredFields:
        if field not in data:
            return jsonify({"error": f"Missing required field: {field}"}), 400
    serviceId = generateServiceId(data['service_name'], data['host'], data['port'])
    serviceRecord = {
        "serviceId": serviceId,
        "serviceName": data['service_name'],
        "host": data['host'],
        "port": data['port'],
        "status": "healthy",
        "lastUpdate": getCurrentTime(),
        "registrationTime": getCurrentTime(),
        "metadata": data.get('metadata', {})
    }
    with servicesLock:
        services[serviceId] = serviceRecord
    print(f"Service registered: {serviceId} at {data['host']}:{data['port']}")
    return jsonify({"serviceId": serviceId, "status": "registered"}), 201

@app.route('/services', methods=['GET'])
def getServices():
    serviceName = request.args.get('service_name')
    status = request.args.get('status', 'healthy')
    filteredServices = []
    currentTime = datetime.now(timezone.utc)
    with servicesLock:
        for serviceId, service in services.items():
            if serviceName and service['serviceName'] != serviceName:
                continue
            if status == 'healthy' and service['status'] != 'healthy':
                continue
            filteredServices.append(service)
    return jsonify({"services": filteredServices, "count": len(filteredServices), "timestamp": getCurrentTime()})

@app.route('/services/<serviceId>/status', methods=['PUT'])
def updateServiceStatus():
    # if not verifyApiKey():
    #     return jsonify({"error": "Unauthorized"}), 401
    serviceId = request.view_args['serviceId']
    data = request.json
    if 'status' not in data:
        return jsonify({"error": "Missing status field"}), 400
    with servicesLock:
        if serviceId not in services:
            return jsonify({"error": "Service not found"}), 404
        services[serviceId]['status'] = data['status']
        services[serviceId]['lastUpdate'] = getCurrentTime()
        if 'metrics' in data:
            if 'metadata' not in services[serviceId]:
                services[serviceId]['metadata'] = {}
            services[serviceId]['metadata']['metrics'] = data['metrics']
    return jsonify({"serviceId": serviceId, "status": data['status']})

@app.route('/services/<serviceId>', methods=['DELETE'])
def deregisterService():
    if not verifyApiKey():
        return jsonify({"error": "Unauthorized"}), 401
    serviceId = request.view_args['serviceId']
    with servicesLock:
        if serviceId not in services:
            return jsonify({"error": "Service not found"}), 404
        serviceInfo = services[serviceId]
        del services[serviceId]
    print(f"Service deregistered: {serviceId} at {serviceInfo['host']}:{serviceInfo['port']}")
    return jsonify({"serviceId": serviceId, "status": "deregistered"})

@app.route('/health', methods=['GET'])
def healthCheck():
    return jsonify({"status": "healthy", "serviceCount": len(services), "timestamp": getCurrentTime()})

def checkServiceHealth():
    while True:
        time.sleep(10)
        currentTime = datetime.now(timezone.utc)
        with servicesLock:
            for serviceId, service in list(services.items()):
                lastUpdate = datetime.fromisoformat(service['lastUpdate'].replace('Z', '+00:00'))
                secondsSinceUpdate = (currentTime - lastUpdate).total_seconds()
                if secondsSinceUpdate > config["unhealthyThreshold"] and service['status'] == 'healthy':
                    service['status'] = 'unhealthy'
                    print(f"Service marked unhealthy due to inactivity: {serviceId}")

@app.route('/heartbeat/<serviceId>', methods=['POST'])
def heartbeat(serviceId):
    """
    Heartbeat endpoint to update the lastUpdate timestamp for the service.
    This keeps the service marked as healthy.
    """
    with servicesLock:
        if serviceId not in services:
            return jsonify({"error": "Service not found"}), 404
        services[serviceId]['lastUpdate'] = getCurrentTime()
    return jsonify({"serviceId": serviceId, "status": "heartbeat updated", "timestamp": getCurrentTime()})

if __name__ == '__main__':
    loadRegistryData()
    saveThread = threading.Thread(target=saveRegistryData, daemon=True)
    saveThread.start()
    healthThread = threading.Thread(target=checkServiceHealth, daemon=True)
    healthThread.start()
    app.run(host='0.0.0.0', port=config["port"])
