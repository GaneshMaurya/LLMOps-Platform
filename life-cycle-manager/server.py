from flask import Flask, request, jsonify
import requests
import threading
import time
import json
import os
import logging
import random

app = Flask(__name__)

CONFIG = {
    "REGISTRY_URL": os.environ.get("REGISTRY_URL", "http://localhost:5100"),
    "REGISTRY_API_KEY": os.environ.get("REGISTRY_API_KEY", "change-me-in-production"),
    "PORT": int(os.environ.get("LIFECYCLE_PORT", 5200)),
    "STATE_FILE": os.environ.get("STATE_FILE", "lifecycle_state.json"),
    "SCALING_CHECK_INTERVAL": int(os.environ.get("SCALING_CHECK_INTERVAL", 30)),
    "HIGH_CPU_THRESHOLD": float(os.environ.get("HIGH_CPU_THRESHOLD", 70.0)),
    "LOW_CPU_THRESHOLD": float(os.environ.get("LOW_CPU_THRESHOLD", 30.0)),
    "MAX_SERVICES_PER_WORKER": int(os.environ.get("MAX_SERVICES_PER_WORKER", 3)),
    "SERVICE_PATH": os.environ.get("SERVICE_PATH", "/mnt/nfs/applications/ocr-service"),
    "SERVICE_NAME": os.environ.get("SERVICE_NAME", "ocr-service")
}

workerNodes = {}
deployedServices = {}
stateLock = threading.Lock()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("lifecycle-manager")

def saveState():
    try:
        state = {
            "deployed_services": deployedServices
        }
        
        with open(CONFIG["STATE_FILE"], 'w') as f:
            json.dump(state, f)
        
        logger.info(f"State saved to {CONFIG['STATE_FILE']}")
    except Exception as e:
        logger.error(f"Error saving state: {str(e)}")

def loadState():
    global deployedServices
    
    try:
        if os.path.exists(CONFIG["STATE_FILE"]):
            with open(CONFIG["STATE_FILE"], 'r') as f:
                state = json.load(f)
                
            with stateLock:
                deployedServices = state.get("deployed_services", {})
            
            logger.info(f"Loaded state with {len(deployedServices)} services")
    except Exception as e:
        logger.error(f"Error loading state: {str(e)}")

def validatePackage(servicePath, serviceName):
    fullPath = os.path.join(servicePath, serviceName)
    
    if not os.path.exists(fullPath):
        return False, "Package directory does not exist"
    
    metaJsonPath = os.path.join(fullPath, "meta.json")
    if not os.path.exists(metaJsonPath):
        return False, "meta.json file is missing"
    
    try:
        with open(metaJsonPath, 'r') as f:
            metaData = json.load(f)
        
        requiredFields = ["setup_commands", "start_command"]
        missingFields = [field for field in requiredFields if field not in metaData]
        
        if missingFields:
            return False, f"meta.json is missing required fields: {', '.join(missingFields)}"
        
        startCommand = metaData.get("start_command", "")
        if not startCommand:
            return False, "start_command cannot be empty"
        
        mainAppFile = startCommand.split()[1] if len(startCommand.split()) > 1 else ""
        mainAppPath = os.path.join(fullPath, mainAppFile)
        
        if mainAppFile and not os.path.exists(mainAppPath):
            return False, f"Main application file '{mainAppFile}' not found"
        
        if not os.path.exists(os.path.join(fullPath, "requirements.txt")):
            logger.warning(f"requirements.txt not found in {fullPath}")
        
        return True, "Package validation successful"
    
    except json.JSONDecodeError:
        return False, "meta.json is not a valid JSON file"
    except Exception as e:
        return False, f"Error validating package: {str(e)}"

def discoverWorkers():
    try:
        workerListStr = os.environ.get("WORKER_NODES", "")
        
        if workerListStr:
            workerList = workerListStr.split(",")
        else:
            workerList = ["localhost:5000"]
        
        for worker in workerList:
            if ":" in worker:
                host, port = worker.split(":")
                workerUrl = f"http://{host}:{port}"
            else:
                workerUrl = f"http://{worker}:5000"
            
            response = requests.get(f"{workerUrl}/system_status", timeout=5)
            
            if response.status_code == 200:
                workerStatus = response.json()
                
                with stateLock:
                    workerNodes[workerUrl] = {
                        "lastCheck": time.time(),
                        "status": workerStatus
                    }
                
                logger.info(f"Discovered worker node: {workerUrl}")
    
    except Exception as e:
        logger.error(f"Error discovering workers: {str(e)}")

def checkWorkerStatus(workerUrl):
    try:
        response = requests.get(f"{workerUrl}/system_status", timeout=5)
        
        if response.status_code == 200:
            workerStatus = response.json()
            
            with stateLock:
                if workerUrl in workerNodes:
                    workerNodes[workerUrl]["lastCheck"] = time.time()
                    workerNodes[workerUrl]["status"] = workerStatus
                else:
                    workerNodes[workerUrl] = {
                        "lastCheck": time.time(),
                        "status": workerStatus
                    }
            
            return True, workerStatus
        else:
            logger.warning(f"Failed to get status from worker {workerUrl}: {response.status_code}")
            return False, None
    
    except Exception as e:
        logger.error(f"Error checking worker {workerUrl}: {str(e)}")
        return False, None

def checkServiceStatus(workerUrl, serviceId, port):
    try:
        response = requests.get(
            f"{workerUrl}/status",
            params={
                "service_name": CONFIG["SERVICE_NAME"],
                "service_path": CONFIG["SERVICE_PATH"],
                "port": port
            },
            timeout=5
        )
        
        if response.status_code == 200:
            serviceStatus = response.json()
            return True, serviceStatus
        else:
            logger.warning(f"Failed to get status for service {serviceId}: {response.status_code}")
            return False, None
    
    except Exception as e:
        logger.error(f"Error checking service {serviceId}: {str(e)}")
        return False, None

def registerServiceWithRegistry(serviceId, host, port):
    try:
        registryUrl = f"{CONFIG['REGISTRY_URL']}/register"
        
        headers = {"X-API-Key": CONFIG["REGISTRY_API_KEY"]}
        
        registrationData = {
            "service_name": CONFIG["SERVICE_NAME"],
            "host": host.split("//")[1].split(":")[0],
            "port": port,
            "metadata": {
                "type": "ocr"
            }
        }
        
        response = requests.post(
            registryUrl,
            headers=headers,
            json=registrationData,
            timeout=5
        )
        
        if response.status_code == 201:
            logger.info(f"Service {serviceId} registered with registry")
            return True
        else:
            logger.error(f"Failed to register service with registry: {response.text}")
            return False
    
    except Exception as e:
        logger.error(f"Error registering with registry: {str(e)}")
        return False

def deregisterServiceFromRegistry(serviceId):
    try:
        registryUrl = f"{CONFIG['REGISTRY_URL']}/services/{serviceId}"
        
        headers = {"X-API-Key": CONFIG["REGISTRY_API_KEY"]}
        
        response = requests.delete(
            registryUrl,
            headers=headers,
            timeout=5
        )
        
        if response.status_code == 200:
            logger.info(f"Service {serviceId} deregistered from registry")
            return True
        else:
            logger.error(f"Failed to deregister service from registry: {response.text}")
            return False
    
    except Exception as e:
        logger.error(f"Error deregistering from registry: {str(e)}")
        return False

def selectBestWorker():
    if not workerNodes:
        return None
    
    candidates = []
    
    with stateLock:
        for workerUrl, workerInfo in workerNodes.items():
            if time.time() - workerInfo["lastCheck"] > 60:
                continue
            
            serviceCount = sum(1 for s in deployedServices.values() if s["worker"] == workerUrl)
            
            if serviceCount >= CONFIG["MAX_SERVICES_PER_WORKER"]:
                continue
            
            cpuPercent = workerInfo["status"].get("cpu_percent", 100)
            
            if cpuPercent > 90:
                continue
            
            score = cpuPercent + (serviceCount * 10)
            candidates.append((workerUrl, score))
    
    if not candidates:
        return None
    
    candidates.sort(key=lambda x: x[1])
    return candidates[0][0]

def provisionService():
    isValid, validationMsg = validatePackage(CONFIG["SERVICE_PATH"], CONFIG["SERVICE_NAME"])
    
    if not isValid:
        logger.error(f"Package validation failed: {validationMsg}")
        return None
    
    workerUrl = selectBestWorker()
    
    if not workerUrl:
        logger.warning("No suitable worker found for provisioning")
        return None
    
    try:
        response = requests.post(
            f"{workerUrl}/provision",
            json={
                "service_name": CONFIG["SERVICE_NAME"],
                "service_path": CONFIG["SERVICE_PATH"]
            },
            timeout=10
        )
        
        if response.status_code == 200:
            provisionData = response.json()
            serviceId = provisionData["service_id"]
            port = provisionData["port"]
            
            registerServiceWithRegistry(serviceId, workerUrl, port)
            
            with stateLock:
                deployedServices[serviceId] = {
                    "worker": workerUrl,
                    "port": port,
                    "status": "running",
                    "provisionTime": time.time()
                }
            
            saveState()
            
            logger.info(f"Provisioned new service {serviceId} on {workerUrl}")
            return serviceId
        else:
            logger.error(f"Failed to provision service: {response.text}")
            return None
    
    except Exception as e:
        logger.error(f"Error provisioning service: {str(e)}")
        return None

def releaseService(serviceId):
    if serviceId not in deployedServices:
        logger.warning(f"Cannot release unknown service: {serviceId}")
        return False
    
    serviceInfo = deployedServices[serviceId]
    workerUrl = serviceInfo["worker"]
    port = serviceInfo["port"]
    
    try:
        deregisterServiceFromRegistry(serviceId)
        
        response = requests.post(
            f"{workerUrl}/release",
            json={
                "service_name": CONFIG["SERVICE_NAME"],
                "service_path": CONFIG["SERVICE_PATH"],
                "port": port
            },
            timeout=5
        )
        
        with stateLock:
            if serviceId in deployedServices:
                del deployedServices[serviceId]
        
        saveState()
        
        if response.status_code == 200:
            logger.info(f"Released service {serviceId}")
            return True
        else:
            logger.warning(f"Received error when releasing service {serviceId}: {response.text}")
            return False
    
    except Exception as e:
        logger.error(f"Error releasing service {serviceId}: {str(e)}")
        
        with stateLock:
            if serviceId in deployedServices:
                del deployedServices[serviceId]
        
        saveState()
        
        return False

def monitorServicesThread():
    while True:
        try:
            serviceIds = list(deployedServices.keys())
            
            for serviceId in serviceIds:
                try:
                    with stateLock:
                        if serviceId not in deployedServices:
                            continue
                        
                        serviceInfo = deployedServices[serviceId]
                    
                    workerUrl = serviceInfo["worker"]
                    port = serviceInfo["port"]
                    
                    success, status = checkServiceStatus(workerUrl, serviceId, port)
                    
                    if not success:
                        logger.warning(f"Service {serviceId} appears to be unhealthy")
                        
                        releaseService(serviceId)
                        provisionService()
                
                except Exception as e:
                    logger.error(f"Error monitoring service {serviceId}: {str(e)}")
            
            workerUrls = list(workerNodes.keys())
            
            for workerUrl in workerUrls:
                try:
                    checkWorkerStatus(workerUrl)
                except Exception as e:
                    logger.error(f"Error checking worker {workerUrl}: {str(e)}")
        
        except Exception as e:
            logger.error(f"Error in monitoring thread: {str(e)}")
        
        time.sleep(30)

def scalingDecisionThread():
    while True:
        try:
            time.sleep(CONFIG["SCALING_CHECK_INTERVAL"])
            
            totalCpu = 0
            workerCount = 0
            serviceCount = 0
            
            with stateLock:
                serviceCount = len(deployedServices)
                
                for workerUrl, workerInfo in workerNodes.items():
                    if time.time() - workerInfo["lastCheck"] > 60:
                        continue
                    
                    totalCpu += workerInfo["status"].get("cpu_percent", 0)
                    workerCount += 1
            
            if workerCount == 0:
                logger.warning("No active workers found, cannot make scaling decisions")
                continue
            
            avgCpu = totalCpu / workerCount
            
            if avgCpu > CONFIG["HIGH_CPU_THRESHOLD"] and serviceCount < workerCount * CONFIG["MAX_SERVICES_PER_WORKER"]:
                logger.info(f"Scaling up due to high CPU usage ({avgCpu:.1f}%)")
                provisionService()
            elif avgCpu < CONFIG["LOW_CPU_THRESHOLD"] and serviceCount > 1:
                logger.info(f"Scaling down due to low CPU usage ({avgCpu:.1f}%)")
                
                newestService = None
                newestTime = 0
                
                with stateLock:
                    for serviceId, serviceInfo in deployedServices.items():
                        provisionTime = serviceInfo.get("provisionTime", 0)
                        if provisionTime > newestTime:
                            newestTime = provisionTime
                            newestService = serviceId
                
                if newestService:
                    releaseService(newestService)
        
        except Exception as e:
            logger.error(f"Error in scaling thread: {str(e)}")

@app.route('/workers', methods=['GET'])
def getWorkers():
    with stateLock:
        return jsonify({
            "workers": workerNodes,
            "count": len(workerNodes)
        })

@app.route('/services', methods=['GET'])
def getServices():
    with stateLock:
        return jsonify({
            "services": deployedServices,
            "count": len(deployedServices)
        })

@app.route('/provision', methods=['POST'])
def manualProvision():
    serviceId = provisionService()
    
    if serviceId:
        return jsonify({
            "status": "success",
            "service_id": serviceId
        })
    else:
        return jsonify({
            "status": "error",
            "message": "Failed to provision service"
        }), 500

@app.route('/release', methods=['POST'])
def manualRelease():
    data = request.json
    serviceId = data.get("service_id")
    
    if not serviceId:
        return jsonify({"error": "Missing service_id parameter"}), 400
    
    success = releaseService(serviceId)
    
    if success:
        return jsonify({
            "status": "success",
            "service_id": serviceId
        })
    else:
        return jsonify({
            "status": "error",
            "message": f"Failed to release service {serviceId}"
        }), 500

@app.route('/service-failure', methods=['POST'])
def handleServiceFailure():
    data = request.json
    serviceId = data.get("service_id")
    
    if not serviceId:
        return jsonify({"error": "Missing service_id parameter"}), 400
    
    with stateLock:
        if serviceId not in deployedServices:
            return jsonify({"error": "Unknown service_id"}), 404
        
        serviceInfo = deployedServices[serviceId]
    
    workerUrl = serviceInfo["worker"]
    port = serviceInfo["port"]
    
    success, status = checkServiceStatus(workerUrl, serviceId, port)
    
    if success:
        return jsonify({
            "status": "service_recovered",
            "service_id": serviceId
        })
    else:
        logger.info(f"Confirmed service failure for {serviceId}, redeploying")
        releaseService(serviceId)
        newServiceId = provisionService()
        
        return jsonify({
            "status": "service_redeployed",
            "old_service_id": serviceId,
            "new_service_id": newServiceId
        })

@app.route('/validate-package', methods=['POST'])
def validatePackageEndpoint():
    data = request.json
    servicePath = data.get("service_path", CONFIG["SERVICE_PATH"])
    serviceName = data.get("service_name", CONFIG["SERVICE_NAME"])
    
    isValid, message = validatePackage(servicePath, serviceName)
    
    return jsonify({
        "valid": isValid,
        "message": message,
        "service_path": servicePath,
        "service_name": serviceName
    })

@app.route('/health', methods=['GET'])
def healthCheck():
    return jsonify({
        "status": "healthy",
        "workers": len(workerNodes),
        "services": len(deployedServices)
    })

def initSystem():
    loadState()
    discoverWorkers()
    
    if not deployedServices:
        logger.info("No existing services found, provisioning initial service")
        provisionService()

if __name__ == '__main__':
    initSystem()
    
    monitoringThread = threading.Thread(target=monitorServicesThread, daemon=True)
    monitoringThread.start()
    
    scalingThread = threading.Thread(target=scalingDecisionThread, daemon=True)
    scalingThread.start()
    
    app.run(host='0.0.0.0', port=CONFIG["PORT"])
