from flask import Flask, request, Response, jsonify
import requests
import random
import time
import threading
import logging
import os
import json
from functools import wraps
from urllib.parse import urljoin

app = Flask(__name__)

CONFIG = {
    "REGISTRY_URL": os.environ.get("REGISTRY_URL", "http://localhost:5100"),
    "REGISTRY_API_KEY": os.environ.get("REGISTRY_API_KEY", "change-me-in-production"),
    "PORT": int(os.environ.get("GATEWAY_PORT", 5000)),
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("api-gateway")

serviceCache = {}
cacheLock = threading.Lock()

def getServices(serviceName=None, forceRefresh=False):
    currentTime = time.time()
    
    with cacheLock:
        if not forceRefresh and serviceName in serviceCache:
            cacheEntry = serviceCache[serviceName]
            if currentTime - cacheEntry["lastUpdate"] < 30:
                return cacheEntry["services"]
    
    try:
        url = f"{CONFIG['REGISTRY_URL']}/services"
        if serviceName:
            url += f"?service_name={serviceName}"
        
        headers = {"X-API-Key": CONFIG["REGISTRY_API_KEY"]}
        response = requests.get(url, headers=headers, timeout=5)
        
        if response.status_code == 200:
            servicesData = response.json()["services"]
            
            with cacheLock:
                serviceCache[serviceName] = {
                    "services": servicesData,
                    "lastUpdate": currentTime
                }
            
            return servicesData
        else:
            logger.error(f"Failed to fetch services: {response.status_code} - {response.text}")
            
            with cacheLock:
                if serviceName in serviceCache:
                    return serviceCache[serviceName]["services"]
            
            return []
    
    except Exception as e:
        logger.error(f"Error fetching services: {str(e)}")
        
        with cacheLock:
            if serviceName in serviceCache:
                return serviceCache[serviceName]["services"]
        
        return []

def selectService(services):
    if not services:
        return None
    
    return random.choice(services)

@app.route('/ocr/<path:subpath>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def handleOcrRequest(subpath):
    clientIp = request.remote_addr
    
    ocrServices = getServices("ocr-service")
    
    if not ocrServices:
        return jsonify({"error": "No OCR services available"}), 503
    
    service = selectService(ocrServices)
    
    if not service:
        return jsonify({"error": "All OCR services are currently unavailable"}), 503
    
    targetUrl = f"http://{service['host']}:{service['port']}/{subpath}"
    
    try:
        method = request.method
        headers = {key: value for key, value in request.headers if key != 'Host'}
        data = request.get_data()
        params = request.args
        
        headers['X-Forwarded-For'] = clientIp
        headers['X-Original-URI'] = request.url
        
        response = requests.request(
            method=method,
            url=targetUrl,
            headers=headers,
            params=params,
            data=data,
            timeout=30,
            stream=True
        )
        
        gatewayResponse = Response(
            response=response.iter_content(chunk_size=1024),
            status=response.status_code
        )
        
        for name, value in response.headers.items():
            if name.lower() not in ('transfer-encoding', 'content-encoding', 'content-length'):
                gatewayResponse.headers[name] = value
        
        logger.info(f"Request to {targetUrl} completed with status {response.status_code}")
        
        return gatewayResponse
    
    except Exception as e:
        logger.error(f"Error forwarding request to {targetUrl}: {str(e)}")
        
        threading.Thread(target=getServices, args=("ocr-service", True)).start()
        
        return jsonify({
            "error": "Error communicating with OCR service",
            "details": str(e)
        }), 502

@app.route('/health', methods=['GET'])
def healthCheck():
    try:
        registryResponse = requests.get(
            f"{CONFIG['REGISTRY_URL']}/health",
            headers={"X-API-Key": CONFIG["REGISTRY_API_KEY"]},
            timeout=5
        )
        registryStatus = "healthy" if registryResponse.status_code == 200 else "unhealthy"
    except Exception:
        registryStatus = "unreachable"
    
    return jsonify({
        "status": "healthy",
        "registryStatus": registryStatus,
        "servicesCached": list(serviceCache.keys())
    })

def cacheRefreshThread():
    while True:
        try:
            with cacheLock:
                cachedServices = list(serviceCache.keys())
            
            for serviceName in cachedServices:
                getServices(serviceName, forceRefresh=True)
                time.sleep(1)
        
        except Exception as e:
            logger.error(f"Error in cache refresh thread: {str(e)}")
        
        time.sleep(15)

if __name__ == '__main__':
    refreshThread = threading.Thread(target=cacheRefreshThread, daemon=True)
    refreshThread.start()
    
    app.run(host='0.0.0.0', port=CONFIG["PORT"])