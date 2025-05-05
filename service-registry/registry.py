from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import logging
import time
import os

log_file_path = "service-registry.log"
time.sleep(1)
if os.path.exists(log_file_path):
    os.chmod(log_file_path, 0o777)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s - [%(filename)s:%(lineno)d]',
    handlers=[
        logging.FileHandler("service-registry.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("service-registry")

app = Flask(__name__)
CORS(app) 

service_info=dict()

@app.route('/service-registry/register', methods=['POST'])
def registerService():
    logger.info(f"Received registration request: {request.json}")
    body=request.json
    if not body:
        logger.warning(f"Got request to register but no body present")
        return jsonify({'success':False,'error':'No body received'}),400
    service_name=body['name']
    service_ip=body['ip']
    service_port=body['port']
    last_updated=time.time()
    service_info[service_name]={"ip":service_ip,"port":service_port}
    logger.info(f"Added new service {service_name} to the registry")
    return jsonify({'success':True,'result':'info added to service-registry'}),200

@app.route('/service-registry/getServiceInfo', methods=['GET'])
def getServiceInfo():
    service_name = request.args.get('name')
    logger.info(f"Received Service Info Request for: {service_name}")
    if not service_name:
        logger.warning(f"Got request for service info but no service name provided")
        return jsonify({'success': False, 'error': 'No service name received'}), 400
    returnData = service_info.get(service_name, None)
    if not returnData:
        logger.info(f"No service with name {service_name} exists")
        return jsonify({'success': False, 'error': 'No service found'}), 404
    serviceIP = returnData['ip']
    servicePort = returnData['port']
    requestURL = f"http://{serviceIP}:{servicePort}/health"
    try:
        response = requests.get(requestURL, timeout=5)
        if response.status_code == 200:
            return jsonify({'success': True, 'result': returnData})
        else:
            service_info.pop(service_name, None)
            return jsonify({'success': False, 'error': f'Service returned status code {response.status_code}'})
    except requests.RequestException as e:
        service_info.pop(service_name, None)
        logger.error(f"Error connecting to service {service_name}: {str(e)}")
        return jsonify({'success': False, 'error': 'Service not responding or unreachable'}), 503


def main():
    logger.info("Starting Service-Registry application")
    app.run(host='0.0.0.0', port=9090)

if __name__ == "__main__":
    main()

