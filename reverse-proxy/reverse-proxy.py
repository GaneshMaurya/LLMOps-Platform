from flask import Flask, request, Response, render_template
import requests
import os
from dotenv import load_dotenv

ENV_FILE='/exports/applications/.env'
load_dotenv(ENV_FILE)

app = Flask(__name__)

LIFECYCLE_IP = os.getenv("life_cycle_manager_ip")
LIFECYCLE_PORT = os.getenv("life_cycle_manager_port")

MODEL_REGISTRY_IP = os.getenv("model_registry_ip")
MODEL_REGISTRY_PORT = os.getenv("model_registry_port")

SERVICE_REGISTRY_IP = os.getenv("service_registry_ip")
SERVICE_REGISTRY_PORT = os.getenv("service_registry_port")

# Reusable proxy function
def proxy_request(target_url):
    resp = requests.request(
        method=request.method,
        url=target_url,
        headers={k: v for k, v in request.headers if k.lower() != 'host'},
        data=request.get_data(),
        cookies=request.cookies,
        allow_redirects=False,
    )
    return Response(resp.content, resp.status_code, resp.raw.headers.items())

# Reverse proxy routes
@app.route("/controller/<path:path>", methods=["GET", "POST", "PUT", "DELETE"])
def proxy_lifecycle(path):
    return proxy_request(f"http://{LIFECYCLE_IP}:{LIFECYCLE_PORT}/{path}")

@app.route("/registry/<path:path>", methods=["GET", "POST", "PUT", "DELETE"])
def proxy_model_registry(path):
    return proxy_request(f"http://{MODEL_REGISTRY_IP}:{MODEL_REGISTRY_PORT}/{path}")

@app.route("/service-registry/<path:path>", methods=["GET", "POST", "PUT", "DELETE"])
def proxy_service_registry(path):
    return proxy_request(f"http://{SERVICE_REGISTRY_IP}:{SERVICE_REGISTRY_PORT}/{path}")

# Default route
@app.route("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6000)
