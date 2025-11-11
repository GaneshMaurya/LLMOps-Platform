# MLOps Inference Platform

This project is a comprehensive, distributed MLOps platform for validating, storing, managing, and deploying machine learning models as self-contained web services.

It features a microservices-based architecture with a web frontend, a model registry for validation and storage, a controller for intelligent scheduling, and multiple agent nodes for provisioning deployments inside virtual machines.

## Key Features

  * **Web-Based UI:** A Flask-based frontend for user registration, login, model uploading, listing, and deployment.
  * **Microservice Architecture:** A fully distributed system where each component (frontend, controller, registry, agents) runs as a separate service.
  * **Service Discovery:** A central **Service Registry** allows services to discover each other dynamically.
  * **Automated Model Validation:** On upload, the **Model Registry** unpacks model archives and validates their contents (e.g., presence of `meta.json`, `app.py`, `requirements.txt`).
  * **Centralized & Versioned Model Storage:** Validated models are stored in a versioned directory structure on a shared **NFS (Network File System)**.
  * **Resource-Based Scheduling:** The **Controller** subscribes to a **Kafka** topic to receive real-time performance metrics (CPU, memory) from all available agent nodes. It uses this data to schedule new deployments on the least-loaded agent.
  * **Automated VM Provisioning:** **Agent** nodes use **Vagrant** to automatically provision and configure new virtual machines for each deployment, ensuring complete isolation.
  * **In-Browser Model Editing:** Users can edit model files (like `meta.json` or `app.py`) directly from the web UI, which saves the changes as a new model version.

## System Architecture

The platform is composed of several key microservices that communicate via REST APIs, Kafka, and a shared NFS.

1.  **Frontend (`frontend/app.py`)**

      * **Technology:** Flask
      * **Purpose:** The main user interface.
      * **Functions:**
          * Handles user authentication and sessions (using `users.db` SQLite).
          * Provides pages to upload, list, edit, and deploy models.
          * Communicates with the Service Registry to find other services.
          * Sends deployment requests to the Controller.
          * Sends upload requests to the Model Registry.

2.  **Service Registry (`service-registry/registry.py`)**

      * **Technology:** Flask
      * **Purpose:** The central discovery mechanism.
      * **Functions:**
          * Provides a `/register` endpoint for services to announce their IP and port.
          * Provides a `/getServiceInfo` endpoint for services to find each other.
          * Performs health checks on registered services and removes them if they become unresponsive.

3.  **Model Registry (`model-registry/model-registry.py`)**

      * **Technology:** FastAPI, SQLite, NFS
      * **Purpose:** Manages the storage and validation of all ML models.
      * **Functions:**
          * Provides `/registry/upload-and-validate` to accept new model `.zip` files.
          * Validates the zip contents (see "Model Package Format" below).
          * Stores validated model files on an NFS share (e.g., `/exports/models/<model-id>/v1/`).
          * Records model metadata (version, user, path) in a SQLite database (`model_registry.db`).
          * Provides `/registry/fetch-model` for other services (like Agents) to get the file path for a specific model version.

4.  **Controller Service (`controller-Service/controller.py`)**

      * **Technology:** Flask, Kafka Consumer
      * **Purpose:** The "brains" of the platform; orchestrates deployments.
      * **Functions:**
          * Subscribes to the `system-metrics` Kafka topic to get live performance data from all Agents.
          * Provides a `/controller/deploy` endpoint.
          * When a deployment is requested, it selects the best Agent (lowest CPU/memory load) based on the latest Kafka metrics.
          * Forwards the deployment request to the chosen Agent.
          * Manages a registry of active deployments.

5.  **Agent Service (`agent-Service/agent.py`)**

      * **Technology:** Flask, Kafka Producer, Vagrant, `psutil`
      * **Purpose:** A worker node that runs on multiple machines to provision VMs and report metrics.
      * **Functions:**
          * Continuously collects its own system metrics (CPU, memory, disk) using `psutil`.
          * Publishes these metrics to the `system-metrics` Kafka topic.
          * Provides a `/create-vm` endpoint:
              * Fetches the model's NFS path from the Model Registry.
              * Generates a dynamic `Vagrantfile` that provisions a new VM, mounts the model's NFS path, installs dependencies (`pip install -r requirements.txt`), and starts the model's services.
              * Runs `vagrant up` to create and start the VM.
              * Returns the VM's `access_url` to the Controller.
          * Provides a `/stop-vm` endpoint to destroy Vagrant VMs.

6.  **Kafka (`controller-Service/kafka-docker-setup/`)**

      * **Technology:** Docker Compose (Confluentinc images)
      * **Purpose:** A message bus for decoupling Agents from the Controller.
      * **Components:** Zookeeper, Kafka, and Kafka-UI for monitoring.

7.  **NFS Server (`bootstrap-service/boot_nfs_server.sh`)**

      * **Purpose:** Provides shared storage for all services.
      * **Exports:**
          * `/exports/models`: For versioned model files.
          * `/exports/applications`: Holds the source code for all microservices.
          * `/exports/datasets`: (Presumably for shared datasets).

## Core Workflows

### 1\. Model Upload and Validation

1.  A user logs into the **Frontend** and navigates to the "Upload" page.
2.  The user provides a Model Name and a `.zip` file.
3.  The **Frontend** POSTs the data to the **Model Registry's** `/upload-and-validate` endpoint.
4.  The **Model Registry** validates the package (see format below).
5.  If valid, the files are extracted and saved to a new versioned folder on the **NFS Server** (e.g., `/exports/models/model-uuid/v2/`).
6.  A new entry is made in the `model_registry.db` linking the model, version, and its NFS path.
7.  The **Frontend** shows a success message.

### 2\. Model Deployment

1.  A user logs into the **Frontend** and navigates to the "Models" page.
2.  The user selects a model and a version and clicks "Deploy."
3.  The **Frontend** sends a POST request to the **Controller's** `/controller/deploy` endpoint with the `model_id` and `version`.
4.  The **Controller** consults its internal metrics (from Kafka) and selects the healthiest (least-loaded) **Agent** node.
5.  The **Controller** forwards the deploy request to that **Agent's** `/create-vm` endpoint.
6.  The **Agent** receives the request:
    a.  Calls the **Model Registry** to get the NFS path for the requested model and version.
    b.  Creates a new deployment directory (e.g., `./deployments/deployment-uuid/`).
    c.  Generates a dynamic `Vagrantfile` inside this directory.
    d.  The `Vagrantfile` is configured to:
    \* Use the `ubuntu-ml` Vagrant box.
    \* Mount the model's NFS path (e.g., `/exports/models/model-uuid/v2/`) into the VM's `/app` directory.
    \* Run a provision script to `pip install -r /app/requirements.txt` and execute the `start_commands` from the model's `meta.json`.
    e.  The **Agent** runs `vagrant up`.
    f.  It then determines the access URL (e.g., `http://<agent-ip>:<forwarded-port>`).
    g.  The **Agent** returns this `access_url` to the **Controller**.
7.  The **Controller** returns the `access_url` to the **Frontend**.
8.  The **Frontend** displays the final URL to the user, who can now access their deployed model.

## Model Package Format

To be compatible with the platform, models must be zipped with a specific file structure in the root of the archive:

  * `meta.json` (Required): A JSON file detailing setup and start commands.
    ```json
    {
        "setup_commands": [
            "pip install -r requirements.txt"
        ],
        "start_commands": [
            "python3 app.py &",
            "streamlit run webapp.py"
        ],
        "description": "A description of the model.",
        "ports": {
            "backend": 5000,
            "frontend": 8501
        }
    }
    ```
  * `app.py` (Required): The Flask (or other) backend API for the model.
  * `webapp.py` (Optional): A Streamlit (or other) frontend application.
  * `requirements.txt` (Required): A list of all Python dependencies.
  * `model.pth` (Required): The trained model file (or any other necessary model artifacts).

**Note:** The platform expects all files to be in the root of the zip file, not nested in a subfolder.

## Setup and Installation

This platform is designed for a distributed environment, managed by a central bootstrap script.

1.  **NFS Server Setup:**

      * On a dedicated machine, run `bootstrap-service/boot_nfs_server.sh` to install and configure the NFS server.
      * This will export `/exports/models`, `/exports/applications`, and `/exports/datasets`.

2.  **Environment Configuration:**

      * Copy the source code of all services (frontend, controller, agent-service, etc.) into the `/exports/applications/` directory on the NFS server.
      * Create a `.env` file in `/exports/applications/` and define the IPs, usernames, and passwords for all machines in the cluster (e.g., `service_registry_ip`, `controller_ip`, `agent_1_ip`, `nfs_server_ip`, etc.).

3.  **Client Machine Setup:**

      * The `bootstrap.sh` script is the master installer. It will:
      * SSH into each machine defined in the `.env` file.
      * Run the `boot_nfs_client.sh` script on each machine to mount the shared `/exports/` directory.
      * Run the specific bootstrap script for the service designated for that machine (e.g., `bootstrap-controller.sh` on the controller machine).

4.  **Service Initialization:**

      * Each service's bootstrap script (e.g., `bootstrap-agent.sh`) creates a Python virtual environment, installs dependencies from its `requirements.txt`, and starts the service as a background process using `nohup`.
      * Services automatically register with the Service Registry upon startup.

5.  **Start Kafka:**

      * On the machine designated for Kafka, run `docker-compose up -d` from the `controller-Service/kafka-docker-setup/` directory.
