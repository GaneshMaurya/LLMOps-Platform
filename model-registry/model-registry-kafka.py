import threading
import os
import uuid
import shutil
import zipfile
import sqlite3
import json
import logging
import tempfile
import subprocess
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Path
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any, Tuple
import uvicorn
from dotenv import load_dotenv
import requests
import socket
from config import DB_PATH, NFS_BASE_DIR, ENV_PATH, MODELS_BASE_DIR, KAFKA_BOOTSTRAP_SERVERS
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("model_registry")

app = FastAPI(title="Model Registry with Validation", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

os.makedirs(NFS_BASE_DIR, exist_ok=True)
os.makedirs(MODELS_BASE_DIR, exist_ok=True)

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    # Existing models table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS models (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_id TEXT,
            model_name TEXT,
            user_id TEXT,
            version INTEGER,
            timestamp TEXT,
            metadata TEXT,
            storage_path TEXT,
            status TEXT,
            validation_result TEXT
        )
    ''')
    # New model_paths table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS model_paths (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_id TEXT,
            model_path TEXT,
            version INTEGER
        )
    ''')
    conn.commit()
    conn.close()

class ModelMetadata(BaseModel):
    model_id: str
    model_name: str
    user_id: str
    version: int
    timestamp: str
    metadata: str
    storage_path: str
    status: str
    validation_result: Optional[str] = None

# Add these constants near other configuration settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '192.168.33.61:29092')  # Update with your Kafka broker address
KAFKA_REQUEST_TOPIC = 'model-request'
KAFKA_RESPONSE_TOPIC = 'model-response'
KAFKA_CONSUMER_GROUP = 'model-registry-group'

def process_fetch_model_version(model_id: str, version: int):
    """Process fetch model request for specific version"""
    conn = get_db()
    row = conn.execute('''
        SELECT storage_path, model_name FROM models WHERE model_id = ? AND version = ?
    ''', (model_id, version)).fetchone()
    path_row = conn.execute('''
        SELECT model_path FROM model_paths WHERE model_id = ? AND version = ?
    ''', (model_id, version)).fetchone()
    conn.close()

    if not row or not path_row:
        raise Exception(f"Model version not found: {model_id} v{version}")

    # Find executable paths
    storage_path = Path(row["storage_path"])
    executable_paths = [
        str(storage_path / f"{row['model_name']}_{folder}")
        for folder in os.listdir(storage_path)
        if os.path.isdir(storage_path / folder) and os.path.exists(storage_path / f"{row['model_name']}_{folder}")
    ]

    return {
        "app_path": row["storage_path"],
        "model_path": path_row["model_path"],
        "model_name": row["model_name"],
        "version": version,
        "executable_paths": executable_paths
    }

def process_fetch_latest_model(model_id: str):
    """Process fetch latest model request"""
    conn = get_db()
    row = conn.execute('''
        SELECT storage_path, model_name, version FROM models WHERE model_id = ? ORDER BY version DESC LIMIT 1
    ''', (model_id,)).fetchone()
    path_row = conn.execute('''
        SELECT model_path FROM model_paths WHERE model_id = ? ORDER BY version DESC LIMIT 1
    ''', (model_id,)).fetchone()
    conn.close()

    if not row or not path_row:
        raise Exception(f"Model not found: {model_id}")

    # Find executable paths
    storage_path = Path(row["storage_path"])
    executable_paths = [
        str(storage_path / f"{row['model_name']}_{folder}")
        for folder in os.listdir(storage_path)
        if os.path.isdir(storage_path / folder) and os.path.exists(storage_path / f"{row['model_name']}_{folder}")
    ]

    return {
        "app_path": row["storage_path"],
        "model_path": path_row["model_path"],
        "model_name": row["model_name"],
        "version": row["version"],
        "executable_paths": executable_paths
    }

def kafka_request_consumer():
    """Consumer for model-request topic"""
    kafka_logger.info("Starting Kafka consumer for model-request topic")
    
    consumer = KafkaConsumer(
        KAFKA_REQUEST_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    for message in consumer:
        try:
            kafka_logger.info(f"Received message: {message.value}")
            request_data = message.value
            
            # Extract request parameters
            model_id = request_data.get('model_id')
            version = request_data.get('version')
            request_id = request_data.get('request_id')
            
            if not model_id:
                error_response = {
                    'request_id': request_id,
                    'status': 'error',
                    'message': 'Missing model_id in request'
                }
                producer.send(KAFKA_RESPONSE_TOPIC, error_response)
                continue
            
            # Process the request
            try:
                if version:
                    # Fetch specific version
                    result = process_fetch_model_version(model_id, version)
                else:
                    # Fetch latest version
                    result = process_fetch_latest_model(model_id)
                
                # Add request_id to the response
                result['request_id'] = request_id
                result['status'] = 'success'
                
                # Send response
                producer.send(KAFKA_RESPONSE_TOPIC, result)
                kafka_logger.info(f"Sent response for request_id: {request_id}")
                
            except Exception as e:
                error_response = {
                    'request_id': request_id,
                    'status': 'error',
                    'message': str(e)
                }
                producer.send(KAFKA_RESPONSE_TOPIC, error_response)
                kafka_logger.error(f"Error processing request {request_id}: {str(e)}")
        
        except Exception as e:
            kafka_logger.error(f"Error processing Kafka message: {str(e)}")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Setup logging for Kafka
kafka_logger = logging.getLogger("kafka_handler")
kafka_logger.setLevel(logging.INFO)

class ValidationError(Exception):
    pass

def construct_nfs_paths(model_id: str, version: int) -> Tuple[str, str]:
    model_path = os.path.join(MODELS_BASE_DIR,  model_id, f"v{version}")
    app_path = os.path.join(NFS_BASE_DIR, model_id, f"v{version}")
    return model_path, app_path

def get_latest_version(conn, model_id: str):
    cur = conn.execute('SELECT MAX(version) FROM models WHERE model_id = ?', (model_id,))
    row = cur.fetchone()
    return (row[0] or 0) + 1

# ---- Validation Functions ----

def validate_config_yml(content: str) -> Tuple[bool, Dict]:
    logger.info("Validating config.yml")
    try:
        if not content.strip():
            return False, {"error": "config.yml is empty"}
        # Basic YAML structure check (not parsing, just ensuring it's not malformed)
        if not (content.startswith('---') or '=' in content or ':' in content):
            return False, {"error": "config.yml does not appear to be valid YAML"}
        logger.info("config.yml validation passed")
        return True, {"message": "config.yml validation passed"}
    except Exception as e:
        logger.error(f"Error validating config.yml: {e}")
        return False, {"error": f"Error validating config.yml: {str(e)}"}

def validate_main_py(content: str) -> Tuple[bool, Dict]:
    logger.info("Validating main.py")
    try:
        if not content:
            return False, {"error": "main.py is empty"}
        common_imports = ["import", "from"]
        if not any(imp in content for imp in common_imports):
            return False, {"error": "main.py doesn't contain any imports"}
        logger.info("main.py validation passed")
        return True, {"message": "main.py validation passed"}
    except Exception as e:
        logger.error(f"Error validating main.py: {e}")
        return False, {"error": f"Error validating main.py: {str(e)}"}

def validate_requirements_txt(content: str) -> Tuple[bool, Dict]:
    logger.info("Validating requirements.txt")
    try:
        if not content.strip():
            return False, {"error": "requirements.txt is empty"}
        requirements = [r for r in content.split('\n') if r.strip()]
        invalid_lines = [line for line in requirements if not line.startswith('#') and (not line or line.isspace())]
        if invalid_lines:
            return False, {"error": f"Invalid requirements format: {invalid_lines}"}
        logger.info("requirements.txt validation passed")
        return True, {"message": "requirements.txt validation passed"}
    except Exception as e:
        logger.error(f"Error validating requirements.txt: {e}")
        return False, {"error": f"Error validating requirements.txt: {str(e)}"}

def validate_model_pth(model_path: str) -> Tuple[bool, Dict]:
    logger.info("Validating model.pth")
    try:
        if not os.path.exists(model_path):
            return False, {"error": "model.pth file not found"}
        file_size = os.path.getsize(model_path)
        if file_size == 0:
            return False, {"error": "model.pth is empty"}
        logger.info(f"model.pth validation passed: {file_size} bytes")
        return True, {"message": f"model.pth validation passed: {file_size} bytes"}
    except Exception as e:
        logger.error(f"Error validating model.pth: {e}")
        return False, {"error": f"Error validating model.pth: {str(e)}"}

def validate_folder_structure(extract_dir: str) -> Tuple[bool, Dict]:
    logger.info(f"Validating folder structure in {extract_dir}")
    try:
        # Check for config.yml and model.pth in root
        root_files = os.listdir(extract_dir)
        if "config.yml" not in root_files or "model.pth" not in root_files:
            return False, {"error": "config.yml or model.pth missing in zip root"}

        # Check for at least one folder containing main.py and requirements.txt
        folders = [f for f in root_files if os.path.isdir(os.path.join(extract_dir, f))]
        if not folders:
            return False, {"error": "No folders found in zip"}

        folder_results = {}
        for folder in folders:
            folder_path = os.path.join(extract_dir, folder)
            folder_files = os.listdir(folder_path)
            if "main.py" not in folder_files or "requirements.txt" not in folder_files:
                return False, {"error": f"Folder {folder} missing main.py or requirements.txt"}
            folder_results[folder] = {"main.py": True, "requirements.txt": True}

        logger.info("Folder structure validation passed")
        return True, {"message": "Folder structure validation passed", "folders": folder_results}
    except Exception as e:
        logger.error(f"Error validating folder structure: {e}")
        return False, {"error": f"Error validating folder structure: {str(e)}"}

def find_file_in_zip_structure(extract_dir: str) -> Dict[str, str]:
    logger.info(f"Finding files in extracted structure: {extract_dir}")
    found_files = {}

    # Find config.yml and model.pth in root
    for filename in ["config.yml", "model.pth"]:
        file_path = os.path.join(extract_dir, filename)
        if os.path.isfile(file_path):
            found_files[filename] = file_path
        else:
            raise FileNotFoundError(f"Required file {filename} not found in zip root")

    # Find folders and their main.py and requirements.txt
    folders = [f for f in os.listdir(extract_dir) if os.path.isdir(os.path.join(extract_dir, f))]
    for folder in folders:
        for filename in ["main.py", "requirements.txt"]:
            file_path = os.path.join(extract_dir, folder, filename)
            if os.path.isfile(file_path):
                found_files[f"{folder}/{filename}"] = file_path
            else:
                raise FileNotFoundError(f"Required file {filename} not found in folder {folder}")

    return found_files

def build_all_executables(project_dir: str, model_name: str) -> List[str]:
    logger.info(f"Building executables for project_dir: {project_dir}, model_name: {model_name}")
    project_path = Path(project_dir).resolve()
    executable_paths = []

    if not project_path.is_dir():
        raise ValidationError(f"Provided project path '{project_path}' is not a directory.")

    for service_path in project_path.iterdir():
        if not service_path.is_dir():
            continue

        service_name = service_path.name
        main_py = service_path / "main.py"
        requirements = service_path / "requirements.txt"

        if not main_py.exists() or not requirements.exists():
            logger.warning(f"Skipping '{service_name}' — missing main.py or requirements.txt")
            continue

        logger.info(f"Building executable for service: {service_name}")

        # Create virtual environment
        venv_path = service_path / "venv"
        subprocess.run(["python3", "-m", "venv", str(venv_path)], check=True)

        pip = venv_path / "bin" / "pip"
        python = venv_path / "bin" / "python"

        # Install requirements
        subprocess.run([str(pip), "install", "-r", str(requirements)], check=True)
        subprocess.run([str(pip), "install", "pyinstaller"], check=True)

        # Run PyInstaller
        executable_name = f"{model_name}_{service_name}"
        subprocess.run([
            str(python), "-m", "PyInstaller",
            "--onefile",
            "--name", executable_name,
            "--collect-all", service_name,  # Optional, retained from original
            str(main_py)
        ], check=True, cwd=service_path)

        # Move executable to project_dir
        built_exe = service_path / "dist" / executable_name
        target_path = project_path / executable_name
        shutil.move(str(built_exe), str(target_path))
        executable_paths.append(str(target_path))

        # Clean up
        for folder in ["build", "dist", "__pycache__"]:
            shutil.rmtree(service_path / folder, ignore_errors=True)

        for file in service_path.glob("*.spec"):
            file.unlink()

        shutil.rmtree(venv_path, ignore_errors=True)

        logger.info(f"Built executable: {target_path}")

    if not executable_paths:
        raise ValidationError("No executables were built — no valid service folders found")

    logger.info(f"All executables stored in: {project_path}")
    return executable_paths

def run_validation(
    model_id: str,
    model_file_path: str,
    config_content: str,
    folder_contents: Dict[str, Dict[str, str]],
    user_id: str = None
) -> Dict:
    logger.info(f"Starting validation for model_id: {model_id}")

    config_result = validate_config_yml(config_content)
    model_result = validate_model_pth(model_file_path)

    # Validate main.py and requirements.txt for each folder
    folder_results = {}
    all_valid = True
    for folder, contents in folder_contents.items():
        main_result = validate_main_py(contents["main.py"])
        req_result = validate_requirements_txt(contents["requirements.txt"])
        folder_results[folder] = {
            "main_py": main_result[1],
            "requirements_txt": req_result[1]
        }
        if not (main_result[0] and req_result[0]):
            all_valid = False

    is_valid = config_result[0] and model_result[0] and all_valid

    result = {
        "model_id": model_id,
        "is_valid": is_valid,
        "validation_time": datetime.utcnow().isoformat(),
        "details": {
            "config_yml": config_result[1],
            "model_pth": model_result[1],
            "folders": folder_results
        }
    }

    if user_id:
        result["user_id"] = user_id

    if not is_valid:
        errors = []
        if not config_result[0]:
            errors.append(config_result[1].get("error", "config.yml validation failed"))
        if not model_result[0]:
            errors.append(model_result[1].get("error", "model.pth validation failed"))
        for folder, res in folder_results.items():
            if not res["main_py"].get("message"):
                errors.append(res["main_py"].get("error", f"{folder}/main.py validation failed"))
            if not res["requirements_txt"].get("message"):
                errors.append(res["requirements_txt"].get("error", f"{folder}/requirements.txt validation failed"))
        result["errors"] = errors

    logger.info(f"Validation completed for model_id: {model_id}, is_valid: {is_valid}")
    return result

@app.post("/registry/upload-and-validate/{model_id}", response_model=Dict)
async def upload_and_validate_model(
    model_id: str = Path(),
    model_file: UploadFile = File(...),
    user_id: str = Form(...),
    model_name: str = Form(...),
    metadata: str = Form(default="{}")
):
    logger.info(f"Received upload and validation request for model_id: {model_id}")
    logger.info(f"User ID: {user_id}, Model Name: {model_name}, Metadata: {metadata}")

    # Create temporary directory for validation
    temp_dir = tempfile.mkdtemp()
    extract_dir = os.path.join(temp_dir, "extracted")
    os.makedirs(extract_dir, exist_ok=True)

    # Create connection for database operations
    conn = get_db()

    try:
        # Check if model already exists for this user and get version
        version = get_latest_version(conn, model_id)

        # Construct versioned storage paths
        model_nfs_path, app_nfs_path = construct_nfs_paths(model_id, version)
        os.makedirs(model_nfs_path, exist_ok=True)
        os.makedirs(app_nfs_path, exist_ok=True)

        # Save zip file temporarily
        zip_path = os.path.join(temp_dir, "model.zip")
        with open(zip_path, "wb") as f:
            content = await model_file.read()
            f.write(content)

        # Verify it's a valid zip file
        if not zipfile.is_zipfile(zip_path):
            raise ValidationError("Uploaded file is not a valid zip file")

        # Extract the zip for validation
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        # Validate folder structure
        structure_result = validate_folder_structure(extract_dir)
        if not structure_result[0]:
            return JSONResponse(
                status_code=400,
                content={
                    "request_id": f"val_{model_id}",
                    "status": "FAILED",
                    "error": structure_result[1]["error"]
                }
            )

        # Find files
        try:
            file_paths = find_file_in_zip_structure(extract_dir)
        except FileNotFoundError as e:
            return JSONResponse(
                status_code=400,
                content={
                    "request_id": f"val_{model_id}",
                    "status": "FAILED",
                    "error": str(e)
                }
            )

        # Read file contents for validation
        with open(file_paths["config.yml"], "r", encoding="utf-8") as f:
            config_content = f.read()

        # Collect folder contents
        folder_contents = {}
        folders = [f for f in os.listdir(extract_dir) if os.path.isdir(os.path.join(extract_dir, f))]
        for folder in folders:
            with open(file_paths[f"{folder}/main.py"], "r", encoding="utf-8") as f:
                main_content = f.read()
            with open(file_paths[f"{folder}/requirements.txt"], "r", encoding="utf-8") as f:
                req_content = f.read()
            folder_contents[folder] = {"main.py": main_content, "requirements.txt": req_content}

        # Run validation
        validation_result = run_validation(
            model_id=model_id,
            model_file_path=file_paths["model.pth"],
            config_content=config_content,
            folder_contents=folder_contents,
            user_id=user_id
        )

        # Only proceed with storage if validation passed
        if validation_result["is_valid"]:
            # Store model.pth
            model_target_path = os.path.join(model_nfs_path, "model.pth")
            os.makedirs(os.path.dirname(model_target_path), exist_ok=True)
            shutil.copyfile(file_paths["model.pth"], model_target_path)
            logger.info(f"Stored model.pth to {model_target_path}")

            # Store remaining files to app_nfs_path, preserving structure
            for item in os.listdir(extract_dir):
                src_path = os.path.join(extract_dir, item)
                dst_path = os.path.join(app_nfs_path, item)
                if os.path.isfile(src_path):
                    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                    shutil.copyfile(src_path, dst_path)
                    logger.info(f"Stored {item} to {dst_path}")
                elif os.path.isdir(src_path):
                    shutil.copytree(src_path, dst_path, dirs_exist_ok=True)
                    logger.info(f"Stored folder {item} to {dst_path}")

            # Create executables
            # executable_paths = build_all_executables(app_nfs_path, model_name)

            # Store in databases
            conn.execute('''
                INSERT INTO models (model_id, model_name, user_id, version, timestamp, metadata, storage_path, status, validation_result)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                model_id,
                model_name,
                user_id,
                version,
                datetime.utcnow().isoformat(),
                metadata,
                app_nfs_path,
                "validated_stored",
                json.dumps(validation_result)
            ))

            conn.execute('''
                INSERT INTO model_paths (model_id, model_path, version)
                VALUES (?, ?, ?)
            ''', (
                model_id,
                model_nfs_path,
                version
            ))

            conn.commit()

            return {
                "request_id": f"val_{model_id}",
                "status": "COMPLETED",
                "result": validation_result,
                "storage": {
                    "model_id": model_id,
                    "model_name": model_name,
                    "version": version,
                    "model_path": model_nfs_path,
                    "app_path": app_nfs_path,
                    "executable_paths": executable_paths
                }
            }
        else:
            conn.execute('''
                INSERT INTO models (model_id, model_name, user_id, version, timestamp, metadata, storage_path, status, validation_result)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                model_id,
                model_name,
                user_id,
                version,
                datetime.utcnow().isoformat(),
                metadata,
                None,
                "validation_failed",
                json.dumps(validation_result)
            ))
            conn.commit()

            return JSONResponse(
                status_code=400,
                content={
                    "request_id": f"val_{model_id}",
                    "status": "FAILED",
                    "result": validation_result,
                }
            )

    except ValidationError as e:
        logger.error(f"Validation error: {str(e)}")
        return JSONResponse(
            status_code=400,
            content={
                "request_id": f"val_{model_id}",
                "status": "FAILED",
                "error": str(e)
            }
        )

    except Exception as e:
        logger.error(f"Server error: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "request_id": f"val_{model_id}",
                "status": "FAILED",
                "error": f"Server error: {str(e)}"
            }
        )

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        conn.close()

@app.post("/registry/upload-model/{model_id}")
async def upload_model(
    model_id: str = Path(),
    model: UploadFile = File(...),
    user_id: str = Form(...),
    model_name: str = Form(...),
    metadata: str = Form(default="{}")
):
    conn = get_db()

    # Check if model already exists for this user
    row = conn.execute('''
        SELECT model_id FROM models 
        WHERE model_id = ? 
        ORDER BY version DESC LIMIT 1
    ''', (model_id,)).fetchone()

    if row:
        version = get_latest_version(conn, model_id)
    else:
        version = 1

    # Construct versioned storage paths
    model_nfs_path, app_nfs_path = construct_nfs_paths(model_id, version)
    os.makedirs(model_nfs_path, exist_ok=True)
    os.makedirs(app_nfs_path, exist_ok=True)

    # Save zip file temporarily and extract
    temp_dir = tempfile.mkdtemp()
    zip_path = os.path.join(temp_dir, "model.zip")
    try:
        with open(zip_path, "wb") as f:
            content = await model.read()
            f.write(content)

        # Verify it's a valid zip file
        if not zipfile.is_zipfile(zip_path):
            raise HTTPException(status_code=400, detail="Uploaded file is not a valid zip")

        # Extract the zip file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(app_nfs_path)

        # Move model.pth to model_nfs_path
        model_pth_src = os.path.join(app_nfs_path, "model.pth")
        model_pth_dst = os.path.join(model_nfs_path, "model.pth")
        if os.path.exists(model_pth_src):
            os.makedirs(os.path.dirname(model_pth_dst), exist_ok=True)
            shutil.move(model_pth_src, model_pth_dst)
            logger.info(f"Moved model.pth to {model_pth_dst}")
        else:
            raise HTTPException(status_code=400, detail="model.pth not found in zip")

        # Store metadata and model info
        conn.execute('''
            INSERT INTO models (model_id, model_name, user_id, version, timestamp, metadata, storage_path, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            model_id,
            model_name,
            user_id,
            version,
            datetime.utcnow().isoformat(),
            metadata,
            app_nfs_path,
            "stored"
        ))

        conn.execute('''
            INSERT INTO model_paths (model_id, model_path, version)
            VALUES (?, ?, ?)
        ''', (
            model_id,
            model_nfs_path,
            version
        ))

        conn.commit()
        return {"message": "Model uploaded", "model_id": model_id, "model_name": model_name, "version": version}

    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="Uploaded file is not a valid zip")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        conn.close()

@app.get("/registry/fetch-model/{model_id}/{version}")
def fetch_model_version(model_id: str, version: int):
    conn = get_db()
    row = conn.execute('''
        SELECT storage_path, model_name FROM models WHERE model_id = ? AND version = ?
    ''', (model_id, version)).fetchone()
    path_row = conn.execute('''
        SELECT model_path FROM model_paths WHERE model_id = ? AND version = ?
    ''', (model_id, version)).fetchone()
    conn.close()

    if not row or not path_row:
        raise HTTPException(status_code=404, detail="Model version not found")

    # Find executable paths
    storage_path = Path(row["storage_path"])
    executable_paths = [
        str(storage_path / f"{row['model_name']}_{folder}")
        for folder in os.listdir(storage_path)
        if os.path.isdir(storage_path / folder) and os.path.exists(storage_path / f"{row['model_name']}_{folder}")
    ]

    return {
        "app_path": row["storage_path"],
        "model_path": path_row["model_path"],
        "model_name": row["model_name"],
        "version": version,
        "executable_paths": executable_paths
    }

@app.get("/registry/fetch-model/{model_id}")
def fetch_latest_model(model_id: str):
    conn = get_db()
    row = conn.execute('''
        SELECT storage_path, model_name, version FROM models WHERE model_id = ? ORDER BY version DESC LIMIT 1
    ''', (model_id,)).fetchone()
    path_row = conn.execute('''
        SELECT model_path FROM model_paths WHERE model_id = ? ORDER BY version DESC LIMIT 1
    ''', (model_id,)).fetchone()
    conn.close()

    if not row or not path_row:
        raise HTTPException(status_code=404, detail="Model not found")

    # Find executable paths
    storage_path = Path(row["storage_path"])
    executable_paths = [
        str(storage_path / f"{row['model_name']}_{folder}")
        for folder in os.listdir(storage_path)
        if os.path.isdir(storage_path / folder) and os.path.exists(storage_path / f"{row['model_name']}_{folder}")
    ]

    return {
        "app_path": row["storage_path"],
        "model_path": path_row["model_path"],
        "model_name": row["model_name"],
        "version": row["version"],
        "executable_paths": executable_paths
    }

@app.get("/registry/fetch-validation/{model_id}/{version}")
def fetch_validation_result(model_id: str, version: int):
    conn = get_db()
    row = conn.execute('''
        SELECT validation_result, model_name FROM models WHERE model_id = ? AND version = ?
    ''', (model_id, version)).fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Model version not found")

    result = {"model_name": row["model_name"]}
    if row["validation_result"]:
        result.update(json.loads(row["validation_result"]))
    else:
        result.update({"message": "No validation result available for this model version"})
    return result

@app.get("/registry/display-model/{user_id}", response_model=List[Dict])
def display_user_models(user_id: str):
    conn = get_db()
    rows = conn.execute('''
        SELECT * FROM models WHERE user_id = ?
    ''', (user_id,)).fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.get("/registry/display-model", response_model=List[Dict])
def display_all_models():
    conn = get_db()
    rows = conn.execute('''
        SELECT * FROM models
    ''').fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.delete("/registry/delete-model/{model_id}/{version}")
def delete_model_version(model_id: str, version: int):
    conn = get_db()
    row = conn.execute('SELECT storage_path FROM models WHERE model_id = ? AND version = ?', (model_id, version)).fetchone()
    path_row = conn.execute('SELECT model_path FROM model_paths WHERE model_id = ? AND version = ?', (model_id, version)).fetchone()
    if not row or not path_row:
        conn.close()
        raise HTTPException(status_code=404, detail="Model version not found")

    if row["storage_path"] and os.path.exists(row["storage_path"]):
        shutil.rmtree(row["storage_path"], ignore_errors=True)
    if path_row["model_path"] and os.path.exists(path_row["model_path"]):
        shutil.rmtree(path_row["model_path"], ignore_errors=True)

    conn.execute('DELETE FROM models WHERE model_id = ? AND version = ?', (model_id, version))
    conn.execute('DELETE FROM model_paths WHERE model_id = ? AND version = ?', (model_id, version))
    conn.commit()
    conn.close()
    return {"message": "Model version deleted"}

@app.delete("/registry/delete-model/{model_id}")
def delete_all_versions(model_id: str):
    conn = get_db()
    rows = conn.execute('SELECT DISTINCT storage_path FROM models WHERE model_id = ?', (model_id,)).fetchall()
    path_rows = conn.execute('SELECT DISTINCT model_path FROM model_paths WHERE model_id = ?', (model_id,)).fetchall()
    if not rows:
        conn.close()
        raise HTTPException(status_code=404, detail="Model not found")

    for row in rows:
        if row["storage_path"] and os.path.exists(row["storage_path"]):
            shutil.rmtree(row["storage_path"], ignore_errors=True)
    for path_row in path_rows:
        if path_row["model_path"] and os.path.exists(path_row["model_path"]):
            shutil.rmtree(path_row["model_path"], ignore_errors=True)

    conn.execute('DELETE FROM models WHERE model_id = ?', (model_id,))
    conn.execute('DELETE FROM model_paths WHERE model_id = ?', (model_id,))
    conn.commit()
    conn.close()
    return {"message": "All versions deleted"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return '127.0.0.1'

def register_to_service_registry():
    load_dotenv(ENV_PATH)
    SERVICE_REGISTRY_URL = f"http://{os.getenv('service_registry_ip')}:{os.getenv('service_registry_port')}/service-registry/register"
    try:
        service_info = {
            "name": "model-registry",
            "ip": get_local_ip(),
            "port": 8000,
        }
        response = requests.post(SERVICE_REGISTRY_URL, json=service_info)
        if response.status_code == 200:
            logger.info("Model Registry registered successfully")
        else:
            logger.error(f"Failed to register service: {response.text}")
            exit()
    except Exception as e:
        logger.error(f"Error registering to service registry: {e}")
        exit()

def main():
    uvicorn.run("model-registry-kafka:app", host='0.0.0.0', port=8000, reload=True)

if __name__ == "__main__":
    init_db()
    kafka_thread = threading.Thread(target=kafka_request_consumer, daemon=True)
    kafka_thread.start()
    kafka_logger.info("Kafka consumer thread started")
    # register_to_service_registry()
    main()