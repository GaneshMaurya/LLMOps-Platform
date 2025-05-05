import os
import uuid
import shutil
import zipfile
import sqlite3
import json
import logging
import tempfile
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Path
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any, Tuple
import uvicorn
from dotenv import load_dotenv
import requests
import socket
from config import DB_PATH, NFS_BASE_DIR, ENV_PATH

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

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
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

class ValidationError(Exception):
    pass

def construct_nfs_path(model_id: str, version: int):
    return os.path.join(NFS_BASE_DIR, model_id, f"v{version}")

def get_latest_version(conn, model_id: str):
    cur = conn.execute('SELECT MAX(version) FROM models WHERE model_id = ?', (model_id,))
    row = cur.fetchone()
    return (row[0] or 0) + 1

# ---- Validation Functions ----

def validate_meta_json(meta_content: str) -> Tuple[bool, Dict[str, Any]]:
    logger.info("Validating meta.json")
    try:
        meta_data = json.loads(meta_content)
        
        required_fields = ["setup_commands", "start_commands"]
        for field in required_fields:
            if field not in meta_data:
                return False, {"error": f"Missing required field: {field} in meta.json"}
        
        if not isinstance(meta_data["setup_commands"], list):
            return False, {"error": "setup_commands must be a list"}
            
        if not isinstance(meta_data["start_commands"], list):
            return False, {"error": "start_commands must be a list"}
        
        logger.info("meta.json validation passed")
        return True, meta_data
    except json.JSONDecodeError as e:
        logger.error(f"meta.json is not valid JSON: {e}")
        return False, {"error": f"meta.json is not valid JSON: {str(e)}"}
    except Exception as e:
        logger.error(f"Error validating meta.json: {e}")
        return False, {"error": f"Error validating meta.json: {str(e)}"}

def validate_app_py(app_content: str) -> Tuple[bool, Dict]:
    logger.info("Validating app.py")
    try:
        if not app_content:
            return False, {"error": "app.py is empty"}
        
        common_imports = ["import", "from"]
        if not any(imp in app_content for imp in common_imports):
            return False, {"error": "app.py doesn't contain any imports"}
        
        if "__main__" not in app_content and "app.run" not in app_content:
            logger.warning("app.py might not have a main block or app.run() call")
        
        logger.info("app.py validation passed")
        return True, {"message": "app.py validation passed"}
    except Exception as e:
        logger.error(f"Error validating app.py: {e}")
        return False, {"error": f"Error validating app.py: {str(e)}"}
    
def validate_web_app_py(webapp_content: str) -> Tuple[bool, Dict]:
    logger.info("Validating webapp.py")
    try:
        if not webapp_content:
            return False, {"error": "webapp.py is empty"}
        
        common_imports = ["import", "from"]
        if not any(imp in webapp_content for imp in common_imports):
            return False, {"error": "app.py doesn't contain any imports"}
        
        logger.info("webapp.py validation passed")
        return True, {"message": "webapp.py validation passed"}
    except Exception as e:
        logger.error(f"Error validating webapp.py: {e}")
        return False, {"error": f"Error validating webapp.py: {str(e)}"}

def validate_requirements_txt(requirements_content: str) -> Tuple[bool, Dict]:
    logger.info("Validating requirements.txt")
    try:
        if not requirements_content.strip():
            return False, {"error": "requirements.txt is empty"}
        
        requirements = [r for r in requirements_content.split('\n') if r.strip()]
        
        invalid_lines = []
        for line in requirements:
            if line.startswith('#'):
                continue
                
            if not line or line.isspace():
                invalid_lines.append(line)
        
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

def find_file_in_zip_structure(extract_dir: str, required_files: List[str], optional_files: List[str] = None) -> Dict[str, str]:
    """
    Find required and optional files in extracted zip structure.
    
    Args:
        extract_dir: Directory where zip was extracted
        required_files: List of files that must be found
        optional_files: List of files that are optional
        
    Returns:
        Dictionary mapping file names to their full paths
    """
    logger.info(f"Finding files in extracted structure: {extract_dir}")
    
    optional_files = optional_files or []
    all_files_to_find = set(required_files + optional_files)
    
    # Initialize results dictionary
    found_files = {}
    files_to_find = set(all_files_to_find)
    
    # First check if files are directly in the root
    for filename in files_to_find.copy():
        file_path = os.path.join(extract_dir, filename)
        if os.path.isfile(file_path):
            found_files[filename] = file_path
            files_to_find.remove(filename)
    
    # If all files are found, return early
    if not files_to_find:
        logger.info("All files found at root level")
        return found_files
    
    # Use recursive search for remaining files
    for filename in files_to_find.copy():
        for root, _, files in os.walk(extract_dir):
            if filename in files:
                found_files[filename] = os.path.join(root, filename)
                logger.info(f"Found {filename} at {found_files[filename]}")
                files_to_find.remove(filename)
                break
    
    # Check if any required files are still missing
    missing_required = [f for f in required_files if f not in found_files]
    if missing_required:
        error_msg = f"Required files not found in the zip: {', '.join(missing_required)}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    return found_files

def run_validation(
    model_id: str,
    model_file_path: str,
    meta_content: str,
    app_content: str,
    requirements_content: str,
    webapp_content: str = None,
    user_id: str = None
) -> Dict:
    logger.info(f"Starting validation for model_id: {model_id}")
    
    meta_result = validate_meta_json(meta_content)
    app_result = validate_app_py(app_content)
    req_result = validate_requirements_txt(requirements_content)
    model_result = validate_model_pth(model_file_path)
    
    # Only validate webapp.py if it's provided
    if webapp_content is not None:
        webapp_result = validate_web_app_py(webapp_content)
        validation_results = [meta_result[0], app_result[0], webapp_result[0], req_result[0], model_result[0]]
    else:
        webapp_result = (True, {"message": "webapp.py is optional and was not provided"})
        validation_results = [meta_result[0], app_result[0], req_result[0], model_result[0]]
    
    is_valid = all(validation_results)
    
    result = {
        "model_id": model_id,
        "is_valid": is_valid,
        "validation_time": datetime.utcnow().isoformat(),
        "details": {
            "meta_json": meta_result[1],
            "app_py": app_result[1],
            "requirements_txt": req_result[1],
            "model_pth": model_result[1]
        }
    }
    
    # Only include webapp.py validation result if it was provided
    if webapp_content is not None:
        result["details"]["webapp_py"] = webapp_result[1]
    
    # Include user_id in the result if provided
    if user_id:
        result["user_id"] = user_id
    
    if not is_valid:
        errors = []
        if not meta_result[0]:
            errors.append(meta_result[1].get("error", "meta.json validation failed"))
        if not app_result[0]:
            errors.append(app_result[1].get("error", "app.py validation failed"))
        if webapp_content is not None and not webapp_result[0]:
            errors.append(webapp_result[1].get("error", "webapp.py validation failed"))
        if not req_result[0]:
            errors.append(req_result[1].get("error", "requirements.txt validation failed"))
        if not model_result[0]:
            errors.append(model_result[1].get("error", "model.pth validation failed"))
        
        result["errors"] = errors
    
    logger.info(f"Validation completed for model_id: {model_id}, is_valid: {is_valid}")
    return result

@app.post("/registry/upload-and-validate/{model_id}", response_model=Dict)
async def upload_and_validate_model(
    model_id: str = Path(...),
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
        
        # Construct versioned storage path
        nfs_path = construct_nfs_path(model_id, version)
        os.makedirs(nfs_path, exist_ok=True)
        
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
        
        # Find required and optional files in extracted structure
        required_files = ["meta.json", "app.py", "requirements.txt", "model.pth"]
        optional_files = ["webapp.py"]
        
        try:
            file_paths = find_file_in_zip_structure(extract_dir, required_files, optional_files)
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
        with open(file_paths["meta.json"], "r", encoding="utf-8") as f:
            meta_content = f.read()

        with open(file_paths["app.py"], "r", encoding="utf-8") as f:
            app_content = f.read()

        with open(file_paths["requirements.txt"], "r", encoding="utf-8") as f:
            requirements_content = f.read()
        
        # Check if webapp.py exists and read it if it does
        webapp_content = None
        if "webapp.py" in file_paths:
            with open(file_paths["webapp.py"], "r", encoding="utf-8") as f:
                webapp_content = f.read()
        
        # Run validation
        validation_result = run_validation(
            model_id=model_id,
            model_file_path=file_paths["model.pth"],
            meta_content=meta_content,
            app_content=app_content,
            requirements_content=requirements_content,
            webapp_content=webapp_content,
            user_id=user_id
        )
        
        # Only proceed with storage if validation passed
        if validation_result["is_valid"]:
            # Extract zip contents to nfs_path
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for file_info in zip_ref.infolist():
                    # Skip directories
                    if file_info.filename.endswith('/'):
                        continue
                    
                    # Extract just the filename without path
                    filename = os.path.basename(file_info.filename)
                    
                    # Skip empty filenames (can happen with some zip formats)
                    if not filename:
                        continue
                    
                    # Extract the file to the version folder with just its filename
                    source = zip_ref.open(file_info)
                    target_path = os.path.join(nfs_path, filename)
                    
                    with open(target_path, "wb") as target:
                        shutil.copyfileobj(source, target)
                    
                    logger.info(f"Extracted {filename} to {target_path}")
            
            # Store in database with model_name
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
                nfs_path,
                "validated_stored",
                json.dumps(validation_result)
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
                    "path": nfs_path
                }
            }
        else:
            # Store failed validation result with model_name
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
        error_result = {
            "model_id": model_id,
            "is_valid": False,
            "errors": [str(e)]
        }
        
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
        # Clean up temporary directory
        shutil.rmtree(temp_dir, ignore_errors=True)
        conn.close()

@app.post("/registry/upload-model/{model_id}")
async def upload_model(
    model_id: str = Path(...),
    model: UploadFile = File(...),
    user_id: str = Form(...),
    model_name: str = Form(...),  # Added model_name parameter
    metadata: str = Form(default="{}")
):
    conn = get_db()

    # 1. Check if model already exists for this user
    row = conn.execute('''
        SELECT model_id FROM models 
        WHERE model_id = ? 
        ORDER BY version DESC LIMIT 1
    ''', (model_id,)).fetchone()

    if row:
        version = get_latest_version(conn, model_id)
    else:
        version = 1

    # 2. Construct versioned storage path
    # nfs_main_path = os.path.join(NFS_BASE_DIR, model_id)
    nfs_path = construct_nfs_path(model_id, version)
    os.makedirs(nfs_path, exist_ok=True)
    # os.chmod(nfs_main_path, 0o777)
    # perms = str(oct(os.stat(nfs_path).st_mode))
    # logger.info(perms)

    # 3. Save zip file temporarily and extract
    temp_path = os.path.join(nfs_path, "temp.zip")
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)
        
        # Save the uploaded file
        with open(temp_path, "wb") as f:
            content = await model.read()
            f.write(content)
        
        # Extract the zip file
        try:
            with zipfile.ZipFile(temp_path, 'r') as zip_ref:
                zip_ref.extractall(nfs_path)
        except zipfile.BadZipFile:
            raise HTTPException(status_code=400, detail="Uploaded file is not a valid zip")
    finally:
        # Only remove if the file exists
        if os.path.exists(temp_path):
            os.remove(temp_path)

    # 4. Store metadata and model info including model_name
    conn.execute('''
        INSERT INTO models (model_id, model_name, user_id, version, timestamp, metadata, storage_path, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        model_id,
        model_name,  # Store model_name
        user_id,
        version,
        datetime.utcnow().isoformat(),
        metadata,
        nfs_path,
        "stored"
    ))
    conn.commit()
    conn.close()

    return {"message": "Model uploaded", "model_id": model_id, "model_name": model_name, "version": version}

@app.get("/registry/fetch-model/{model_id}/{version}")
def fetch_model_version(model_id: str, version: int):
    conn = get_db()
    row = conn.execute('''
        SELECT storage_path, model_name FROM models WHERE model_id = ? AND version = ?
    ''', (model_id, version)).fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Model version not found")
    return {"path": row["storage_path"], "model_name": row["model_name"], "version": version}

@app.get("/registry/fetch-model/{model_id}")
def fetch_latest_model(model_id: str):
    conn = get_db()
    row = conn.execute('''
        SELECT storage_path, model_name, version FROM models WHERE model_id = ? ORDER BY version DESC LIMIT 1
    ''', (model_id,)).fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Model not found")
    return {"path": row["storage_path"], "model_name": row["model_name"], "version": row["version"]}

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
        return result
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
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Model version not found")

    path = row["storage_path"]
    if path and os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)
    
    conn.execute('DELETE FROM models WHERE model_id = ? AND version = ?', (model_id, version))
    conn.commit()
    conn.close()
    return {"message": "Model version deleted"}

@app.delete("/registry/delete-model/{model_id}")
def delete_all_versions(model_id: str):
    conn = get_db()
    rows = conn.execute('SELECT DISTINCT storage_path FROM models WHERE model_id = ?', (model_id,)).fetchall()
    if not rows:
        conn.close()
        raise HTTPException(status_code=404, detail="Model not found")

    for row in rows:
        if row["storage_path"] and os.path.exists(row["storage_path"]):
            parent_folder = os.path.dirname(row["storage_path"])
            shutil.rmtree(parent_folder, ignore_errors=True)

    conn.execute('DELETE FROM models WHERE model_id = ?', (model_id,))
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
    uvicorn.run("model-registry:app", host='0.0.0.0', port=8000, reload=True)

if __name__ == "__main__":
    init_db()
    register_to_service_registry()
    main()