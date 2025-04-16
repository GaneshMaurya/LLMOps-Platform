import os
import uuid
import shutil
import zipfile
import sqlite3
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Path
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List
import uvicorn
from config import DB_PATH, NFS_BASE_DIR

app = FastAPI(title="Model Registry", version="1.0")

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
            user_id TEXT,
            version INTEGER,
            timestamp TEXT,
            metadata TEXT,
            storage_path TEXT,
            status TEXT
        )
    ''')
    conn.commit()
    conn.close()

init_db()

class ModelMetadata(BaseModel):
    model_id: str
    model_name: str
    user_id: str
    version: int
    timestamp: str
    metadata: str
    storage_path: str
    status: str

def construct_nfs_path(model_id: str, version: int):
    return os.path.join(NFS_BASE_DIR, model_id, f"v{version}")

def get_latest_version(conn, model_id: str):
    cur = conn.execute('SELECT MAX(version) FROM models WHERE model_id = ?', (model_id,))
    row = cur.fetchone()
    return (row[0] or 0) + 1

@app.post("/registry/upload-model/{model_id}")
def upload_model(
    model_id: str = Path(...),
    model: UploadFile = File(...),
    user_id: str = Form(...),
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
    nfs_path = construct_nfs_path(model_id, version)
    os.makedirs(nfs_path, exist_ok=True)

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

    # 4. Store metadata and model info
    conn.execute('''
        INSERT INTO models (model_id, user_id, version, timestamp, metadata, storage_path, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        model_id,
        user_id,
        version,
        datetime.utcnow().isoformat(),
        metadata,
        nfs_path,
        "stored"
    ))
    conn.commit()
    conn.close()

    return {"message": "Model uploaded", "model_id": model_id, "version": version}

@app.get("/registry/fetch-model/{model_id}/{version}")
def fetch_model_version(model_id: str, version: int):
    conn = get_db()
    row = conn.execute('''
        SELECT storage_path FROM models WHERE model_id = ? AND version = ?
    ''', (model_id, version)).fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Model version not found")
    return {"path": row["storage_path"]}


@app.get("/registry/fetch-model/{model_id}")
def fetch_latest_model(model_id: str):
    conn = get_db()
    row = conn.execute('''
        SELECT storage_path FROM models WHERE model_id = ? ORDER BY version DESC LIMIT 1
    ''', (model_id,)).fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Model not found")
    return {"path": row["storage_path"]}


@app.get("/registry/display-model/{user_id}", response_model=List[ModelMetadata])
def display_user_models(user_id: str):
    conn = get_db()
    rows = conn.execute('''
        SELECT * FROM models WHERE user_id = ?
    ''', (user_id,)).fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get("/registry/display-model", response_model=List[ModelMetadata])
def display_user_models(user_id: str):
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
    shutil.rmtree(path, ignore_errors=True)
    conn.execute('DELETE FROM models WHERE model_id = ? AND version = ?', (model_id, version))
    conn.commit()
    conn.close()
    return {"message": "Model version deleted"}


@app.delete("/registry/delete-model/{model_id}")
def delete_all_versions(model_id: str):
    conn = get_db()
    rows = conn.execute('SELECT DISTINCT storage_path FROM models WHERE model_id = ?', (model_id,)).fetchall()
    print([dict(row) for row in rows])
    if not rows:
        conn.close()
        raise HTTPException(status_code=404, detail="Model not found")

    for row in rows:
        parent_folder = os.path.dirname(row["storage_path"])
        shutil.rmtree(parent_folder, ignore_errors=True)

    conn.execute('DELETE FROM models WHERE model_id = ?', (model_id,))
    conn.commit()
    conn.close()
    return {"message": "All versions deleted"}

def main():
    uvicorn.run("model-registry:app", port=8000, reload=True)

if __name__ == "__main__":
    main()