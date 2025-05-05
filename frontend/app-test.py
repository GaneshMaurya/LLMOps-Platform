from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, g
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from datetime import timedelta
from flask_wtf.csrf import CSRFProtect,  CSRFError
import sqlite3
import os
import tempfile
import shutil
import zipfile
import requests
import time
import json
from dotenv import load_dotenv

app = Flask(__name__)
app.config.update(
    SECRET_KEY='your_secret_key_here',
    WTF_CSRF_ENABLED=True,
    WTF_CSRF_SECRET_KEY='csrf-secret-key',
    PERMANENT_SESSION_LIFETIME=timedelta(days=5),
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    SESSION_COOKIE_SECURE=False   
)

ENV_PATH = "/exports/applications/.env"
load_dotenv(ENV_PATH)
SERVICE_REGISTRY_URL = f"http://{os.getenv('service_registry_ip')}:{os.getenv('service_registry_port')}/service-registry/getServiceInfo"

res_controller = requests.get(SERVICE_REGISTRY_URL, params={'name': 'controller'}).json()
CONTROLLER_IP = res_controller['result']['ip']
CONTROLLER_PORT = res_controller['result']['port']

res_model_registry = requests.get(SERVICE_REGISTRY_URL, params={'name': 'model-registry'}).json()
MODEL_REGISTRY_IP = res_model_registry['result']['ip']
MODEL_REGISTRY_PORT = res_model_registry['result']['port']

controller_url = f"http://{CONTROLLER_IP}:{CONTROLLER_PORT}/controller/deploy"
registry_url = f"http://{MODEL_REGISTRY_IP}:{MODEL_REGISTRY_PORT}/registry/display-model"

csrf = CSRFProtect(app)

models = []
uploaded_model = None

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            flash('Please login to access this page')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'role' not in session or session['role'] != 'admin':
            flash("Admin access required.")
            return redirect(url_for('home'))
        return f(*args, **kwargs)
    return decorated_function

def get_user_role(username):
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute("SELECT role FROM users WHERE username = ?", (username,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

@app.context_processor
def inject_role():
    if 'user' in session:
        role = get_user_role(session['user'])
    else:
        role = None
    return dict(role=role)

@app.before_request
def session_management():
    if request.endpoint in ['static', 'login', 'register', 'index'] or request.path.startswith('/static/'):
        return

    session.modified = True
    if 'user' in session and 'last_activity' in session:
        if time.time() - session['last_activity'] > 3600:
            session.clear()
            flash("Session expired. Please login again.")
            return redirect(url_for('login'))
    if 'user' in session:
        session['last_activity'] = time.time()

def init_db():
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user'
        )
    ''')
    conn.commit()
    conn.close()

		
@app.errorhandler(CSRFError)
def handle_csrf_error(e):
    return jsonify({'error': 'CSRF token missing or invalid'}), 400

@app.route('/')
def index():
    return redirect(url_for('login'))

@app.route('/get-registry-url', methods=['GET'])
@login_required
def get_registry_url():
    try:
        res_model_registry = requests.get(SERVICE_REGISTRY_URL, params={'name': 'model-registry'}).json()
        registry_ip = res_model_registry['result']['ip']
        registry_port = res_model_registry['result']['port']
        return jsonify({
            'url': f"http://{registry_ip}:{registry_port}/registry/upload-and-validate"
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/register', methods=['GET', 'POST'])
def register():
    if 'user' in session:
        return redirect(url_for('home'))

    if request.method == 'POST':
        username = request.form.get('username', '')
        password = request.form.get('password', '')
        role = request.form.get('role', 'user')

        if not username or not password:
            flash('Username and password are required')
            return redirect(url_for('register'))

        if len(password) < 8:
            flash('Password must be at least 8 characters long')
            return redirect(url_for('register'))

        hashed_password = generate_password_hash(password, method='pbkdf2:sha256')

        conn = sqlite3.connect('users.db')
        c = conn.cursor()
        try:
            c.execute("INSERT INTO users (username, password, role) VALUES (?, ?, ?)",
                     (username, hashed_password, role))
            conn.commit()
            flash('Registered successfully! Please login.')
            return redirect(url_for('login'))
        except sqlite3.IntegrityError:
            flash('Username already exists!')
            return redirect(url_for('register'))
        finally:
            conn.close()
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'user' in session:
        return redirect(url_for('home'))

    if request.method == 'POST':
        username = request.form.get('username', '')
        password = request.form.get('password', '')

        if not username or not password:
            flash('Username and password are required')
            return redirect(url_for('login'))

        conn = sqlite3.connect('users.db')
        c = conn.cursor()
        c.execute("SELECT * FROM users WHERE username=?", (username,))
        user = c.fetchone()
        conn.close()

        if user and check_password_hash(user[2], password):
            session.clear()
            session['user'] = username
            session['user_id'] = user[0]
            session['role'] = user[3]
            session['last_activity'] = time.time()
            session.permanent = True
            return redirect(url_for('home'))
        else:
            flash('Invalid credentials')
            return redirect(url_for('login'))

    return render_template('login.html')

@app.route('/home')
@login_required
def home():
    return render_template('home.html', username=session['user'])

@app.route('/upload', methods=['GET'])
@login_required
def upload():
    return render_template('upload.html', username=session['user'])

@app.route('/models')
@login_required
def model_list():
    try:
        conn = sqlite3.connect('../model-registry/model_registry.db')
        cursor = conn.cursor()
        
        query = """
        SELECT 
            model_name,
            version,
            model_id,
            user_id
        FROM models
        ORDER BY model_name, version DESC
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Group models by model_name
        grouped_models = {}
        for row in rows:
            model_name = row[0]
            if model_name not in grouped_models:
                grouped_models[model_name] = {
                    'versions': [],
                    'user_id': row[3]
                }
            grouped_models[model_name]['versions'].append({
                'version': row[1],
                'model_id': row[2]
            })
        
        return render_template('model_list.html', grouped_models=grouped_models)
    
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        flash('Error fetching models from registry')
        return render_template('model_list.html', grouped_models={})
    
    finally:
        if 'conn' in locals():
            conn.close()

# @app.route('/model/edit/<model_name>/<model_id>/<version>', methods=['GET', 'POST'])
# @login_required
# def edit_model(model_name, model_id, version):
#     if request.method == 'GET':
#         try:
#             response = requests.get(
#                 f"http://{MODEL_REGISTRY_IP}:{MODEL_REGISTRY_PORT}/registry/fetch-model/{model_id}/{version}",
#             )
            
#             if not response.ok:
#                 flash('Failed to fetch model files')
#                 return redirect(url_for('model_list'))
            
#             response_data = response.json()
#             model_path = response_data["path"]
            
#             # Check if the path exists
#             if not os.path.exists(model_path):
#                 flash(f'Model path not found: {model_path}')
#                 return redirect(url_for('model_list'))
            
#             # Process the files in the directory
#             editable_files = []
#             editable_extensions = ['.txt', '.json', '.py', '.yaml', '.yml', '.md', '.cfg', '.ini', '.conf']
            
#             # Walk through the directory tree
#             for root, _, files in os.walk(model_path):
#                 for file in files:
#                     full_path = os.path.join(root, file)
#                     # Get relative path for display
#                     rel_path = os.path.relpath(full_path, model_path)
                    
#                     # Check if the file has an editable extension
#                     if any(file.lower().endswith(ext) for ext in editable_extensions):
#                         try:
#                             # Read the file content
#                             with open(full_path, 'r') as f:
#                                 file_content = f.read()
                            
#                             # Add to the list of editable files
#                             editable_files.append({
#                                 'path': rel_path,
#                                 'content': file_content,
#                                 'full_path': full_path  # Store full path for writing back
#                             })
#                         except Exception as e:
#                             logging.warning(f"Could not read file {full_path}: {e}")
            
#             return render_template(
#                 'edit_model.html',
#                 model_name=model_name,
#                 model_id=model_id,
#                 version=version,
#                 files=editable_files
#             )
            
#         except Exception as e:
#             logging.error(f"Error fetching model files: {e}")
#             flash('Error accessing model files')
#             return redirect(url_for('model_list'))
            
#     elif request.method == 'POST':
#         try:
#             # Get the edited files content
#             files_content = request.json.get('files', {})
            
#             # First, get the original model path to access the files
#             response = requests.get(
#                 f"http://{MODEL_REGISTRY_IP}:{MODEL_REGISTRY_PORT}/registry/fetch-model/{model_id}/{version}",
#             )
            
#             if not response.ok:
#                 return jsonify({
#                     'success': False,
#                     'error': 'Failed to fetch original model files'
#                 })
                
#             original_model_path = response.json()["path"]
            
#             # Create a temporary directory for creating a new zip file
#             temp_dir = tempfile.mkdtemp()
#             new_version_dir = os.path.join(temp_dir, "model_files")
#             os.makedirs(new_version_dir, exist_ok=True)
            
#             # Copy all original files to the temporary directory
#             for root, _, files in os.walk(original_model_path):
#                 for file in files:
#                     src_path = os.path.join(root, file)
#                     # Get relative path
#                     rel_path = os.path.relpath(src_path, original_model_path)
#                     # Create destination path 
#                     dst_path = os.path.join(new_version_dir, rel_path)
                    
#                     # Ensure directory exists
#                     os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                    
#                     # Copy file (we'll overwrite edited files later)
#                     shutil.copy2(src_path, dst_path)
            
#             # Update the edited files in the temporary directory
#             for file_path, content in files_content.items():
#                 # Create the full path in the temp directory
#                 temp_file_path = os.path.join(new_version_dir, file_path)
                
#                 # Ensure the directory exists
#                 os.makedirs(os.path.dirname(temp_file_path), exist_ok=True)
                
#                 # Write the updated content
#                 with open(temp_file_path, 'w') as f:
#                     f.write(content)
            
#             # Create a zip file of the updated files
#             zip_path = os.path.join(temp_dir, "model.zip")
#             with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
#                 for root, _, files in os.walk(new_version_dir):
#                     for file in files:
#                         file_path = os.path.join(root, file)
#                         # Add file to zip with relative path
#                         zipf.write(
#                             file_path, 
#                             os.path.relpath(file_path, new_version_dir)
#                         )

#             # Prepare to upload the new version
#             with open(zip_path, 'rb') as zip_file:
#                 files = {
#                     'model_file': ('model.zip', zip_file, 'application/zip')
#                 }
#                 data = {
#                     'user_id': session['user'],
#                     'model_name': model_name,
#                     'metadata': '{}'  # You might want to pass additional metadata here
#                 }
                
#                 # Upload to registry
#                 upload_response = requests.post(
#                     f"http://{MODEL_REGISTRY_IP}:{MODEL_REGISTRY_PORT}/registry/upload-and-validate/{model_id}",
#                     files=files,
#                     data=data
#                 )
            
#             # Clean up temporary directory
#             shutil.rmtree(temp_dir, ignore_errors=True)
            
#             if not upload_response.ok:
#                 return jsonify({
#                     'success': False,
#                     'error': f'Failed to upload new version: {upload_response.text}'
#                 })
            
#             result = upload_response.json()
#             new_version = result['storage']['version']
            
#             return jsonify({
#                 'success': True,
#                 'new_version': new_version
#             })
            
#         except Exception as e:
#             logging.error(f"Error saving model changes: {e}")
#             return jsonify({
#                 'success': False,
#                 'error': str(e)
#             })

@app.route('/model/edit/<model_name>/<model_id>/<version>', methods=['GET', 'POST'])
@login_required
def edit_model(model_name, model_id, version):
    # Check if user has permission to edit this model
    try:
        conn = sqlite3.connect('../model-registry/model_registry.db')
        cursor = conn.cursor()
        cursor.execute("""
            SELECT user_id FROM models 
            WHERE model_id = ? AND version = ?
        """, (model_id, version))
        result = cursor.fetchone()
        
        if not result or result[0] != session['user']:
            flash('You do not have permission to edit this model')
            return redirect(url_for('model_list'))
    except sqlite3.Error as e:
        logging.error(f"Database error checking permissions: {e}")
        flash('Error checking edit permissions')
        return redirect(url_for('model_list'))
    finally:
        if 'conn' in locals():
            conn.close()

    if request.method == 'GET':
        try:
            # Fetch model files from registry
            response = requests.get(
                f"http://{MODEL_REGISTRY_IP}:{MODEL_REGISTRY_PORT}/registry/fetch-model/{model_id}/{version}",
                timeout=10
            )
            
            if not response.ok:
                flash('Failed to fetch model files')
                return redirect(url_for('model_list'))
            
            response_data = response.json()
            model_path = response_data["path"]
            
            if not os.path.exists(model_path):
                flash(f'Model path not found: {model_path}')
                return redirect(url_for('model_list'))
            
            editable_files = []
            editable_extensions = ['.txt', '.json', '.py', '.yaml', '.yml', '.md', '.cfg', '.ini', '.conf']
            
            for root, _, files in os.walk(model_path):
                for file in files:
                    full_path = os.path.join(root, file)
                    rel_path = os.path.relpath(full_path, model_path)
                    
                    if any(file.lower().endswith(ext) for ext in editable_extensions):
                        try:
                            with open(full_path, 'r') as f:
                                file_content = f.read()
                            
                            editable_files.append({
                                'path': rel_path,
                                'content': file_content,
                                'full_path': full_path
                            })
                        except Exception as e:
                            logging.warning(f"Could not read file {full_path}: {e}")
            
            return render_template(
                'edit_model.html',
                model_name=model_name,
                model_id=model_id,
                version=version,
                files=editable_files
            )
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Registry service error: {e}")
            flash('Error connecting to model registry')
            return redirect(url_for('model_list'))
        except Exception as e:
            logging.error(f"Error fetching model files: {e}")
            flash('Error accessing model files')
            return redirect(url_for('model_list'))
            
    elif request.method == 'POST':
        try:
            files_content = request.json.get('files', {})
            if not files_content:
                return jsonify({
                    'success': False,
                    'error': 'No file content provided'
                }), 400
            
            # Fetch original model files
            response = requests.get(
                f"http://{MODEL_REGISTRY_IP}:{MODEL_REGISTRY_PORT}/registry/fetch-model/{model_id}/{version}",
                timeout=10
            )
            
            if not response.ok:
                return jsonify({
                    'success': False,
                    'error': 'Failed to fetch original model files'
                }), 500
                
            original_model_path = response.json()["path"]
            
            # Create temporary directory for new version
            temp_dir = tempfile.mkdtemp()
            new_version_dir = os.path.join(temp_dir, "model_files")
            os.makedirs(new_version_dir, exist_ok=True)
            
            # Copy original files
            try:
                for root, _, files in os.walk(original_model_path):
                    for file in files:
                        src_path = os.path.join(root, file)
                        rel_path = os.path.relpath(src_path, original_model_path)
                        dst_path = os.path.join(new_version_dir, rel_path)
                        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                        shutil.copy2(src_path, dst_path)
            except (OSError, shutil.Error) as e:
                logging.error(f"Error copying files: {e}")
                return jsonify({
                    'success': False,
                    'error': 'Failed to prepare model files'
                }), 500
            
            # Update edited files
            for file_path, content in files_content.items():
                temp_file_path = os.path.join(new_version_dir, file_path)
                os.makedirs(os.path.dirname(temp_file_path), exist_ok=True)
                try:
                    with open(temp_file_path, 'w') as f:
                        f.write(content)
                except IOError as e:
                    logging.error(f"Error writing file {file_path}: {e}")
                    return jsonify({
                        'success': False,
                        'error': f'Failed to write file: {file_path}'
                    }), 500
            
            # Create zip file
            zip_path = os.path.join(temp_dir, "model.zip")
            try:
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for root, _, files in os.walk(new_version_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arc_path = os.path.relpath(file_path, new_version_dir)
                            zipf.write(file_path, arc_path)
            except zipfile.BadZipFile as e:
                logging.error(f"Error creating zip file: {e}")
                return jsonify({
                    'success': False,
                    'error': 'Failed to create model package'
                }), 500

            # Upload new version
            try:
                with open(zip_path, 'rb') as zip_file:
                    files = {
                        'model_file': ('model.zip', zip_file, 'application/zip')
                    }
                    data = {
                        'user_id': session['user'],
                        'model_name': model_name,
                        'metadata': json.dumps({
                            'edited_from_version': version,
                            'edited_by': session['user']
                        })
                    }
                    
                    upload_response = requests.post(
                        f"http://{MODEL_REGISTRY_IP}:{MODEL_REGISTRY_PORT}/registry/upload-and-validate/{model_id}",
                        files=files,
                        data=data,
                        timeout=30
                    )
            except requests.exceptions.RequestException as e:
                logging.error(f"Upload error: {e}")
                return jsonify({
                    'success': False,
                    'error': 'Failed to upload new version'
                }), 500
            finally:
                # Clean up temporary files
                shutil.rmtree(temp_dir, ignore_errors=True)
            
            if not upload_response.ok:
                return jsonify({
                    'success': False,
                    'error': f'Registry error: {upload_response.text}'
                }), upload_response.status_code
            
            result = upload_response.json()
            new_version = result['storage']['version']
            
            return jsonify({
                'success': True,
                'new_version': new_version,
                'message': f'Successfully created version {new_version}'
            })
            
        except Exception as e:
            logging.error(f"Error saving model changes: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

@app.route('/deploy/<model_name>/<model_id>/<model_version>', methods=['GET'])
@login_required
def deploy(model_name, model_id, model_version):
    return render_template('deploy.html',
                           model_name=model_name,
                           model_id=model_id,
                           model_version=model_version,
                           controller_URL=controller_url)

@app.route('/admin/deployments')
@login_required
@admin_required
def admin_deployments():
    # Dummy data
    deployments = [
        {'model_name': 'Model A', 'model_id': 'abc123', 'version': '1.0'},
        {'model_name': 'Model B', 'model_id': 'def456', 'version': '2.0'}
    ]
    return render_template('admin_deployments.html', deployments=deployments)

@app.route('/admin/kill/<model_id>', methods=['POST'])
@login_required
@admin_required
def kill_model(model_id):
    print(f"Kill request received for model {model_id}")
    return jsonify({'message': f'Model {model_id} terminated.'})

@app.route('/logout', methods=['POST'])
def logout():
    session.clear()
    flash('You have been logged out successfully')
    return redirect(url_for('login'))

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
    return render_template('500.html'), 500

if __name__ == '__main__':
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    debug_mode = os.environ.get('FLASK_ENV') == 'development'
    port = int(os.environ.get('FLASK_PORT', 5000))
    
    try:
        init_db()
        logging.info("Database initialized successfully")
        
        @app.after_request
        def add_security_headers(response):
            response.headers['X-Content-Type-Options'] = 'nosniff'
            response.headers['X-Frame-Options'] = 'SAMEORIGIN'
            response.headers['X-XSS-Protection'] = '1; mode=block'
            return response
        
        app.run(
            debug=debug_mode,
            host='0.0.0.0',
            port=port,
            use_reloader=debug_mode
        )
        
    except sqlite3.Error as db_error:
        logging.error(f"Database error: {db_error}")
        exit(1)
    except Exception as e:
        logging.error(f"Application failed to start: {e}")
        exit(1)