from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, g
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from datetime import timedelta
from flask_wtf.csrf import CSRFProtect,  CSRFError
import sqlite3
import os
import requests
import time
import json
from dotenv import load_dotenv

app = Flask(__name__)
app.secret_key = 'your_secret_key'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=5)
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_SECURE'] = True

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

# @app.route('/models')
# @login_required
# def model_list():
#     try:
#         response = requests.get(registry_url)

#         if response.status_code == 200:
#             models_data = response.json()
#             models_info = [{
#                 'model_id': m.get('model_id'),
#                 'model_name': m.get('model_name'),
#                 'version': m.get('version'),
#                 'user_id': m.get('user_id'),
#                 'timestamp': m.get('timestamp')
#             } for m in models_data]
#             return render_template('model_list.html', models=models_info)
#         else:
#             flash(f'Error fetching models: {response.status_code}')
#             return render_template('model_list.html', models=[])

#     except Exception as e:
#         flash(f'Error connecting to registry: {str(e)}')
#         return render_template('model_list.html', models=[])

@app.route('/models')
@login_required
def model_list():
    try:
        conn = sqlite3.connect('model_registry.db')
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
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=True)
