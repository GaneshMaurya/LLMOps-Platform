from flask import Flask, render_template, request, redirect, url_for, session, flash
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from datetime import timedelta
from flask_wtf.csrf import CSRFProtect
import sqlite3
import os
import requests
import time

app = Flask(__name__)
app.secret_key = 'your_secret_key'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=5)
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_SECURE'] = True

controller_url = 'http://192.168.141.61:8090/controller/deploy'

csrf = CSRFProtect(app)

models = []
uploaded_model = None

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            flash('Please login to access this page')
            return redirect(url_for('login'))
        # Optional: Additional security checks can be added here
        return f(*args, **kwargs)
    return decorated_function

@app.before_request
def session_management():
    # Skip for static files and public routes
    if request.endpoint in ['static', 'login', 'register', 'index'] or request.path.startswith('/static/'):
        return
    
    session.modified = True
    # Optional session timeout check
    if 'user' in session and 'last_activity' in session:
        if time.time() - session['last_activity'] > 3600:  # 1 hour
            session.clear()
            flash("Session expired. Please login again.")
            return redirect(url_for('login'))
    
    # Update last activity timestamp
    if 'user' in session:
        session['last_activity'] = time.time()

def init_db():
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

@app.route('/')
def index():
    return redirect(url_for('login'))

@app.route('/register', methods=['GET', 'POST'])
def register():
    # Redirect if already logged in
    if 'user' in session:
        return redirect(url_for('home'))
        
    if request.method == 'POST':
        username = request.form.get('username', '')
        password = request.form.get('password', '')
        
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
            c.execute("INSERT INTO users (username, password) VALUES (?, ?)", 
                     (username, hashed_password))
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
    # Redirect if already logged in
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
            session.clear()  # Clear any existing session
            session['user'] = username
            session['user_id'] = user[0]
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
    username = session['user']
    return render_template('upload.html', username=username)

@app.route('/models')
@login_required
def model_list():
    try:
        registry_host = "192.168.141.124"
        registry_port = "8000"
        
        registry_url = f"http://{registry_host}:{registry_port}/registry/display-model"
        response = requests.get(registry_url)
        
        if response.status_code == 200:
            models_data = response.json()
            models_info = []
            for model in models_data:
                model_info = {
                    'model_id': model.get('model_id'),
                    'model_name': model.get('model_name'),
                    'version': model.get('version'),
                    'user_id': model.get('user_id'),
                    'timestamp': model.get('timestamp')
                }
                models_info.append(model_info)
            print(models_info)
            return render_template('model_list.html', models=models_info)
        else:
            flash(f'Error fetching models: {response.status_code} - {response.text}')
            return render_template('model_list.html', models=[])
    
    except Exception as e:
        flash(f'Error connecting to registry: {str(e)}')
        return render_template('model_list.html', models=[])

@app.route('/deploy/<model_name>/<model_id>/<model_version>', methods=['GET'])
@login_required
def deploy(model_name, model_id, model_version):
    print(model_name, model_id, model_version)
    return render_template(
        'deploy.html',
        model_name=model_name,
        model_id=model_id,
        model_version=model_version,
        controller_URL=controller_url  # make sure this is defined before
    )


@app.route('/logout', methods=['POST'])
def logout():
    session.clear()
    flash('You have been logged out successfully')
    return redirect(url_for('login'))

# Error handlers
@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
    return render_template('500.html'), 500

if __name__ == '__main__':
    init_db()
    # Set debug=False in production
    app.run(debug=True)