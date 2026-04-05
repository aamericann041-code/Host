import telebot
import subprocess
import os
import zipfile
import tempfile
import shutil
from telebot import types
import time
from datetime import datetime, timedelta
# Removed unused telegram.* imports as we are using telebot consistently
# from telegram import Update
# from telegram.ext import Updater, CommandHandler, CallbackContext
import psutil
import sqlite3
import json # Kept in case needed elsewhere, but not used in provided logic
import logging # Kept in case needed elsewhere
import signal # Kept in case needed elsewhere
import threading
import re # Added for regex matching in auto-install
import sys # Added for sys.executable
import atexit
import requests # For polling exceptions

# --- Flask Keep Alive + HTML Hosting ---
from flask import Flask, send_from_directory, abort
from threading import Thread

app = Flask('')

# HTML websites storage: {user_id: {'folder': path, 'entry': filename}}
hosted_websites = {}
# Node.js port tracking: {user_id: port}
node_ports = {}
NODE_PORT_START = 4000

def get_local_ip():
    """Get device local IP address"""
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "127.0.0.1"

def assign_node_port(user_id):
    """Assign a unique port for a Node.js app"""
    used = set(node_ports.values())
    port = NODE_PORT_START
    while port in used:
        port += 1
    node_ports[user_id] = port
    return port

@app.route('/')
def home():
    return "🦇 Batman File Host - يعمل!"

@app.route('/site/<int:user_id>/')
@app.route('/site/<int:user_id>/<path:filename>')
def serve_site(user_id, filename=None):
    """Serve hosted HTML website for a user"""
    if user_id not in hosted_websites:
        abort(404)
    site_info = hosted_websites[user_id]
    folder = site_info['folder']
    if filename is None:
        filename = site_info.get('entry', 'index.html')
    try:
        return send_from_directory(folder, filename)
    except Exception:
        abort(404)

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, use_reloader=False)

def keep_alive():
    t = Thread(target=run_flask)
    t.daemon = True
    t.start()
    print("Flask Keep-Alive server started.")
# --- End Flask Keep Alive ---

# --- Configuration ---
TOKEN = '8553199423:AAH4NVlWl89ePPFr5XtOzage0WtRTLJEtrw' # توكن البوت
OWNER_ID = 7970883512 # ID المالك
ADMIN_ID = 8206539702 # ID الأدمن
YOUR_USERNAME = '@I_tt_6' # يوزر المالك
UPDATE_CHANNEL = 'https://t.me/ul2fg' # قناة التحديثات

# Folder setup - using absolute paths
BASE_DIR = os.path.abspath(os.path.dirname(__file__)) # Get script's directory
UPLOAD_BOTS_DIR = os.path.join(BASE_DIR, 'upload_bots')
IROTECH_DIR = os.path.join(BASE_DIR, 'inf') # Assuming this name is intentional
DATABASE_PATH = os.path.join(IROTECH_DIR, 'bot_data.db')
WEBSITES_DIR = os.path.join(BASE_DIR, 'websites') # HTML websites hosting

# File upload limits
FREE_USER_LIMIT = 3
SUBSCRIBED_USER_LIMIT = 15 # Changed from 10 to 15
ADMIN_LIMIT = 999       # Changed from 50 to 999
OWNER_LIMIT = float('inf') # Changed from 999 to infinity
# FREE_MODE_LIMIT = 3 # Removed as free_mode is removed

# Create necessary directories
os.makedirs(UPLOAD_BOTS_DIR, exist_ok=True)
os.makedirs(IROTECH_DIR, exist_ok=True)
os.makedirs(WEBSITES_DIR, exist_ok=True)

# Initialize bot
bot = telebot.TeleBot(TOKEN)

# --- Data structures ---
bot_scripts = {} # Stores info about running scripts {script_key: info_dict}
user_subscriptions = {} # {user_id: {'expiry': datetime_object}}
user_files = {} # {user_id: [(file_name, file_type), ...]}
active_users = set() # Set of all user IDs that have interacted with the bot
admin_ids = {ADMIN_ID, OWNER_ID} # Set of admin IDs
banned_users = set() # Set of banned user IDs
bot_locked = False
# free_mode = False # Removed free_mode

# --- Logging Setup ---
# Configure basic logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Command Button Layouts (ReplyKeyboardMarkup) ---
COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📢 قناة التحديثات"],
    ["📤 رفع ملف", "📂 فحص الملفات"],
    ["🌐 مواقعي", "⚡ سرعة البوت"],
    ["📊 الإحصائيات", "📞 التواصل مع المالك"]
]
ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📢 قناة التحديثات"],
    ["📤 رفع ملف", "📂 فحص الملفات"],
    ["🌐 مواقعي", "⚡ سرعة البوت"],
    ["📊 الإحصائيات", "💳 الاشتراكات"],
    ["📢 بث رسالة", "🔒 قفل البوت"],
    ["🟢 تشغيل كل الأكواد", "👑 لوحة الأدمن"],
    ["📞 التواصل مع المالك"]
]

# --- Database Setup ---
def init_db():
    """Initialize the database with required tables"""
    logger.info(f"Initializing database at: {DATABASE_PATH}")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False) # Allow access from multiple threads
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS subscriptions
                     (user_id INTEGER PRIMARY KEY, expiry TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS user_files
                     (user_id INTEGER, file_name TEXT, file_type TEXT,
                      PRIMARY KEY (user_id, file_name))''')
        c.execute('''CREATE TABLE IF NOT EXISTS active_users
                     (user_id INTEGER PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS admins
                     (user_id INTEGER PRIMARY KEY)''') # Added admins table
        c.execute('''CREATE TABLE IF NOT EXISTS banned_users
                     (user_id INTEGER PRIMARY KEY)''')
        # Ensure owner and initial admin are in admins table
        c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (OWNER_ID,))
        if ADMIN_ID != OWNER_ID:
             c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (ADMIN_ID,))
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"❌ Database initialization error: {e}", exc_info=True)

def load_data():
    """Load data from database into memory"""
    logger.info("Loading data from database...")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()

        # Load subscriptions
        c.execute('SELECT user_id, expiry FROM subscriptions')
        for user_id, expiry in c.fetchall():
            try:
                user_subscriptions[user_id] = {'expiry': datetime.fromisoformat(expiry)}
            except ValueError:
                logger.warning(f"⚠️ Invalid expiry date format for user {user_id}: {expiry}. Skipping.")

        # Load user files
        c.execute('SELECT user_id, file_name, file_type FROM user_files')
        for user_id, file_name, file_type in c.fetchall():
            if user_id not in user_files:
                user_files[user_id] = []
            user_files[user_id].append((file_name, file_type))

        # Load active users
        c.execute('SELECT user_id FROM active_users')
        active_users.update(user_id for (user_id,) in c.fetchall())

        # Load admins
        c.execute('SELECT user_id FROM admins')
        admin_ids.update(user_id for (user_id,) in c.fetchall()) # Load admins into the set

        # Load banned users
        c.execute('SELECT user_id FROM banned_users')
        banned_users.update(user_id for (user_id,) in c.fetchall())

        conn.close()
        logger.info(f"Data loaded: {len(active_users)} users, {len(user_subscriptions)} subscriptions, {len(admin_ids)} admins.")
    except Exception as e:
        logger.error(f"❌ Error loading data: {e}", exc_info=True)

# Initialize DB and Load Data at startup
init_db()
load_data()
# --- End Database Setup ---

# --- Helper Functions ---
def get_user_folder(user_id):
    """Get or create user's folder for storing files"""
    user_folder = os.path.join(UPLOAD_BOTS_DIR, str(user_id))
    os.makedirs(user_folder, exist_ok=True)
    return user_folder

def get_user_file_limit(user_id):
    """Get the file upload limit for a user"""
    # if free_mode: return FREE_MODE_LIMIT # Removed free_mode check
    if user_id == OWNER_ID: return OWNER_LIMIT
    if user_id in admin_ids: return ADMIN_LIMIT
    if user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now():
        return SUBSCRIBED_USER_LIMIT
    return FREE_USER_LIMIT

def get_user_file_count(user_id):
    """Get the number of files uploaded by a user"""
    return len(user_files.get(user_id, []))

def is_bot_running(script_owner_id, file_name): # Parameter renamed for clarity
    """Check if a bot script is currently running for a specific user"""
    script_key = f"{script_owner_id}_{file_name}" # Key uses script_owner_id
    script_info = bot_scripts.get(script_key)
    if script_info and script_info.get('process'):
        try:
            proc = psutil.Process(script_info['process'].pid)
            is_running = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
            if not is_running:
                logger.warning(f"Process {script_info['process'].pid} for {script_key} found in memory but not running/zombie. Cleaning up.")
                if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
                    try:
                        script_info['log_file'].close()
                    except Exception as log_e:
                        logger.error(f"Error closing log file during zombie cleanup {script_key}: {log_e}")
                if script_key in bot_scripts:
                    del bot_scripts[script_key]
            return is_running
        except psutil.NoSuchProcess:
            logger.warning(f"Process for {script_key} not found (NoSuchProcess). Cleaning up.")
            if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
                try:
                     script_info['log_file'].close()
                except Exception as log_e:
                     logger.error(f"Error closing log file during cleanup of non-existent process {script_key}: {log_e}")
            if script_key in bot_scripts:
                 del bot_scripts[script_key]
            return False
        except Exception as e:
            logger.error(f"Error checking process status for {script_key}: {e}", exc_info=True)
            return False
    return False


def kill_process_tree(process_info):
    """Kill a process and all its children, ensuring log file is closed."""
    pid = None
    log_file_closed = False
    script_key = process_info.get('script_key', 'N/A') 

    try:
        if 'log_file' in process_info and hasattr(process_info['log_file'], 'close') and not process_info['log_file'].closed:
            try:
                process_info['log_file'].close()
                log_file_closed = True
                logger.info(f"Closed log file for {script_key} (PID: {process_info.get('process', {}).get('pid', 'N/A')})")
            except Exception as log_e:
                logger.error(f"Error closing log file during kill for {script_key}: {log_e}")

        process = process_info.get('process')
        if process and hasattr(process, 'pid'):
           pid = process.pid
           if pid: 
                try:
                    parent = psutil.Process(pid)
                    children = parent.children(recursive=True)
                    logger.info(f"Attempting to kill process tree for {script_key} (PID: {pid}, Children: {[c.pid for c in children]})")

                    for child in children:
                        try:
                            child.terminate()
                            logger.info(f"Terminated child process {child.pid} for {script_key}")
                        except psutil.NoSuchProcess:
                            logger.warning(f"Child process {child.pid} for {script_key} already gone.")
                        except Exception as e:
                            logger.error(f"Error terminating child {child.pid} for {script_key}: {e}. Trying kill...")
                            try: child.kill(); logger.info(f"Killed child process {child.pid} for {script_key}")
                            except Exception as e2: logger.error(f"Failed to kill child {child.pid} for {script_key}: {e2}")

                    gone, alive = psutil.wait_procs(children, timeout=1)
                    for p in alive:
                        logger.warning(f"Child process {p.pid} for {script_key} still alive. Killing.")
                        try: p.kill()
                        except Exception as e: logger.error(f"Failed to kill child {p.pid} for {script_key} after wait: {e}")

                    try:
                        parent.terminate()
                        logger.info(f"Terminated parent process {pid} for {script_key}")
                        try: parent.wait(timeout=1)
                        except psutil.TimeoutExpired:
                            logger.warning(f"Parent process {pid} for {script_key} did not terminate. Killing.")
                            parent.kill()
                            logger.info(f"Killed parent process {pid} for {script_key}")
                    except psutil.NoSuchProcess:
                        logger.warning(f"Parent process {pid} for {script_key} already gone.")
                    except Exception as e:
                        logger.error(f"Error terminating parent {pid} for {script_key}: {e}. Trying kill...")
                        try: parent.kill(); logger.info(f"Killed parent process {pid} for {script_key}")
                        except Exception as e2: logger.error(f"Failed to kill parent {pid} for {script_key}: {e2}")

                except psutil.NoSuchProcess:
                    logger.warning(f"Process {pid or 'N/A'} for {script_key} not found during kill. Already terminated?")
           else: logger.error(f"Process PID is None for {script_key}.")
        elif log_file_closed: logger.warning(f"Process object missing for {script_key}, but log file closed.")
        else: logger.error(f"Process object missing for {script_key}, and no log file. Cannot kill.")
    except Exception as e:
        logger.error(f"❌ Unexpected error killing process tree for PID {pid or 'N/A'} ({script_key}): {e}", exc_info=True)

# --- Automatic Package Installation & Script Running ---

def attempt_install_pip(module_name, message):
    package_name = TELEGRAM_MODULES.get(module_name.lower(), module_name) 
    if package_name is None: 
        logger.info(f"Module '{module_name}' is core. Skipping pip install.")
        return False 
    try:
        bot.reply_to(message, f"🐍 الوحدة `{module_name}` غير موجودة. جاري تثبيت `{package_name}`...", parse_mode='Markdown')
        command = [sys.executable, '-m', 'pip', 'install', package_name]
        logger.info(f"Running install: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, encoding='utf-8', errors='ignore')
        if result.returncode == 0:
            logger.info(f"Installed {package_name}. Output:\n{result.stdout}")
            bot.reply_to(message, f"✅ تم تثبيت الحزمة `{package_name}` (للوحدة `{module_name}`).", parse_mode='Markdown')
            return True
        else:
            error_msg = f"❌ فشل في تثبيت `{package_name}` للوحدة `{module_name}`.\nالسجل:\n```\n{result.stderr or result.stdout}\n```"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (السجل مقتطع)"
            bot.reply_to(message, error_msg, parse_mode='Markdown')
            return False
    except Exception as e:
        error_msg = f"❌ خطأ في تثبيت `{package_name}`: {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message, error_msg)
        return False

def attempt_install_npm(module_name, user_folder, message):
    try:
        bot.reply_to(message, f"🟠 حزمة Node `{module_name}` غير موجودة. جاري التثبيت محلياً...", parse_mode='Markdown')
        command = ['npm', 'install', module_name]
        logger.info(f"Running npm install: {' '.join(command)} in {user_folder}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, cwd=user_folder, encoding='utf-8', errors='ignore')
        if result.returncode == 0:
            logger.info(f"Installed {module_name}. Output:\n{result.stdout}")
            bot.reply_to(message, f"✅ تم تثبيت حزمة Node `{module_name}` محلياً.", parse_mode='Markdown')
            return True
        else:
            error_msg = f"❌ فشل في تثبيت حزمة Node `{module_name}`.\nالسجل:\n```\n{result.stderr or result.stdout}\n```"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (السجل مقتطع)"
            bot.reply_to(message, error_msg, parse_mode='Markdown')
            return False
    except FileNotFoundError:
         error_msg = "❌ خطأ: 'npm' غير موجود. تأكد من تثبيت Node.js/npm ووجوده في PATH."
         logger.error(error_msg)
         bot.reply_to(message, error_msg)
         return False
    except Exception as e:
        error_msg = f"❌ خطأ في تثبيت حزمة Node `{module_name}`: {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message, error_msg)
        return False

def run_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    """Run Python script. script_owner_id is used for the script_key. message_obj_for_reply is for sending feedback."""
    max_attempts = 2 
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ فشل في تشغيل '{file_name}' بعد {max_attempts} محاولات. تحقق من السجلات.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Attempt {attempt} to run Python script: {script_path} (Key: {script_key}) for user {script_owner_id}")

    try:
        if not os.path.exists(script_path):
             bot.reply_to(message_obj_for_reply, f"❌ خطأ: النص '{file_name}' غير موجود في '{script_path}'!")
             logger.error(f"Script not found: {script_path} for user {script_owner_id}")
             if script_owner_id in user_files:
                 user_files[script_owner_id] = [f for f in user_files.get(script_owner_id, []) if f[0] != file_name]
             remove_user_file_db(script_owner_id, file_name)
             return

        if attempt == 1:
            check_command = [sys.executable, script_path]
            logger.info(f"Running Python pre-check: {' '.join(check_command)}")
            check_proc = None
            try:
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
                stdout, stderr = check_proc.communicate(timeout=5)
                return_code = check_proc.returncode
                logger.info(f"Python Pre-check early. RC: {return_code}. Stderr: {stderr[:200]}...")
                if return_code != 0 and stderr:
                    match_py = re.search(r"ModuleNotFoundError: No module named '(.+?)'", stderr)
                    if match_py:
                        module_name = match_py.group(1).strip().strip("'\"")
                        logger.info(f"Detected missing Python module: {module_name}")
                        if attempt_install_pip(module_name, message_obj_for_reply):
                            logger.info(f"Install OK for {module_name}. Retrying run_script...")
                            bot.reply_to(message_obj_for_reply, f"🔄 نجح التثبيت. جاري إعادة محاولة '{file_name}'...")
                            time.sleep(2)
                            threading.Thread(target=run_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                            return
                        else:
                            bot.reply_to(message_obj_for_reply, f"❌ فشل التثبيت. لا يمكن تشغيل '{file_name}'.")
                            return
                    else:
                         error_summary = stderr[:500]
                         bot.reply_to(message_obj_for_reply, f"❌ خطأ في فحص النص المسبق لـ '{file_name}':\n```\n{error_summary}\n```\nقم بإصلاح النص.", parse_mode='Markdown')
                         return
            except subprocess.TimeoutExpired:
                logger.info("Python Pre-check timed out (>5s), imports likely OK. Killing check process.")
                if check_proc and check_proc.poll() is None: check_proc.kill(); check_proc.communicate()
                logger.info("Python Check process killed. Proceeding to long run.")
            except FileNotFoundError:
                 logger.error(f"Python interpreter not found: {sys.executable}")
                 bot.reply_to(message_obj_for_reply, f"❌ خطأ: مفسر Python '{sys.executable}' غير موجود.")
                 return
            except Exception as e:
                 logger.error(f"Error in Python pre-check for {script_key}: {e}", exc_info=True)
                 bot.reply_to(message_obj_for_reply, f"❌ خطأ غير متوقع في فحص النص المسبق لـ '{file_name}': {e}")
                 return
            finally:
                 if check_proc and check_proc.poll() is None:
                     logger.warning(f"Python Check process {check_proc.pid} still running. Killing.")
                     check_proc.kill(); check_proc.communicate()

        logger.info(f"Starting long-running Python process for {script_key}")
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = None; process = None
        try: log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        except Exception as e:
             logger.error(f"Failed to open log file '{log_file_path}' for {script_key}: {e}", exc_info=True)
             bot.reply_to(message_obj_for_reply, f"❌ فشل في فتح ملف السجل '{log_file_path}': {e}")
             return
        try:
            startupinfo = None; creationflags = 0
            if os.name == 'nt':
                 startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                 startupinfo.wShowWindow = subprocess.SW_HIDE
            process = subprocess.Popen(
                [sys.executable, script_path], cwd=user_folder, stdout=log_file, stderr=log_file,
                stdin=subprocess.PIPE, startupinfo=startupinfo, creationflags=creationflags,
                encoding='utf-8', errors='ignore'
            )
            logger.info(f"Started Python process {process.pid} for {script_key}")
            bot_scripts[script_key] = {
                'process': process, 'log_file': log_file, 'file_name': file_name,
                'chat_id': message_obj_for_reply.chat.id, # Chat ID for potential future direct replies from script, defaults to admin/triggering user
                'script_owner_id': script_owner_id, # Actual owner of the script
                'start_time': datetime.now(), 'user_folder': user_folder, 'type': 'py', 'script_key': script_key
            }
            bot.reply_to(message_obj_for_reply, f"✅ تم تشغيل نص Python '{file_name}'! (PID: {process.pid}) (للمستخدم: {script_owner_id})")
        except FileNotFoundError:
             logger.error(f"Python interpreter {sys.executable} not found for long run {script_key}")
             bot.reply_to(message_obj_for_reply, f"❌ خطأ: مفسر Python '{sys.executable}' غير موجود.")
             if log_file and not log_file.closed: log_file.close()
             if script_key in bot_scripts: del bot_scripts[script_key]
        except Exception as e:
            if log_file and not log_file.closed: log_file.close()
            error_msg = f"❌ خطأ في بدء نص Python '{file_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            bot.reply_to(message_obj_for_reply, error_msg)
            if process and process.poll() is None:
                 logger.warning(f"Killing potentially started Python process {process.pid} for {script_key}")
                 kill_process_tree({'process': process, 'log_file': log_file, 'script_key': script_key})
            if script_key in bot_scripts: del bot_scripts[script_key]
    except Exception as e:
        error_msg = f"❌ خطأ غير متوقع في تشغيل نص Python '{file_name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message_obj_for_reply, error_msg)
        if script_key in bot_scripts:
             logger.warning(f"Cleaning up {script_key} due to error in run_script.")
             kill_process_tree(bot_scripts[script_key])
             del bot_scripts[script_key]

def run_js_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    """Run JS script. script_owner_id is used for the script_key. message_obj_for_reply is for sending feedback."""
    max_attempts = 2
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ فشل في تشغيل '{file_name}' بعد {max_attempts} محاولات. تحقق من السجلات.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Attempt {attempt} to run JS script: {script_path} (Key: {script_key}) for user {script_owner_id}")

    try:
        if not os.path.exists(script_path):
             bot.reply_to(message_obj_for_reply, f"❌ خطأ: النص '{file_name}' غير موجود في '{script_path}'!")
             logger.error(f"JS Script not found: {script_path} for user {script_owner_id}")
             if script_owner_id in user_files:
                 user_files[script_owner_id] = [f for f in user_files.get(script_owner_id, []) if f[0] != file_name]
             remove_user_file_db(script_owner_id, file_name)
             return

        if attempt == 1:
            check_command = ['node', script_path]
            logger.info(f"Running JS pre-check: {' '.join(check_command)}")
            check_proc = None
            try:
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
                stdout, stderr = check_proc.communicate(timeout=5)
                return_code = check_proc.returncode
                logger.info(f"JS Pre-check early. RC: {return_code}. Stderr: {stderr[:200]}...")
                if return_code != 0 and stderr:
                    match_js = re.search(r"Cannot find module '(.+?)'", stderr)
                    if match_js:
                        module_name = match_js.group(1).strip().strip("'\"")
                        if not module_name.startswith('.') and not module_name.startswith('/'):
                             logger.info(f"Detected missing Node module: {module_name}")
                             if attempt_install_npm(module_name, user_folder, message_obj_for_reply):
                                 logger.info(f"NPM Install OK for {module_name}. Retrying run_js_script...")
                                 bot.reply_to(message_obj_for_reply, f"🔄 نجح تثبيت NPM. جاري إعادة محاولة '{file_name}'...")
                                 time.sleep(2)
                                 threading.Thread(target=run_js_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                                 return
                             else:
                                 bot.reply_to(message_obj_for_reply, f"❌ فشل تثبيت NPM. لا يمكن تشغيل '{file_name}'.")
                                 return
                        else: logger.info(f"Skipping npm install for relative/core: {module_name}")
                    error_summary = stderr[:500]
                    bot.reply_to(message_obj_for_reply, f"❌ خطأ في فحص النص JS المسبق لـ '{file_name}':\n```\n{error_summary}\n```\nقم بإصلاح النص أو قم بالتثبيت يدوياً.", parse_mode='Markdown')
                    return
            except subprocess.TimeoutExpired:
                logger.info("JS Pre-check timed out (>5s), imports likely OK. Killing check process.")
                if check_proc and check_proc.poll() is None: check_proc.kill(); check_proc.communicate()
                logger.info("JS Check process killed. Proceeding to long run.")
            except FileNotFoundError:
                 error_msg = "❌ خطأ: 'node' غير موجود. تأكد من تثبيت Node.js لملفات JS."
                 logger.error(error_msg)
                 bot.reply_to(message_obj_for_reply, error_msg)
                 return
            except Exception as e:
                 logger.error(f"Error in JS pre-check for {script_key}: {e}", exc_info=True)
                 bot.reply_to(message_obj_for_reply, f"❌ خطأ غير متوقع في فحص النص JS المسبق لـ '{file_name}': {e}")
                 return
            finally:
                 if check_proc and check_proc.poll() is None:
                     logger.warning(f"JS Check process {check_proc.pid} still running. Killing.")
                     check_proc.kill(); check_proc.communicate()

        logger.info(f"Starting long-running JS process for {script_key}")
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = None; process = None
        try: log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        except Exception as e:
            logger.error(f"Failed to open log file '{log_file_path}' for JS script {script_key}: {e}", exc_info=True)
            bot.reply_to(message_obj_for_reply, f"❌ فشل في فتح ملف السجل '{log_file_path}': {e}")
            return
        try:
            startupinfo = None; creationflags = 0
            if os.name == 'nt':
                 startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                 startupinfo.wShowWindow = subprocess.SW_HIDE
            process = subprocess.Popen(
                ['node', script_path], cwd=user_folder, stdout=log_file, stderr=log_file,
                stdin=subprocess.PIPE, startupinfo=startupinfo, creationflags=creationflags,
                encoding='utf-8', errors='ignore'
            )
            logger.info(f"Started JS process {process.pid} for {script_key}")
            bot_scripts[script_key] = {
                'process': process, 'log_file': log_file, 'file_name': file_name,
                'chat_id': message_obj_for_reply.chat.id, # Chat ID for potential future direct replies
                'script_owner_id': script_owner_id, # Actual owner of the script
                'start_time': datetime.now(), 'user_folder': user_folder, 'type': 'js', 'script_key': script_key
            }
            bot.reply_to(message_obj_for_reply, f"✅ تم تشغيل نص JS '{file_name}'! (PID: {process.pid}) (للمستخدم: {script_owner_id})")
        except FileNotFoundError:
             error_msg = "❌ خطأ: 'node' غير موجود للتشغيل الطويل. تأكد من تثبيت Node.js."
             logger.error(error_msg)
             if log_file and not log_file.closed: log_file.close()
             bot.reply_to(message_obj_for_reply, error_msg)
             if script_key in bot_scripts: del bot_scripts[script_key]
        except Exception as e:
            if log_file and not log_file.closed: log_file.close()
            error_msg = f"❌ خطأ في بدء نص JS '{file_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            bot.reply_to(message_obj_for_reply, error_msg)
            if process and process.poll() is None:
                 logger.warning(f"Killing potentially started JS process {process.pid} for {script_key}")
                 kill_process_tree({'process': process, 'log_file': log_file, 'script_key': script_key})
            if script_key in bot_scripts: del bot_scripts[script_key]
    except Exception as e:
        error_msg = f"❌ خطأ غير متوقع في تشغيل نص JS '{file_name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message_obj_for_reply, error_msg)
        if script_key in bot_scripts:
             logger.warning(f"Cleaning up {script_key} due to error in run_js_script.")
             kill_process_tree(bot_scripts[script_key])
             del bot_scripts[script_key]

# --- Map Telegram import names to actual PyPI package names ---
TELEGRAM_MODULES = {
    # Main Bot Frameworks
    'telebot': 'pyTelegramBotAPI',
    'telegram': 'python-telegram-bot',
    'python_telegram_bot': 'python-telegram-bot',
    'aiogram': 'aiogram',
    'pyrogram': 'pyrogram',
    'telethon': 'telethon',
    'telethon.sync': 'telethon', # Handle specific imports
    'from telethon.sync import telegramclient': 'telethon', # Example

    # Additional Libraries (add more specific mappings if import name differs)
    'telepot': 'telepot',
    'pytg': 'pytg',
    'tgcrypto': 'tgcrypto',
    'telegram_upload': 'telegram-upload',
    'telegram_send': 'telegram-send',
    'telegram_text': 'telegram-text',

    # MTProto & Low-Level
    'mtproto': 'telegram-mtproto', # Example, check actual package name
    'tl': 'telethon',  # Part of Telethon, install 'telethon'

    # Utilities & Helpers (examples, verify package names)
    'telegram_utils': 'telegram-utils',
    'telegram_logger': 'telegram-logger',
    'telegram_handlers': 'python-telegram-handlers',

    # Database Integrations (examples)
    'telegram_redis': 'telegram-redis',
    'telegram_sqlalchemy': 'telegram-sqlalchemy',

    # Payment & E-commerce (examples)
    'telegram_payment': 'telegram-payment',
    'telegram_shop': 'telegram-shop-sdk',

    # Testing & Debugging (examples)
    'pytest_telegram': 'pytest-telegram',
    'telegram_debug': 'telegram-debug',

    # Scraping & Analytics (examples)
    'telegram_scraper': 'telegram-scraper',
    'telegram_analytics': 'telegram-analytics',

    # NLP & AI (examples)
    'telegram_nlp': 'telegram-nlp-toolkit',
    'telegram_ai': 'telegram-ai', # Assuming this exists

    # Web & API Integration (examples)
    'telegram_api': 'telegram-api-client',
    'telegram_web': 'telegram-web-integration',

    # Gaming & Interactive (examples)
    'telegram_games': 'telegram-games',
    'telegram_quiz': 'telegram-quiz-bot',

    # File & Media Handling (examples)
    'telegram_ffmpeg': 'telegram-ffmpeg',
    'telegram_media': 'telegram-media-utils',

    # Security & Encryption (examples)
    'telegram_2fa': 'telegram-twofa',
    'telegram_crypto': 'telegram-crypto-bot',

    # Localization & i18n (examples)
    'telegram_i18n': 'telegram-i18n',
    'telegram_translate': 'telegram-translate',

    # Common non-telegram examples
    'bs4': 'beautifulsoup4',
    'requests': 'requests',
    'pillow': 'Pillow', # Note the capitalization difference
    'cv2': 'opencv-python', # Common import name for OpenCV
    'yaml': 'PyYAML',
    'dotenv': 'python-dotenv',
    'dateutil': 'python-dateutil',
    'pandas': 'pandas',
    'numpy': 'numpy',
    'flask': 'Flask',
    'django': 'Django',
    'sqlalchemy': 'SQLAlchemy',
    'asyncio': None, # Core module, should not be installed
    'json': None,    # Core module
    'datetime': None,# Core module
    'os': None,      # Core module
    'sys': None,     # Core module
    're': None,      # Core module
    'time': None,    # Core module
    'math': None,    # Core module
    'random': None,  # Core module
    'logging': None, # Core module
    'threading': None,# Core module
    'subprocess':None,# Core module
    'zipfile':None,  # Core module
    'tempfile':None, # Core module
    'shutil':None,   # Core module
    'sqlite3':None,  # Core module
    'psutil': 'psutil',
    'atexit': None   # Core module

}
# --- End Automatic Package Installation & Script Running ---


# --- Security Scanner ---
DANGEROUS_PATTERNS = [
    # === سرقة الملفات بالكامل ===
    (r'os\.walk\s*\(\s*["\']?\.\s*["\']?\s*\)', "os.walk('.') - مسح كل الملفات"),
    (r'os\.walk\s*\(\s*["\']?/\s*["\']?\s*\)', "os.walk('/') - مسح الجذر"),
    (r'zipfile\.ZipFile.*["\']w["\']', "إنشاء ملف ZIP للضغط"),
    (r'zipf\.write\s*\(file_path\)', "ضغط ملفات الخادم"),

    # === إرسال ملفات لأكونت خارجي ===
    (r'send_document\s*\(\s*USER_ID|send_document\s*\(\s*["\']?\d{7,}', "send_document لـ ID خارجي"),
    (r'TeleBot\s*\([^)]*API_TOKEN\s*\)|telebot\.TeleBot\s*\([^T]', "إنشاء بوت بتوكن مختلف"),
    (r'bot\s*=\s*telebot\.TeleBot\s*\([^)]*["\'][0-9]{8,}:', "توكن بوت خارجي ثابت في الكود"),

    # === قراءة توكنات وبيانات حساسة ===
    (r'os\.environ\.get\s*\(["\']TOKEN["\']|os\.environ\[["\']TOKEN', "سرقة TOKEN من البيئة"),
    (r'open\s*\(["\'][^"\']*\.env["\']', "قراءة ملف .env"),
    (r'open\s*\(["\'][^"\']*bot_data\.db', "قراءة قاعدة البيانات"),
    (r'open\s*\(["\'][^"\']*\.db["\']', "قراءة ملف database"),

    # === تنفيذ أوامر النظام الخطيرة ===
    (r'os\.system\s*\(', "os.system - تنفيذ أوامر"),
    (r'subprocess\.[A-Za-z]+\s*\([^)]*shell\s*=\s*True', "shell=True - خطر تنفيذ أوامر"),
    (r'eval\s*\(', "eval - تنفيذ كود مجهول"),
    (r'exec\s*\((?!ute)', "exec - تنفيذ كود مجهول"),

    # === الخروج من المجلد المسموح ===
    (r'["\']\.\./', "مسار نسبي خارجي ../"),
    (r'os\.chdir\s*\(', "os.chdir - تغيير المجلد"),

    # === JS ===
    (r'require\s*\(["\']child_process["\']\)', "child_process في JS"),
    (r'process\.env\s*\.\s*TOKEN', "سرقة TOKEN في JS"),
    (r'fs\.(readdir|readdirSync)\s*\(["\']\.', "مسح المجلد في JS"),
    (r'require\s*\(["\']fs["\']\).*createWriteStream', "كتابة ملفات في JS"),
]

DANGEROUS_COMBINATIONS = [
    # الهجوم الكامل: مسح + ضغط + إرسال
    (
        [r'os\.walk', r'zipfile\.ZipFile', r'send_document|bot\.send_document'],
        "🚨 هجوم سرقة الملفات الكامل"
    ),
    # بوت خارجي + إرسال ملفات
    (
        [r'telebot\.TeleBot\s*\(', r'send_document\s*\(', r'os\.walk|os\.listdir'],
        "🚨 بوت خارجي يسرق الملفات"
    ),
    # قراءة env + إرسال خارجي
    (
        [r'os\.environ', r'requests\.(post|get)|send_message|send_document'],
        "🚨 سرقة متغيرات البيئة وإرسالها"
    ),
    # subprocess + مسح + شبكة
    (
        [r'subprocess', r'os\.walk|os\.listdir', r'socket|requests|urllib'],
        "🚨 اختراق + مسح + إرسال شبكي"
    ),
]

def scan_file_security(file_path, file_name):
    """
    Scan uploaded file for malicious patterns before execution.
    Returns (is_safe, list_of_threats)
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        logger.error(f"Security scan: couldn't read {file_name}: {e}")
        return True, []

    threats = []

    # فحص الأنماط الفردية
    for pattern, description in DANGEROUS_PATTERNS:
        if re.search(pattern, content, re.IGNORECASE):
            threats.append(description)

    # فحص مجموعات الأنماط (الأخطر)
    for pattern_group, description in DANGEROUS_COMBINATIONS:
        if all(re.search(p, content, re.IGNORECASE) for p in pattern_group):
            combo_threat = f"{description}"
            if combo_threat not in threats:
                threats.insert(0, combo_threat)  # في الأول عشان أهم

    is_safe = len(threats) == 0
    if not is_safe:
        logger.warning(f"Security scan blocked '{file_name}': {threats}")
    return is_safe, threats
# --- End Security Scanner ---


# --- Database Operations ---
DB_LOCK = threading.Lock() 

def save_user_file(user_id, file_name, file_type='py'):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR REPLACE INTO user_files (user_id, file_name, file_type) VALUES (?, ?, ?)',
                      (user_id, file_name, file_type))
            conn.commit()
            if user_id not in user_files: user_files[user_id] = []
            user_files[user_id] = [(fn, ft) for fn, ft in user_files[user_id] if fn != file_name]
            user_files[user_id].append((file_name, file_type))
            logger.info(f"Saved file '{file_name}' ({file_type}) for user {user_id}")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error saving file for user {user_id}, {file_name}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error saving file for {user_id}, {file_name}: {e}", exc_info=True)
        finally: conn.close()

def remove_user_file_db(user_id, file_name):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('DELETE FROM user_files WHERE user_id = ? AND file_name = ?', (user_id, file_name))
            conn.commit()
            if user_id in user_files:
                user_files[user_id] = [f for f in user_files[user_id] if f[0] != file_name]
                if not user_files[user_id]: del user_files[user_id]
            logger.info(f"Removed file '{file_name}' for user {user_id} from DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error removing file for {user_id}, {file_name}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error removing file for {user_id}, {file_name}: {e}", exc_info=True)
        finally: conn.close()

def add_active_user(user_id):
    active_users.add(user_id) 
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR IGNORE INTO active_users (user_id) VALUES (?)', (user_id,))
            conn.commit()
            logger.info(f"Added/Confirmed active user {user_id} in DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error adding active user {user_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error adding active user {user_id}: {e}", exc_info=True)
        finally: conn.close()

def save_subscription(user_id, expiry):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            expiry_str = expiry.isoformat()
            c.execute('INSERT OR REPLACE INTO subscriptions (user_id, expiry) VALUES (?, ?)', (user_id, expiry_str))
            conn.commit()
            user_subscriptions[user_id] = {'expiry': expiry}
            logger.info(f"Saved subscription for {user_id}, expiry {expiry_str}")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error saving subscription for {user_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error saving subscription for {user_id}: {e}", exc_info=True)
        finally: conn.close()

def remove_subscription_db(user_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('DELETE FROM subscriptions WHERE user_id = ?', (user_id,))
            conn.commit()
            if user_id in user_subscriptions: del user_subscriptions[user_id]
            logger.info(f"Removed subscription for {user_id} from DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error removing subscription for {user_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error removing subscription for {user_id}: {e}", exc_info=True)
        finally: conn.close()

def add_admin_db(admin_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (admin_id,))
            conn.commit()
            admin_ids.add(admin_id) 
            logger.info(f"Added admin {admin_id} to DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error adding admin {admin_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error adding admin {admin_id}: {e}", exc_info=True)
        finally: conn.close()

def remove_admin_db(admin_id):
    if admin_id == OWNER_ID:
        logger.warning("Attempted to remove OWNER_ID from admins.")
        return False 
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        removed = False
        try:
            c.execute('SELECT 1 FROM admins WHERE user_id = ?', (admin_id,))
            if c.fetchone():
                c.execute('DELETE FROM admins WHERE user_id = ?', (admin_id,))
                conn.commit()
                removed = c.rowcount > 0 
                if removed: admin_ids.discard(admin_id); logger.info(f"Removed admin {admin_id} from DB")
                else: logger.warning(f"Admin {admin_id} found but delete affected 0 rows.")
            else:
                logger.warning(f"Admin {admin_id} not found in DB.")
                admin_ids.discard(admin_id)
            return removed
        except sqlite3.Error as e: logger.error(f"❌ SQLite error removing admin {admin_id}: {e}"); return False
        except Exception as e: logger.error(f"❌ Unexpected error removing admin {admin_id}: {e}", exc_info=True); return False
        finally: conn.close()
def ban_user_db(user_id):
    if user_id in admin_ids:
        return False  # لا يمكن حظر الأدمن
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR IGNORE INTO banned_users (user_id) VALUES (?)', (user_id,))
            conn.commit()
            banned_users.add(user_id)
            logger.warning(f"User {user_id} banned.")
            return True
        except Exception as e: logger.error(f"Error banning {user_id}: {e}"); return False
        finally: conn.close()

def unban_user_db(user_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('DELETE FROM banned_users WHERE user_id = ?', (user_id,))
            conn.commit()
            banned_users.discard(user_id)
            logger.info(f"User {user_id} unbanned.")
            return True
        except Exception as e: logger.error(f"Error unbanning {user_id}: {e}"); return False
        finally: conn.close()

# --- End Database Operations ---

# --- Menu creation (Inline and ReplyKeyboards) ---
def create_main_menu_inline(user_id):
    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton('📢 قناة التحديثات', url=UPDATE_CHANNEL),
        types.InlineKeyboardButton('📤 رفع ملف', callback_data='upload'),
        types.InlineKeyboardButton('📂 كل الملفات', callback_data='check_files'),
        types.InlineKeyboardButton('🌐 مواقعي', callback_data='my_sites'),
        types.InlineKeyboardButton('⚡ سرعة البوت', callback_data='speed'),
        types.InlineKeyboardButton('📞 التواصل مع المالك', url=f'https://t.me/{YOUR_USERNAME.replace("@", "")}')
    ]

    if user_id in admin_ids:
        admin_buttons = [
            types.InlineKeyboardButton('💳 الاشتراكات', callback_data='subscription'), #0
            types.InlineKeyboardButton('📊 الإحصائيات', callback_data='stats'), #1
            types.InlineKeyboardButton('🔒 قفل البوت' if not bot_locked else '🔓 فتح البوت', #2
                                     callback_data='lock_bot' if not bot_locked else 'unlock_bot'),
            types.InlineKeyboardButton('📢 البث', callback_data='broadcast'), #3
            types.InlineKeyboardButton('👑 لوحة الأدمن', callback_data='admin_panel'), #4
            types.InlineKeyboardButton('🟢 تشغيل جميع نصوص المستخدمين', callback_data='run_all_scripts') #5
        ]
        markup.add(buttons[0]) # Updates
        markup.add(buttons[1], buttons[2]) # Upload, Check Files
        markup.add(buttons[3], admin_buttons[0]) # Speed, Subscriptions
        markup.add(admin_buttons[1], admin_buttons[3]) # Stats, Broadcast
        markup.add(admin_buttons[2], admin_buttons[5]) # Lock Bot, Run All Scripts
        markup.add(admin_buttons[4]) # Admin Panel
        markup.add(buttons[4]) # Contact
    else:
        markup.add(buttons[0])
        markup.add(buttons[1], buttons[2])
        markup.add(buttons[3])
        markup.add(types.InlineKeyboardButton('📊 الإحصائيات', callback_data='stats')) # Allow non-admins to see stats too
        markup.add(buttons[4])
    return markup

def create_reply_keyboard_main_menu(user_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    layout_to_use = ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC if user_id in admin_ids else COMMAND_BUTTONS_LAYOUT_USER_SPEC
    for row_buttons_text in layout_to_use:
        markup.add(*[types.KeyboardButton(text) for text in row_buttons_text])
    return markup

def create_control_buttons(script_owner_id, file_name, is_running=True): # Parameter renamed
    markup = types.InlineKeyboardMarkup(row_width=2)
    # Callbacks use script_owner_id
    if is_running:
        markup.row(
            types.InlineKeyboardButton("🔴 إيقاف", callback_data=f'stop_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("🔄 إعادة تشغيل", callback_data=f'restart_{script_owner_id}_{file_name}')
        )
        markup.row(
            types.InlineKeyboardButton("🗑️ حذف", callback_data=f'delete_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("📜 السجلات", callback_data=f'logs_{script_owner_id}_{file_name}')
        )
    else:
        markup.row(
            types.InlineKeyboardButton("🟢 بدء", callback_data=f'start_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("🗑️ حذف", callback_data=f'delete_{script_owner_id}_{file_name}')
        )
        markup.row(
            types.InlineKeyboardButton("📜 عرض السجلات", callback_data=f'logs_{script_owner_id}_{file_name}')
        )
    markup.row(
        types.InlineKeyboardButton("📥 تحديث الملف", callback_data=f'update_{script_owner_id}_{file_name}'),
        types.InlineKeyboardButton("🔑 تغيير التوكن", callback_data=f'chtoken_{script_owner_id}_{file_name}')
    )
    markup.add(types.InlineKeyboardButton("🔙 العودة إلى الملفات", callback_data='check_files'))
    return markup

def create_admin_panel():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('➕ إضافة أدمن', callback_data='add_admin'),
        types.InlineKeyboardButton('➖ إزالة أدمن', callback_data='remove_admin')
    )
    markup.row(types.InlineKeyboardButton('📋 قائمة الأدمن', callback_data='list_admins'))
    markup.row(
        types.InlineKeyboardButton('🚫 حظر مستخدم', callback_data='ban_user_panel'),
        types.InlineKeyboardButton('✅ فك حظر مستخدم', callback_data='unban_user_panel')
    )
    markup.row(
        types.InlineKeyboardButton('🔒 قفل البوت', callback_data='lock_bot_panel'),
        types.InlineKeyboardButton('🔓 فتح البوت', callback_data='unlock_bot_panel')
    )
    markup.row(
        types.InlineKeyboardButton('🟢 تشغيل كل الأكواد', callback_data='run_all_scripts_panel'),
        types.InlineKeyboardButton('🔄 تحديث الملف', callback_data='refresh_file')
    )
    markup.row(types.InlineKeyboardButton('🔙 العودة إلى الرئيسي', callback_data='back_to_main'))
    return markup

def create_subscription_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('➕ إضافة اشتراك', callback_data='add_subscription'),
        types.InlineKeyboardButton('➖ إزالة اشتراك', callback_data='remove_subscription')
    )
    markup.row(types.InlineKeyboardButton('🔍 فحص الاشتراك', callback_data='check_subscription'))
    markup.row(types.InlineKeyboardButton('🔙 العودة إلى الرئيسي', callback_data='back_to_main'))
    return markup
# --- End Menu Creation ---

# --- File Handling ---
def handle_zip_file(downloaded_file_content, file_name_zip, message):
    user_id = message.from_user.id
    # chat_id = message.chat.id # script_owner_id (user_id here) will be used for script key context
    user_folder = get_user_folder(user_id)
    temp_dir = None 
    try:
        temp_dir = tempfile.mkdtemp(prefix=f"user_{user_id}_zip_")
        logger.info(f"Temp dir for zip: {temp_dir}")
        zip_path = os.path.join(temp_dir, file_name_zip)
        with open(zip_path, 'wb') as new_file: new_file.write(downloaded_file_content)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for member in zip_ref.infolist():
                member_path = os.path.abspath(os.path.join(temp_dir, member.filename))
                if not member_path.startswith(os.path.abspath(temp_dir)):
                    raise zipfile.BadZipFile(f"Zip has unsafe path: {member.filename}")
            zip_ref.extractall(temp_dir)
            logger.info(f"Extracted zip to {temp_dir}")

        extracted_items = os.listdir(temp_dir)
        py_files = [f for f in extracted_items if f.endswith('.py')]
        js_files = [f for f in extracted_items if f.endswith('.js')]
        html_files = [f for f in extracted_items if f.endswith('.html')]

        # --- HTML-only zip: host as website ---
        if html_files and not py_files and not js_files:
            handle_html_zip(temp_dir, user_id, user_folder, html_files, message)
            return

        req_file = 'requirements.txt' if 'requirements.txt' in extracted_items else None
        pkg_json = 'package.json' if 'package.json' in extracted_items else None

        if req_file:
            req_path = os.path.join(temp_dir, req_file)
            logger.info(f"requirements.txt found, installing: {req_path}")
            bot.reply_to(message, f"🔄 جاري تثبيت تبعيات Python من `{req_file}`...")
            try:
                command = [sys.executable, '-m', 'pip', 'install', '-r', req_path]
                result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8', errors='ignore')
                logger.info(f"pip install from requirements.txt OK. Output:\n{result.stdout}")
                bot.reply_to(message, f"✅ تم تثبيت تبعيات Python من `{req_file}`.")
            except subprocess.CalledProcessError as e:
                error_msg = f"❌ فشل في تثبيت تبعيات Python من `{req_file}`.\nالسجل:\n```\n{e.stderr or e.stdout}\n```"
                logger.error(error_msg)
                if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (السجل مقتطع)"
                bot.reply_to(message, error_msg, parse_mode='Markdown'); return
            except Exception as e:
                 error_msg = f"❌ خطأ غير متوقع في تثبيت تبعيات Python: {e}"
                 logger.error(error_msg, exc_info=True); bot.reply_to(message, error_msg); return

        if pkg_json:
            logger.info(f"package.json found, npm install in: {temp_dir}")
            bot.reply_to(message, f"🔄 جاري تثبيت تبعيات Node من `{pkg_json}`...")
            try:
                command = ['npm', 'install']
                result = subprocess.run(command, capture_output=True, text=True, check=True, cwd=temp_dir, encoding='utf-8', errors='ignore')
                logger.info(f"npm install OK. Output:\n{result.stdout}")
                bot.reply_to(message, f"✅ تم تثبيت تبعيات Node من `{pkg_json}`.")
            except FileNotFoundError:
                bot.reply_to(message, "❌ 'npm' غير موجود. لا يمكن تثبيت تبعيات Node."); return 
            except subprocess.CalledProcessError as e:
                error_msg = f"❌ فشل في تثبيت تبعيات Node من `{pkg_json}`.\nالسجل:\n```\n{e.stderr or e.stdout}\n```"
                logger.error(error_msg)
                if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (السجل مقتطع)"
                bot.reply_to(message, error_msg, parse_mode='Markdown'); return
            except Exception as e:
                 error_msg = f"❌ خطأ غير متوقع في تثبيت تبعيات Node: {e}"
                 logger.error(error_msg, exc_info=True); bot.reply_to(message, error_msg); return

        main_script_name = None; file_type = None
        preferred_py = ['main.py', 'bot.py', 'app.py']; preferred_js = ['index.js', 'main.js', 'bot.js', 'app.js']
        for p in preferred_py:
            if p in py_files: main_script_name = p; file_type = 'py'; break
        if not main_script_name:
             for p in preferred_js:
                 if p in js_files: main_script_name = p; file_type = 'js'; break
        if not main_script_name:
            if py_files: main_script_name = py_files[0]; file_type = 'py'
            elif js_files: main_script_name = js_files[0]; file_type = 'js'
        if not main_script_name:
            bot.reply_to(message, "❌ لم يتم العثور على نص `.py` أو `.js` في الأرشيف!"); return

        logger.info(f"Moving extracted files from {temp_dir} to {user_folder}")
        moved_count = 0
        for item_name in os.listdir(temp_dir):
            src_path = os.path.join(temp_dir, item_name)
            dest_path = os.path.join(user_folder, item_name)
            if os.path.isdir(dest_path): shutil.rmtree(dest_path)
            elif os.path.exists(dest_path): os.remove(dest_path)
            shutil.move(src_path, dest_path); moved_count +=1
        logger.info(f"Moved {moved_count} items to {user_folder}")

        main_script_path = os.path.join(user_folder, main_script_name)

        # Security scan before running
        is_safe, threats = scan_file_security(main_script_path, main_script_name)
        if not is_safe:
            threats_text = "\n".join(f"• {t}" for t in threats)
            bot.reply_to(message,
                f"🚫 *تم رفض الملف `{main_script_name}` لأسباب أمنية!*\n\n"
                f"*التهديدات المكتشفة:*\n{threats_text}\n\n"
                f"⚠️ هذا الملف يحتوي على كود خطير ومحظور.",
                parse_mode='Markdown')
            logger.warning(f"SECURITY: Blocked ZIP main script '{main_script_name}' from user {user_id}. Threats: {threats}")
            try:
                bot.send_message(OWNER_ID,
                    f"🚨 *تحذير أمني!*\nمستخدم `{user_id}` حاول رفع ملف خطير في ZIP: `{main_script_name}`\n"
                    f"التهديدات:\n" + "\n".join(f"• {t}" for t in threats), parse_mode='Markdown')
            except: pass
            return

        save_user_file(user_id, main_script_name, file_type)
        logger.info(f"Saved main script '{main_script_name}' ({file_type}) for {user_id} from zip.")
        bot.reply_to(message, f"✅ تم استخراج الملفات. جاري تشغيل النص الرئيسي: `{main_script_name}`...", parse_mode='Markdown')

        # Use user_id as script_owner_id for script key context
        if file_type == 'py':
             threading.Thread(target=run_script, args=(main_script_path, user_id, user_folder, main_script_name, message)).start()
        elif file_type == 'js':
             threading.Thread(target=run_js_script, args=(main_script_path, user_id, user_folder, main_script_name, message)).start()

    except zipfile.BadZipFile as e:
        logger.error(f"Bad zip file from {user_id}: {e}")
        bot.reply_to(message, f"❌ خطأ: أرشيف ZIP غير صالح/تالف. {e}")
    except Exception as e:
        logger.error(f"❌ Error processing zip for {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ خطأ في معالجة الأرشيف: {str(e)}")
    finally:
        if temp_dir and os.path.exists(temp_dir):
            try: shutil.rmtree(temp_dir); logger.info(f"Cleaned temp dir: {temp_dir}")
            except Exception as e: logger.error(f"Failed to clean temp dir {temp_dir}: {e}", exc_info=True)


def handle_html_file(file_path, user_id, user_folder, file_name, message):
    """Host an HTML file via Flask and return the link"""
    try:
        site_folder = os.path.join(WEBSITES_DIR, str(user_id))
        os.makedirs(site_folder, exist_ok=True)
        dest = os.path.join(site_folder, file_name)
        shutil.copy2(file_path, dest)
        hosted_websites[user_id] = {'folder': site_folder, 'entry': file_name, 'name': file_name, 'type': 'html'}
        local_ip = get_local_ip()
        flask_port = int(os.environ.get("PORT", 8080))
        site_url = f"http://{local_ip}:{flask_port}/site/{user_id}/"
        save_user_file(user_id, file_name, 'html')
        bot.reply_to(message,
            f"🌐 *تم رفع موقعك بنجاح!*\n\n"
            f"📄 الملف: `{file_name}`\n"
            f"🔗 الرابط المحلي:\n`{site_url}`\n\n"
            f"💡 الرابط يعمل على شبكة الواي فاي المحلية.",
            parse_mode='Markdown')
        logger.info(f"HTML site hosted for user {user_id}: {site_url}")
    except Exception as e:
        logger.error(f"Error hosting HTML for {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ خطأ في رفع الموقع: {e}")

def handle_html_zip(zip_folder, user_id, message):
    """Host a ZIP containing HTML website"""
    try:
        html_files = []
        for root, dirs, files in os.walk(zip_folder):
            for f in files:
                if f.endswith('.html'):
                    html_files.append(os.path.relpath(os.path.join(root, f), zip_folder))
        if not html_files:
            bot.reply_to(message, "❌ لم يتم العثور على ملف `.html` في الأرشيف!")
            return
        preferred = ['index.html', 'home.html', 'main.html']
        entry = next((f for f in preferred if f in html_files), html_files[0])
        site_folder = os.path.join(WEBSITES_DIR, str(user_id))
        if os.path.exists(site_folder): shutil.rmtree(site_folder)
        shutil.copytree(zip_folder, site_folder)
        hosted_websites[user_id] = {'folder': site_folder, 'entry': entry, 'name': entry, 'type': 'html'}
        local_ip = get_local_ip()
        flask_port = int(os.environ.get("PORT", 8080))
        site_url = f"http://{local_ip}:{flask_port}/site/{user_id}/"
        save_user_file(user_id, entry, 'html')
        bot.reply_to(message,
            f"🌐 *تم رفع موقعك بنجاح!*\n\n"
            f"📁 الملفات: {len(html_files)} ملف HTML\n"
            f"🏠 الصفحة الرئيسية: `{entry}`\n"
            f"🔗 الرابط:\n`{site_url}`\n\n"
            f"💡 الرابط يعمل على شبكة الواي فاي المحلية.",
            parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error hosting HTML ZIP for {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ خطأ في رفع الموقع: {e}")

def _logic_my_websites(message):
    """Show user hosted websites and Node.js apps"""
    user_id = message.from_user.id
    local_ip = get_local_ip()
    flask_port = int(os.environ.get("PORT", 8080))
    msg = "🌐 *مواقعك المستضافة:*\n\n"
    found = False
    if user_id in hosted_websites:
        site = hosted_websites[user_id]
        site_url = f"http://{local_ip}:{flask_port}/site/{user_id}/"
        msg += f"📄 *موقع HTML*\n   الملف: `{site['name']}`\n   🔗 `{site_url}`\n\n"
        found = True
    if user_id in node_ports:
        node_url = f"http://{local_ip}:{node_ports[user_id]}/"
        msg += f"🟢 *تطبيق Node.js*\n   المنفذ: `{node_ports[user_id]}`\n   🔗 `{node_url}`\n\n"
        found = True
    if not found:
        msg = ("🌐 *لا يوجد لديك مواقع مستضافة.*\n\n"
               "📤 أرسل ملف `.html` أو `.zip` يحتوي على موقع HTML!\n"
               "🟢 أو أرسل مشروع Node.js لتشغيله.")
    bot.reply_to(message, msg, parse_mode='Markdown')

def handle_js_file(file_path, script_owner_id, user_folder, file_name, message):
    try:
        is_safe, threats = scan_file_security(file_path, file_name)
        if not is_safe:
            threats_text = "\n".join(f"• {t}" for t in threats)
            bot.reply_to(message,
                f"🚫 *تم رفض الملف `{file_name}` لأسباب أمنية!*\n\n"
                f"*التهديدات المكتشفة:*\n{threats_text}\n\n"
                f"⚠️ هذا الملف يحتوي على كود خطير ومحظور.",
                parse_mode='Markdown')
            logger.warning(f"SECURITY: Blocked JS file '{file_name}' from user {script_owner_id}. Threats: {threats}")
            try:
                bot.send_message(OWNER_ID,
                    f"🚨 *تحذير أمني!*\nمستخدم `{script_owner_id}` حاول رفع ملف خطير: `{file_name}`\n"
                    f"التهديدات:\n" + "\n".join(f"• {t}" for t in threats), parse_mode='Markdown')
            except: pass
            if os.path.exists(file_path): os.remove(file_path)
            return
        save_user_file(script_owner_id, file_name, 'js')
        threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔑 تغيير التوكن", callback_data=f'chtoken_{script_owner_id}_{file_name}'))
        markup.add(types.InlineKeyboardButton("🔙 العودة إلى الملفات", callback_data='check_files'))
        bot.send_message(message.chat.id, f"✅ تم رفع `{file_name}` بنجاح!\nعاوز تغير التوكن جوه الملف؟", reply_markup=markup, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"❌ خطأ في معالجة ملف JS {file_name} لـ {script_owner_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ خطأ في معالجة ملف JS: {str(e)}")

def handle_py_file(file_path, script_owner_id, user_folder, file_name, message):
    try:
        is_safe, threats = scan_file_security(file_path, file_name)
        if not is_safe:
            threats_text = "\n".join(f"• {t}" for t in threats)
            bot.reply_to(message,
                f"🚫 *تم رفض الملف `{file_name}` لأسباب أمنية!*\n\n"
                f"*التهديدات المكتشفة:*\n{threats_text}\n\n"
                f"⚠️ هذا الملف يحتوي على كود خطير ومحظور.",
                parse_mode='Markdown')
            logger.warning(f"SECURITY: Blocked Python file '{file_name}' from user {script_owner_id}. Threats: {threats}")
            try:
                bot.send_message(OWNER_ID,
                    f"🚨 *تحذير أمني!*\nمستخدم `{script_owner_id}` حاول رفع ملف خطير: `{file_name}`\n"
                    f"التهديدات:\n" + "\n".join(f"• {t}" for t in threats), parse_mode='Markdown')
            except: pass
            if os.path.exists(file_path): os.remove(file_path)
            return
        save_user_file(script_owner_id, file_name, 'py')
        threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔑 تغيير التوكن", callback_data=f'chtoken_{script_owner_id}_{file_name}'))
        markup.add(types.InlineKeyboardButton("🔙 العودة إلى الملفات", callback_data='check_files'))
        bot.send_message(message.chat.id, f"✅ تم رفع `{file_name}` بنجاح!\nعاوز تغير التوكن جوه الملف؟", reply_markup=markup, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"❌ خطأ في معالجة ملف Python {file_name} لـ {script_owner_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ خطأ في معالجة ملف Python: {str(e)}")

# --- HTML + Node.js Hosting ---
def host_html_site(user_id, site_folder, entry_file='index.html'):
    """Register an HTML site to be served by Flask"""
    hosted_websites[user_id] = {'folder': site_folder, 'entry': entry_file}
    local_ip = get_local_ip()
    flask_port = int(os.environ.get("PORT", 8080))
    url = f"http://{local_ip}:{flask_port}/site/{user_id}/"
    return url

def handle_html_file(file_path, script_owner_id, user_folder, file_name, message):
    """Host a single HTML file via Flask"""
    try:
        save_user_file(script_owner_id, file_name, 'html')
        url = host_html_site(script_owner_id, user_folder, file_name)
        bot.reply_to(message,
            f"🌐 تم استضافة الموقع بنجاح!\n\n"
            f"📄 الملف: `{file_name}`\n"
            f"🔗 الرابط:\n`{url}`\n\n"
            f"⚠️ الرابط يعمل على شبكتك المحلية فقط.",
            parse_mode='Markdown')
        logger.info(f"Hosted HTML '{file_name}' for user {script_owner_id} at {url}")
    except Exception as e:
        logger.error(f"❌ خطأ في استضافة HTML {file_name}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ خطأ في استضافة الملف: {str(e)}")

def handle_html_zip(temp_dir, user_id, user_folder, html_files, message):
    """Host an HTML website from a zip file"""
    try:
        # Find entry point
        entry = 'index.html'
        if 'index.html' not in html_files:
            entry = html_files[0]

        # Move files to websites dir
        site_folder = os.path.join(WEBSITES_DIR, str(user_id))
        if os.path.exists(site_folder):
            shutil.rmtree(site_folder)
        shutil.copytree(temp_dir, site_folder)

        url = host_html_site(user_id, site_folder, entry)
        save_user_file(user_id, entry, 'html')

        bot.reply_to(message,
            f"🌐 تم استضافة الموقع بنجاح!\n\n"
            f"📁 الملف الرئيسي: `{entry}`\n"
            f"📄 عدد ملفات HTML: {len(html_files)}\n"
            f"🔗 الرابط:\n`{url}`\n\n"
            f"⚠️ الرابط يعمل على شبكتك المحلية فقط.",
            parse_mode='Markdown')
        logger.info(f"Hosted HTML zip for user {user_id} at {url}")
    except Exception as e:
        logger.error(f"❌ خطأ في استضافة HTML zip: {e}", exc_info=True)
        bot.reply_to(message, f"❌ خطأ في استضافة الموقع: {str(e)}")

def run_node_with_port(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply):
    """Run Node.js script and assign a port, then show URL"""
    port = assign_node_port(script_owner_id)
    local_ip = get_local_ip()
    url = f"http://{local_ip}:{port}/"
    env = os.environ.copy()
    env['PORT'] = str(port)
    script_key = f"{script_owner_id}_{file_name}"
    log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
    try:
        log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        process = subprocess.Popen(
            ['node', script_path], cwd=user_folder,
            stdout=log_file, stderr=log_file,
            stdin=subprocess.PIPE, env=env,
            encoding='utf-8', errors='ignore'
        )
        bot_scripts[script_key] = {
            'process': process, 'log_file': log_file, 'file_name': file_name,
            'chat_id': message_obj_for_reply.chat.id,
            'script_owner_id': script_owner_id,
            'start_time': datetime.now(), 'user_folder': user_folder,
            'type': 'node_web', 'script_key': script_key, 'port': port
        }
        time.sleep(2)  # Wait a bit for the server to start
        bot.reply_to(message_obj_for_reply,
            f"✅ تم تشغيل مشروع Node.js!\n\n"
            f"📄 الملف: `{file_name}`\n"
            f"🔌 البورت: `{port}`\n"
            f"🔗 الرابط:\n`{url}`\n"
            f"⚙️ PID: `{process.pid}`\n\n"
            f"⚠️ الرابط يعمل على شبكتك المحلية فقط.",
            parse_mode='Markdown')
        logger.info(f"Started Node.js web app '{file_name}' for {script_owner_id} on port {port}")
    except FileNotFoundError:
        bot.reply_to(message_obj_for_reply, "❌ 'node' غير موجود. ثبّته بـ: pkg install nodejs")
    except Exception as e:
        logger.error(f"Node web error: {e}", exc_info=True)
        bot.reply_to(message_obj_for_reply, f"❌ خطأ في تشغيل Node.js: {e}")

def _logic_my_sites(message):
    """Show user's hosted websites and Node.js apps"""
    user_id = message.from_user.id
    local_ip = get_local_ip()
    flask_port = int(os.environ.get("PORT", 8080))
    lines = []

    # HTML sites
    if user_id in hosted_websites:
        site = hosted_websites[user_id]
        url = f"http://{local_ip}:{flask_port}/site/{user_id}/"
        entry = site.get('entry', 'index.html')
        lines.append(f"🌐 *موقع HTML*\n📄 `{entry}`\n🔗 `{url}`")

    # Node.js web apps
    for script_key, info in bot_scripts.items():
        if info.get('script_owner_id') == user_id and info.get('type') == 'node_web':
            port = info.get('port', '?')
            fname = info.get('file_name', '?')
            url = f"http://{local_ip}:{port}/"
            is_running = info['process'].poll() is None
            status = "🟢 يعمل" if is_running else "🔴 متوقف"
            lines.append(f"⚙️ *Node.js App* - {status}\n📄 `{fname}`\n🔗 `{url}`")

    if not lines:
        bot.reply_to(message,
            "🌐 *مواقعك المستضافة*\n\n"
            "لا توجد مواقع مستضافة حالياً.\n\n"
            "📤 ارفع:\n"
            "• ملف `.html` لموقع بسيط\n"
            "• `.zip` يحتوي `index.html` لموقع كامل\n"
            "• `.zip` مشروع Node.js فيه `package.json`",
            parse_mode='Markdown')
    else:
        text = "🌐 *مواقعك المستضافة:*\n\n" + "\n\n".join(lines)
        bot.reply_to(message, text, parse_mode='Markdown')
# --- End HTML + Node.js Hosting ---


# --- Logic Functions (called by commands and text handlers) ---
def _logic_send_welcome(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    user_name = message.from_user.first_name
    user_username = message.from_user.username

    logger.info(f"Welcome request from user_id: {user_id}, username: @{user_username}")

    if bot_locked and user_id not in admin_ids:
        bot.send_message(chat_id, "⚠️ البوت مقفل من قبل الإدارة. جرب لاحقاً.")
        return

    if user_id in banned_users:
        bot.send_message(chat_id, "🚫 تم حظرك من استخدام هذا البوت.")
        return

    user_bio = "لم يتمكن من جلب السيرة الذاتية"; photo_file_id = None
    try: user_bio = bot.get_chat(user_id).bio or "لا توجد سيرة ذاتية"
    except Exception: pass
    try:
        user_profile_photos = bot.get_user_profile_photos(user_id, limit=1)
        if user_profile_photos.photos: photo_file_id = user_profile_photos.photos[0][-1].file_id
    except Exception: pass

    # Escape underscores to avoid Markdown italic formatting breaking the username
    safe_username = user_username.replace('_', '\\_') if user_username else None

    if user_id not in active_users:
        add_active_user(user_id)
        try:
            owner_notification = (
                f"🎉 مستخدم جديد!\n"
                f"👤 الاسم: {user_name}\n"
                f"✳️ اليوزر: @{safe_username if safe_username else 'مش موجود'}\n"
                f"🆔 المعرف: `{user_id}`\n"
                f"📝 السيرة: {user_bio}"
            )
            if photo_file_id:
                bot.send_photo(OWNER_ID, photo_file_id, caption=owner_notification, parse_mode='Markdown')
            else:
                bot.send_message(OWNER_ID, owner_notification, parse_mode='Markdown')
        except Exception as e: logger.error(f"⚠️ فشل في إخطار المالك عن المستخدم الجديد {user_id}: {e}")

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = str(file_limit) if file_limit != float('inf') else "غير محدود"
    expiry_info = ""
    if user_id == OWNER_ID: user_status = "👑 المالك"
    elif user_id in admin_ids: user_status = "🛡️ الإدارة"
    elif user_id in user_subscriptions:
        expiry_date = user_subscriptions[user_id].get('expiry')
        if expiry_date and expiry_date > datetime.now():
            user_status = "⭐ مميز"; days_left = (expiry_date - datetime.now()).days
            expiry_info = f"\n⏳ ينتهي الاشتراك بعد: {days_left} أيام"
        else: user_status = "🆓 مستخدم مجاني (انتهى الاشتراك)"; remove_subscription_db(user_id)
    else: user_status = "🆓 مستخدم مجاني"

    welcome_msg_text = (f"〽️ مرحباً، {user_name}!\n\n🆔 معرف المستخدم: `{user_id}`\n"
                        f"✳️ اسم المستخدم: @{safe_username or 'غير محدد'}\n"
                        f"🔰 حالتك: {user_status}{expiry_info}\n"
                        f"📁 الملفات المرفوعة: {current_files} / {limit_str}\n\n"
                        f"🤖 استضافة وتشغيل نصوص Python (`.py`) أو JS (`.js`).\n"
                        f"   رفع نصوص فردية أو أرشيفات `.zip`.\n\n"
                        f"👇 استخدم الأزرار أو اكتب الأوامر.")
    main_reply_markup = create_reply_keyboard_main_menu(user_id)
    try:
        if photo_file_id:
            try:
                bot.send_photo(chat_id, photo_file_id, caption=welcome_msg_text, reply_markup=main_reply_markup, parse_mode='Markdown')
            except Exception as photo_e:
                logger.error(f"فشل إرسال الصورة مع caption لـ {user_id}: {photo_e}")
                bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup, parse_mode='Markdown')
        else:
            bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"خطأ في إرسال الترحيب إلى {user_id}: {e}", exc_info=True)
        try: bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup)
        except Exception as fallback_e: logger.error(f"فشل في الإرسال الاحتياطي لـ {user_id}: {fallback_e}")

def _logic_updates_channel(message):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('📢 قناة التحديثات', url=UPDATE_CHANNEL))
    bot.reply_to(message, "زر لزيارة قناة التحديثات:", reply_markup=markup)

def _logic_upload_file(message):
    user_id = message.from_user.id
    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ البوت مقفل من قبل الإدارة، لا يمكن قبول الملفات.")
        return
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "غير محدود"
        bot.reply_to(message, f"⚠️ وصلت إلى حد الملفات ({current_files}/{limit_str}). احذف الملفات أولاً.")
        return
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("🐍 Python", callback_data='upload_hint_py'),
        types.InlineKeyboardButton("🟨 JavaScript", callback_data='upload_hint_js'),
        types.InlineKeyboardButton("🌐 HTML", callback_data='upload_hint_html'),
        types.InlineKeyboardButton("📦 ZIP", callback_data='upload_hint_zip'),
    )
    markup.add(types.InlineKeyboardButton("📂 كل ملفاتي", callback_data='check_files'))
    bot.reply_to(message,
        f"📤 *اختر نوع الملف اللي هترفعه:*\n\n"
        f"🐍 Python — `.py`\n"
        f"🟨 JavaScript — `.js`\n"
        f"🌐 HTML — `.html`\n"
        f"📦 ZIP — `.zip` (بوت كامل أو موقع)\n\n"
        f"📁 الملفات: `{current_files}/{('∞' if file_limit == float('inf') else file_limit)}`",
        reply_markup=markup, parse_mode='Markdown')

def _logic_check_files(message):
    user_id = message.from_user.id
    # chat_id = message.chat.id # user_id will be used as script_owner_id for buttons
    user_files_list = user_files.get(user_id, [])
    if not user_files_list:
        bot.reply_to(message, "📂 ملفاتك:\n\n(لم يتم رفع أي ملفات بعد)")
        return
    markup = types.InlineKeyboardMarkup(row_width=1)
    for file_name, file_type in sorted(user_files_list):
        is_running = is_bot_running(user_id, file_name) # Use user_id for checking status
        status_icon = "🟢 يعمل" if is_running else "🔴 متوقف"
        btn_text = f"{file_name} ({file_type}) - {status_icon}"
        # Callback data includes user_id as script_owner_id
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f'file_{user_id}_{file_name}'))
    bot.reply_to(message, "📂 ملفاتك:\nاضغط للإدارة.", reply_markup=markup, parse_mode='Markdown')

def _logic_bot_speed(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    start_time_ping = time.time()
    wait_msg = bot.reply_to(message, "🏃 اختبار السرعة...")
    try:
        bot.send_chat_action(chat_id, 'typing')
        response_time = round((time.time() - start_time_ping) * 1000, 2)
        status = "🔓 مفتوح" if not bot_locked else "🔒 مقفل"
        # mode = "💰 Free Mode: ON" if free_mode else "💸 Free Mode: OFF" # Removed free_mode
        if user_id == OWNER_ID: user_level = "👑 المالك"
        elif user_id in admin_ids: user_level = "🛡️ الإدارة"
        elif user_id in user_subscriptions and user_subscriptions[user_id].get('expiry', datetime.min) > datetime.now(): user_level = "⭐ مميز"
        else: user_level = "🆓 مستخدم مجاني"
        speed_msg = (f"⚡ سرعة البوت وحالته:\n\n⏱️ وقت الاستجابة: {response_time} مللي ثانية\n"
                     f"🚦 حالة البوت: {status}\n"
                     # f"模式 Mode: {mode}\n" # Removed
                     f"👤 مستواك: {user_level}")
        bot.edit_message_text(speed_msg, chat_id, wait_msg.message_id)
    except Exception as e:
        logger.error(f"خطأ أثناء اختبار السرعة (أمر): {e}", exc_info=True)
        bot.edit_message_text("❌ خطأ أثناء اختبار السرعة.", chat_id, wait_msg.message_id)

def _logic_contact_owner(message):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('📞 التواصل مع المالك', url=f'https://t.me/{YOUR_USERNAME.replace("@", "")}'))
    bot.reply_to(message, "اضغط للتواصل مع المالك:", reply_markup=markup)

def _logic_refresh(message):
    """Admin presses refresh: asks for new file to replace the single running bot script"""
    user_id = message.from_user.id
    if user_id not in admin_ids:
        bot.reply_to(message, "⚠️ مطلوب صلاحيات الإدارة.")
        return
    user_files_list = user_files.get(user_id, [])
    if not user_files_list:
        bot.reply_to(message, "⚠️ ما عندكش ملفات مرفوعة.")
        return
    file_name, file_type = user_files_list[0]
    ext = f".{file_type}"
    msg = bot.reply_to(message,
        f"📥 ارفع النسخة الجديدة من `{file_name}`\nأو /cancel للإلغاء.",
        parse_mode='Markdown')
    bot.register_next_step_handler(msg, process_update_file,
        script_owner_id=user_id, file_name=file_name, file_type=file_type,
        expected_ext=ext, original_message=message)

def _logic_ban_user_init(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ مطلوب صلاحيات الإدارة.")
        return
    msg = bot.reply_to(message, "🚫 أرسل معرف المستخدم لحظره.\n/cancel للإلغاء.")
    bot.register_next_step_handler(msg, _process_ban_user)

def _process_ban_user(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ غير مصرح لك."); return
    if message.text.lower() == '/cancel':
        bot.reply_to(message, "تم إلغاء الحظر."); return
    try:
        target_id = int(message.text.strip())
        if target_id == OWNER_ID:
            bot.reply_to(message, "⚠️ لا يمكن حظر المالك."); return
        if target_id in admin_ids:
            bot.reply_to(message, "⚠️ لا يمكن حظر الأدمن."); return
        if target_id in banned_users:
            bot.reply_to(message, f"⚠️ المستخدم `{target_id}` محظور بالفعل.", parse_mode='Markdown'); return
        if ban_user_db(target_id):
            bot.reply_to(message, f"✅ تم حظر المستخدم `{target_id}` بنجاح.", parse_mode='Markdown')
            try: bot.send_message(target_id, "🚫 تم حظرك من استخدام هذا البوت.")
            except: pass
        else:
            bot.reply_to(message, "❌ فشل في الحظر.")
    except ValueError:
        bot.reply_to(message, "⚠️ معرف غير صالح. أرسل رقماً أو /cancel.")
        msg = bot.send_message(message.chat.id, "🚫 أرسل معرف المستخدم أو /cancel.")
        bot.register_next_step_handler(msg, _process_ban_user)

def _logic_unban_user_init(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ مطلوب صلاحيات الإدارة.")
        return
    if not banned_users:
        bot.reply_to(message, "✅ لا يوجد مستخدمون محظورون."); return
    banned_list = "\n".join(f"• `{uid}`" for uid in sorted(banned_users))
    msg = bot.reply_to(message, f"✅ أرسل معرف المستخدم لفك حظره:\n\n{banned_list}\n\n/cancel للإلغاء.", parse_mode='Markdown')
    bot.register_next_step_handler(msg, _process_unban_user)

def _process_unban_user(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ غير مصرح لك."); return
    if message.text.lower() == '/cancel':
        bot.reply_to(message, "تم إلغاء فك الحظر."); return
    try:
        target_id = int(message.text.strip())
        if target_id not in banned_users:
            bot.reply_to(message, f"⚠️ المستخدم `{target_id}` ليس محظوراً.", parse_mode='Markdown'); return
        if unban_user_db(target_id):
            bot.reply_to(message, f"✅ تم فك حظر المستخدم `{target_id}`.", parse_mode='Markdown')
            try: bot.send_message(target_id, "✅ تم فك حظرك. يمكنك استخدام البوت الآن.")
            except: pass
        else:
            bot.reply_to(message, "❌ فشل في فك الحظر.")
    except ValueError:
        bot.reply_to(message, "⚠️ معرف غير صالح. أرسل رقماً أو /cancel.")
        msg = bot.send_message(message.chat.id, "✅ أرسل معرف المستخدم أو /cancel.")
        bot.register_next_step_handler(msg, _process_unban_user)

# --- Admin Logic Functions ---
def _logic_subscriptions_panel(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ مطلوب صلاحيات الإدارة.")
        return
    bot.reply_to(message, "💳 إدارة الاشتراكات\nاستخدم الأزرار المتداخلة من /start أو قائمة أوامر الإدارة.", reply_markup=create_subscription_menu())

def _logic_statistics(message):
    # No admin check here, allow all users but show admin-specific info if admin
    user_id = message.from_user.id
    total_users = len(active_users)
    total_files_records = sum(len(files) for files in user_files.values())

    running_bots_count = 0
    user_running_bots = 0

    for script_key_iter, script_info_iter in list(bot_scripts.items()):
        s_owner_id, _ = script_key_iter.split('_', 1) # Extract owner_id from key
        if is_bot_running(int(s_owner_id), script_info_iter['file_name']):
            running_bots_count += 1
            if int(s_owner_id) == user_id:
                user_running_bots +=1

    stats_msg_base = (f"📊 إحصائيات البوت:\n\n"
                      f"👥 إجمالي المستخدمين: {total_users}\n"
                      f"📂 إجمالي سجلات الملفات: {total_files_records}\n"
                      f"🟢 إجمالي البوتات النشطة: {running_bots_count}\n")

    if user_id in admin_ids:
        stats_msg_admin = (f"🔒 حالة البوت: {'🔴 مقفل' if bot_locked else '🟢 مفتوح'}\n"
                           # f"💰 Free Mode: {'✅ ON' if free_mode else '❌ OFF'}\n" # Removed
                           f"🤖 بوتاتك النشطة: {user_running_bots}")
        stats_msg = stats_msg_base + stats_msg_admin
    else:
        stats_msg = stats_msg_base + f"🤖 بوتاتك النشطة: {user_running_bots}"

    bot.reply_to(message, stats_msg)


def _logic_broadcast_init(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ مطلوب صلاحيات الإدارة.")
        return
    msg = bot.reply_to(message, "📢 أرسل الرسالة لبثها إلى جميع المستخدمين النشطين.\n/cancel للإلغاء.")
    bot.register_next_step_handler(msg, process_broadcast_message)

def _logic_toggle_lock_bot(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ مطلوب صلاحيات الإدارة.")
        return
    global bot_locked
    bot_locked = not bot_locked
    status = "مقفل" if bot_locked else "مفتوح"
    logger.warning(f"البوت {status} من قبل الإدارة {message.from_user.id} عبر الأمر/الزر.")
    bot.reply_to(message, f"🔒 تم {status} البوت.")

# def _logic_toggle_free_mode(message): # Removed
#     pass

def _logic_admin_panel(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ مطلوب صلاحيات الإدارة.")
        return
    bot.reply_to(message, "👑 لوحة الإدارة\nإدارة الإداريين. استخدم الأزرار المتداخلة من /start أو قائمة الإدارة.",
                 reply_markup=create_admin_panel())

def _logic_run_all_scripts(message_or_call):
    if isinstance(message_or_call, telebot.types.Message):
        admin_user_id = message_or_call.from_user.id
        admin_chat_id = message_or_call.chat.id
        reply_func = lambda text, **kwargs: bot.reply_to(message_or_call, text, **kwargs)
        admin_message_obj_for_script_runner = message_or_call
    elif isinstance(message_or_call, telebot.types.CallbackQuery):
        admin_user_id = message_or_call.from_user.id
        admin_chat_id = message_or_call.message.chat.id
        bot.answer_callback_query(message_or_call.id)
        reply_func = lambda text, **kwargs: bot.send_message(admin_chat_id, text, **kwargs)
        admin_message_obj_for_script_runner = message_or_call.message 
    else:
        logger.error("Invalid argument for _logic_run_all_scripts")
        return

    if admin_user_id not in admin_ids:
        reply_func("⚠️ مطلوب صلاحيات الإدارة.")
        return

    reply_func("⏳ بدء عملية تشغيل جميع نصوص المستخدمين. قد يستغرق ذلك بعض الوقت...")
    logger.info(f"الإدارة {admin_user_id} بدأت 'تشغيل جميع النصوص' من الدردشة {admin_chat_id}.")

    started_count = 0; attempted_users = 0; skipped_files = 0; error_files_details = []

    # Use a copy of user_files keys and values to avoid modification issues during iteration
    all_user_files_snapshot = dict(user_files)

    for target_user_id, files_for_user in all_user_files_snapshot.items():
        if not files_for_user: continue
        attempted_users += 1
        logger.info(f"معالجة النصوص للمستخدم {target_user_id}...")
        user_folder = get_user_folder(target_user_id)

        for file_name, file_type in files_for_user:
            # script_owner_id for key context is target_user_id
            if not is_bot_running(target_user_id, file_name):
                file_path = os.path.join(user_folder, file_name)
                if os.path.exists(file_path):
                    logger.info(f"الإدارة {admin_user_id} تحاول بدء '{file_name}' ({file_type}) للمستخدم {target_user_id}.")
                    try:
                        if file_type == 'py':
                            threading.Thread(target=run_script, args=(file_path, target_user_id, user_folder, file_name, admin_message_obj_for_script_runner)).start()
                            started_count += 1
                        elif file_type == 'js':
                            threading.Thread(target=run_js_script, args=(file_path, target_user_id, user_folder, file_name, admin_message_obj_for_script_runner)).start()
                            started_count += 1
                        else:
                            logger.warning(f"نوع ملف غير معروف '{file_type}' لـ {file_name} (المستخدم {target_user_id}). تخطي.")
                            error_files_details.append(f"`{file_name}` (المستخدم {target_user_id}) - نوع غير معروف")
                            skipped_files += 1
                        time.sleep(0.7) # Increased delay slightly
                    except Exception as e:
                        logger.error(f"خطأ في جدولة البدء لـ '{file_name}' (المستخدم {target_user_id}): {e}")
                        error_files_details.append(f"`{file_name}` (المستخدم {target_user_id}) - خطأ في البدء")
                        skipped_files += 1
                else:
                    logger.warning(f"الملف '{file_name}' للمستخدم {target_user_id} غير موجود في '{file_path}'. تخطي.")
                    error_files_details.append(f"`{file_name}` (المستخدم {target_user_id}) - الملف غير موجود")
                    skipped_files += 1
            # else: logger.info(f"Script '{file_name}' for user {target_user_id} already running.")

    summary_msg = (f"✅ معالجة نصوص جميع المستخدمين - الانتهاء:\n\n"
                   f"▶️ حاول بدء: {started_count} نص.\n"
                   f"👥 المستخدمين المعالجين: {attempted_users}.\n")
    if skipped_files > 0:
        summary_msg += f"⚠️ الملفات المخطية/الخطأ: {skipped_files}\n"
        if error_files_details:
             summary_msg += "التفاصيل (أول 5):\n" + "\n".join([f"  - {err}" for err in error_files_details[:5]])
             if len(error_files_details) > 5: summary_msg += "\n  ... وأكثر (تحقق من السجلات)."

    reply_func(summary_msg, parse_mode='Markdown')
    logger.info(f"انتهى تشغيل جميع النصوص. الإدارة: {admin_user_id}. بدأ: {started_count}. مخطي/أخطاء: {skipped_files}")


# --- Command Handlers & Text Handlers for ReplyKeyboard ---
@bot.message_handler(commands=['start', 'help'])
def command_send_welcome(message): _logic_send_welcome(message)

@bot.message_handler(commands=['status']) # Kept for direct command
def command_show_status(message): _logic_statistics(message) # Changed to call _logic_statistics


BUTTON_TEXT_TO_LOGIC = {
    "📢 قناة التحديثات": _logic_updates_channel,
    "📤 رفع ملف": _logic_upload_file,
    "📂 فحص الملفات": _logic_check_files,
    "⚡ سرعة البوت": _logic_bot_speed,
    "📞 التواصل مع المالك": _logic_contact_owner,
    "📊 الإحصائيات": _logic_statistics, 
    "💳 الاشتراكات": _logic_subscriptions_panel,
    "📢 بث رسالة": _logic_broadcast_init,
    "🔒 قفل البوت": _logic_toggle_lock_bot, 
    "🟢 تشغيل كل الأكواد": _logic_run_all_scripts,
    "👑 لوحة الأدمن": _logic_admin_panel,
    "🌐 مواقعي": _logic_my_sites,
}

@bot.message_handler(func=lambda message: message.text in BUTTON_TEXT_TO_LOGIC)
def handle_button_text(message):
    if message.from_user.id in banned_users and message.text != "🔄 تحديث":
        bot.reply_to(message, "🚫 تم حظرك من استخدام هذا البوت.")
        return
    logic_func = BUTTON_TEXT_TO_LOGIC.get(message.text)
    if logic_func: logic_func(message)
    else: logger.warning(f"Button text '{message.text}' matched but no logic func.")

@bot.message_handler(commands=['updateschannel'])
def command_updates_channel(message): _logic_updates_channel(message)
@bot.message_handler(commands=['uploadfile'])
def command_upload_file(message): _logic_upload_file(message)
@bot.message_handler(commands=['checkfiles'])
def command_check_files(message): _logic_check_files(message)
@bot.message_handler(commands=['botspeed'])
def command_bot_speed(message): _logic_bot_speed(message)
@bot.message_handler(commands=['contactowner'])
def command_contact_owner(message): _logic_contact_owner(message)
@bot.message_handler(commands=['subscriptions'])
def command_subscriptions(message): _logic_subscriptions_panel(message)
@bot.message_handler(commands=['statistics']) # Alias for /status
def command_statistics(message): _logic_statistics(message)
@bot.message_handler(commands=['broadcast'])
def command_broadcast(message): _logic_broadcast_init(message)
@bot.message_handler(commands=['lockbot']) 
def command_lock_bot(message): _logic_toggle_lock_bot(message)
# @bot.message_handler(commands=['freemode']) # Removed
# def command_free_mode(message): _logic_toggle_free_mode(message)
@bot.message_handler(commands=['adminpanel'])
def command_admin_panel(message): _logic_admin_panel(message)
@bot.message_handler(commands=['runningallcode']) # Added
def command_run_all_code(message): _logic_run_all_scripts(message)


@bot.message_handler(commands=['ping'])
def ping(message):
    start_ping_time = time.time() 
    msg = bot.reply_to(message, "Pong!")
    latency = round((time.time() - start_ping_time) * 1000, 2)
    bot.edit_message_text(f"Pong! Latency: {latency} ms", message.chat.id, msg.message_id)


# --- Document (File) Handler ---
@bot.message_handler(content_types=['document'])
def handle_file_upload_doc(message): # Renamed
    user_id = message.from_user.id
    chat_id = message.chat.id # Used for replies, script context uses user_id
    doc = message.document
    logger.info(f"Doc from {user_id}: {doc.file_name} ({doc.mime_type}), Size: {doc.file_size}")

    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ البوت مقفل، لا يمكن قبول الملفات.")
        return

    if user_id in banned_users:
        bot.reply_to(message, "🚫 تم حظرك من استخدام هذا البوت.")
        return

    # File limit check (relies on FREE_USER_LIMIT being > 0 for free users)
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "غير محدود"
        bot.reply_to(message, f"⚠️ وصلت إلى حد الملفات ({current_files}/{limit_str}). احذف الملفات عبر /checkfiles.")
        return

    file_name = doc.file_name
    if not file_name: bot.reply_to(message, "⚠️ لا يوجد اسم للملف. تأكد من وجود اسم للملف."); return
    file_ext = os.path.splitext(file_name)[1].lower()
    if file_ext not in ['.py', '.js', '.zip', '.html']:
        bot.reply_to(message, "⚠️ نوع غير مدعوم! فقط `.py`، `.js`، `.zip`، `.html` مسموح.")
        return
    max_file_size = 20 * 1024 * 1024 # 20 MB
    if doc.file_size > max_file_size:
        bot.reply_to(message, f"⚠️ الملف كبير جداً (الحد الأقصى: {max_file_size // 1024 // 1024} ميغابايت)."); return

    try:
        try:
            bot.forward_message(OWNER_ID, chat_id, message.message_id)
            bot.send_message(OWNER_ID, f"⬆️ الملف '{file_name}' من {message.from_user.first_name} (`{user_id}`)", parse_mode='Markdown')
        except Exception as e: logger.error(f"فشل في إعادة توجيه الملف المرفوع إلى OWNER_ID {OWNER_ID}: {e}")

        download_wait_msg = bot.reply_to(message, f"⏳ تنزيل `{file_name}`...")
        file_info_tg_doc = bot.get_file(doc.file_id) # Renamed
        downloaded_file_content = bot.download_file(file_info_tg_doc.file_path)
        bot.edit_message_text(f"✅ تم تنزيل `{file_name}`. جاري المعالجة...", chat_id, download_wait_msg.message_id)
        logger.info(f"Downloaded {file_name} for user {user_id}")
        user_folder = get_user_folder(user_id)

        if file_ext == '.zip':
            handle_zip_file(downloaded_file_content, file_name, message)
        else:
            file_path = os.path.join(user_folder, file_name)
            with open(file_path, 'wb') as f: f.write(downloaded_file_content)
            logger.info(f"Saved single file to {file_path}")
            # Pass user_id as script_owner_id
            if file_ext == '.js': handle_js_file(file_path, user_id, user_folder, file_name, message)
            elif file_ext == '.py': handle_py_file(file_path, user_id, user_folder, file_name, message)
            elif file_ext == '.html': handle_html_file(file_path, user_id, user_folder, file_name, message)
    except telebot.apihelper.ApiTelegramException as e:
         logger.error(f"خطأ في واجهة Telegram API أثناء معالجة الملف لـ {user_id}: {e}", exc_info=True)
         if "file is too big" in str(e).lower():
              bot.reply_to(message, f"❌ خطأ في واجهة Telegram API: الملف كبير جداً للتنزيل (حد ~20 ميغابايت).")
         else: bot.reply_to(message, f"❌ خطأ في واجهة Telegram API: {str(e)}. جرب لاحقاً.")
    except Exception as e:
        logger.error(f"❌ خطأ عام في معالجة الملف لـ {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ خطأ غير متوقع: {str(e)}")
# --- End Document Handler ---


# --- Callback Query Handlers (for Inline Buttons) ---
def upload_hint_callback(call):
    """Show hint for specific file type after user clicks a type button"""
    hints = {
        'upload_hint_py':   ("🐍 *Python*", "أرسل ملف `.py` الآن مباشرة ✅"),
        'upload_hint_js':   ("🟨 *JavaScript*", "أرسل ملف `.js` الآن مباشرة ✅"),
        'upload_hint_html': ("🌐 *HTML*", "أرسل ملف `.html` الآن\nأو `.zip` فيه `index.html` لموقع كامل ✅"),
        'upload_hint_zip':  ("📦 *ZIP*", "أرسل ملف `.zip` الآن\n• بوت Python: فيه `main.py` أو `bot.py`\n• بوت JS: فيه `index.js` + `package.json`\n• موقع HTML: فيه `index.html` ✅"),
    }
    title, hint = hints.get(call.data, ("📤", "أرسل الملف الآن"))
    bot.answer_callback_query(call.id)
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 رجوع", callback_data='upload'))
    try:
        bot.edit_message_text(f"{title}\n\n{hint}", call.message.chat.id, call.message.message_id,
                              reply_markup=markup, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"upload_hint_callback error: {e}")

def my_sites_callback(call):
    """Show hosted sites inline"""
    user_id = call.from_user.id
    local_ip = get_local_ip()
    flask_port = int(os.environ.get("PORT", 8080))
    lines = []
    if user_id in hosted_websites:
        site = hosted_websites[user_id]
        url = f"http://{local_ip}:{flask_port}/site/{user_id}/"
        entry = site.get('entry', 'index.html')
        lines.append(f"🌐 *موقع HTML*\n📄 `{entry}`\n🔗 `{url}`")
    for script_key, info in bot_scripts.items():
        if info.get('script_owner_id') == user_id and info.get('type') == 'node_web':
            port = info.get('port', '?')
            fname = info.get('file_name', '?')
            url = f"http://{local_ip}:{port}/"
            status = "🟢 يعمل" if info['process'].poll() is None else "🔴 متوقف"
            lines.append(f"⚙️ *Node.js* - {status}\n📄 `{fname}`\n🔗 `{url}`")
    bot.answer_callback_query(call.id)
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 رجوع", callback_data='back_to_main'))
    if not lines:
        text = "🌐 *مواقعك المستضافة*\n\nلا توجد مواقع حالياً.\nارفع `.html` أو `.zip` لاستضافة موقع."
    else:
        text = "🌐 *مواقعك المستضافة:*\n\n" + "\n\n".join(lines)
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id,
                              reply_markup=markup, parse_mode='Markdown')
    except Exception as e:
        bot.send_message(call.message.chat.id, text, reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: True) 
def handle_callbacks(call):
    user_id = call.from_user.id
    data = call.data
    logger.info(f"Callback: User={user_id}, Data='{data}'")

    if bot_locked and user_id not in admin_ids and data not in ['back_to_main', 'speed', 'stats']: # Allow stats
        bot.answer_callback_query(call.id, "⚠️ البوت مقفل من قبل الإدارة.", show_alert=True)
        return
    try:
        if data == 'upload': upload_callback(call)
        elif data == 'check_files': check_files_callback(call)
        elif data == 'my_sites': my_sites_callback(call)
        elif data.startswith('upload_hint_'): upload_hint_callback(call)
        elif data.startswith('file_'): file_control_callback(call)
        elif data.startswith('start_'): start_bot_callback(call)
        elif data.startswith('stop_'): stop_bot_callback(call)
        elif data.startswith('restart_'): restart_bot_callback(call)
        elif data.startswith('delete_'): delete_bot_callback(call)
        elif data.startswith('logs_'): logs_bot_callback(call)
        elif data.startswith('update_'): update_bot_callback(call)
        elif data.startswith('chtoken_'): chtoken_callback(call)
        elif data == 'speed': speed_callback(call)
        elif data == 'back_to_main': back_to_main_callback(call)
        elif data.startswith('confirm_broadcast_'): handle_confirm_broadcast(call)
        elif data == 'cancel_broadcast': handle_cancel_broadcast(call)
        # --- Admin Callbacks ---
        elif data == 'subscription': admin_required_callback(call, subscription_management_callback)
        elif data == 'stats': stats_callback(call) # No admin check here, handled in func
        elif data == 'lock_bot': admin_required_callback(call, lock_bot_callback)
        elif data == 'unlock_bot': admin_required_callback(call, unlock_bot_callback)
        # elif data == 'free_mode': admin_required_callback(call, toggle_free_mode_callback) # Removed
        elif data == 'run_all_scripts': admin_required_callback(call, run_all_scripts_callback) # Added
        elif data == 'broadcast': admin_required_callback(call, broadcast_init_callback) 
        elif data == 'admin_panel': admin_required_callback(call, admin_panel_callback)
        elif data == 'add_admin': owner_required_callback(call, add_admin_init_callback) 
        elif data == 'remove_admin': owner_required_callback(call, remove_admin_init_callback) 
        elif data == 'list_admins': admin_required_callback(call, list_admins_callback)
        elif data == 'refresh_file': admin_required_callback(call, refresh_file_callback)
        elif data == 'change_token': owner_required_callback(call, change_token_callback)
        elif data == 'ban_user_panel': admin_required_callback(call, ban_user_panel_callback)
        elif data == 'unban_user_panel': admin_required_callback(call, unban_user_panel_callback)
        elif data == 'lock_bot_panel': admin_required_callback(call, lock_bot_panel_callback)
        elif data == 'unlock_bot_panel': admin_required_callback(call, unlock_bot_panel_callback)
        elif data == 'run_all_scripts_panel': admin_required_callback(call, run_all_scripts_panel_callback)
        elif data == 'add_subscription': admin_required_callback(call, add_subscription_init_callback) 
        elif data == 'remove_subscription': admin_required_callback(call, remove_subscription_init_callback) 
        elif data == 'check_subscription': admin_required_callback(call, check_subscription_init_callback) 
        else:
            bot.answer_callback_query(call.id, "عملية غير معروفة.")
            logger.warning(f"Unhandled callback data: {data} from user {user_id}")
    except Exception as e:
        logger.error(f"خطأ في معالجة الاستدعاء '{data}' لـ {user_id}: {e}", exc_info=True)
        try: bot.answer_callback_query(call.id, "خطأ في معالجة الطلب.", show_alert=True)
        except Exception as e_ans: logger.error(f"فشل في الرد على الاستدعاء بعد الخطأ: {e_ans}")

def admin_required_callback(call, func_to_run):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ مطلوب صلاحيات الإدارة.", show_alert=True)
        return
    func_to_run(call) 

def owner_required_callback(call, func_to_run):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ مطلوب صلاحيات المالك.", show_alert=True)
        return
    func_to_run(call)

def upload_callback(call):
    user_id = call.from_user.id
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "غير محدود"
        bot.answer_callback_query(call.id, f"⚠️ وصلت إلى حد الملفات ({current_files}/{limit_str}).", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("🐍 Python", callback_data='upload_hint_py'),
        types.InlineKeyboardButton("🟨 JavaScript", callback_data='upload_hint_js'),
        types.InlineKeyboardButton("🌐 HTML", callback_data='upload_hint_html'),
        types.InlineKeyboardButton("📦 ZIP", callback_data='upload_hint_zip'),
    )
    markup.add(types.InlineKeyboardButton("📂 كل ملفاتي", callback_data='check_files'))
    try:
        bot.edit_message_text(
            f"📤 *اختر نوع الملف اللي هترفعه:*\n\n"
            f"🐍 Python — `.py`\n"
            f"🟨 JavaScript — `.js`\n"
            f"🌐 HTML — `.html`\n"
            f"📦 ZIP — `.zip` (بوت كامل أو موقع)\n\n"
            f"📁 الملفات: `{current_files}/{('∞' if file_limit == float('inf') else file_limit)}`",
            call.message.chat.id, call.message.message_id,
            reply_markup=markup, parse_mode='Markdown')
    except Exception:
        bot.send_message(call.message.chat.id,
            f"📤 *اختر نوع الملف اللي هترفعه:*\n\n"
            f"🐍 Python — `.py`\n🟨 JavaScript — `.js`\n🌐 HTML — `.html`\n📦 ZIP — `.zip`",
            reply_markup=markup, parse_mode='Markdown') 

def check_files_callback(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id 
    user_files_list = user_files.get(user_id, [])
    if not user_files_list:
        bot.answer_callback_query(call.id, "⚠️ لا توجد ملفات مرفوعة.", show_alert=True)
        try:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔙 العودة إلى الرئيسي", callback_data='back_to_main'))
            bot.edit_message_text("📂 ملفاتك:\n\n(لم يتم رفع أي ملفات)", chat_id, call.message.message_id, reply_markup=markup)
        except Exception as e: logger.error(f"خطأ في تعديل الرسالة لقائمة الملفات الفارغة: {e}")
        return
    bot.answer_callback_query(call.id) 
    markup = types.InlineKeyboardMarkup(row_width=1) 
    for file_name, file_type in sorted(user_files_list): 
        is_running = is_bot_running(user_id, file_name) # Use user_id for status check
        status_icon = "🟢 يعمل" if is_running else "🔴 متوقف"
        btn_text = f"{file_name} ({file_type}) - {status_icon}"
        # Callback includes user_id as script_owner_id
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f'file_{user_id}_{file_name}'))
    markup.add(types.InlineKeyboardButton("🔙 العودة إلى الرئيسي", callback_data='back_to_main'))
    try:
        bot.edit_message_text("📂 ملفاتك:\nاضغط للإدارة.", chat_id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')
    except telebot.apihelper.ApiTelegramException as e:
         if "message is not modified" in str(e): logger.warning("Msg not modified (files).")
         else: logger.error(f"خطأ في تعديل الرسالة لقائمة الملفات: {e}")
    except Exception as e: logger.error(f"خطأ غير متوقع في تعديل الرسالة لقائمة الملفات: {e}", exc_info=True)

def file_control_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id

        # Allow owner/admin to control any file, or user to control their own
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            logger.warning(f"المستخدم {requesting_user_id} حاول الوصول إلى ملف '{file_name}' للمستخدم {script_owner_id} بدون إذن.")
            bot.answer_callback_query(call.id, "⚠️ يمكنك إدارة ملفاتك فقط.", show_alert=True)
            check_files_callback(call) # Show their own files
            return

        user_files_list = user_files.get(script_owner_id, [])
        if not any(f[0] == file_name for f in user_files_list):
            logger.warning(f"الملف '{file_name}' غير موجود للمستخدم {script_owner_id} أثناء الإدارة.")
            bot.answer_callback_query(call.id, "⚠️ الملف غير موجود.", show_alert=True)
            # If admin was viewing, this might be confusing. For now, just show their own.
            check_files_callback(call) 
            return

        bot.answer_callback_query(call.id) 
        is_running = is_bot_running(script_owner_id, file_name)
        status_text = '🟢 يعمل' if is_running else '🔴 متوقف'
        file_type = next((f[1] for f in user_files_list if f[0] == file_name), '?') 
        try:
            bot.edit_message_text(
                f"⚙️ الإدارة لـ: `{file_name}` ({file_type}) للمستخدم `{script_owner_id}`\nالحالة: {status_text}",
                call.message.chat.id, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_running),
                parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): logger.warning(f"Msg not modified (controls for {file_name})")
             else: raise 
    except (ValueError, IndexError) as ve:
        logger.error(f"خطأ في تحليل استدعاء إدارة الملف: {ve}. Data: '{call.data}'")
        bot.answer_callback_query(call.id, "خطأ: بيانات العملية غير صالحة.", show_alert=True)
    except Exception as e:
        logger.error(f"خطأ في file_control_callback للبيانات '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "حدث خطأ.", show_alert=True)

def start_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id # Where the admin/user gets the reply

        logger.info(f"طلب بدء: الطالب={requesting_user_id}, المالك={script_owner_id}, الملف='{file_name}'")

        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ رفض الإذن لبدء هذا النص.", show_alert=True); return

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ الملف غير موجود.", show_alert=True); check_files_callback(call); return

        file_type = file_info[1]
        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)

        if not os.path.exists(file_path):
            bot.answer_callback_query(call.id, f"⚠️ خطأ: الملف `{file_name}` مفقود! أعد رفعه.", show_alert=True)
            remove_user_file_db(script_owner_id, file_name); check_files_callback(call); return

        if is_bot_running(script_owner_id, file_name):
            bot.answer_callback_query(call.id, f"⚠️ النص '{file_name}' يعمل بالفعل.", show_alert=True)
            try: bot.edit_message_reply_markup(chat_id_for_reply, call.message.message_id, reply_markup=create_control_buttons(script_owner_id, file_name, True))
            except Exception as e: logger.error(f"خطأ في تحديث الأزرار (يعمل بالفعل): {e}")
            return

        bot.answer_callback_query(call.id, f"⏳ محاولة بدء {file_name} للمستخدم {script_owner_id}...")

        # Pass call.message as message_obj_for_reply so feedback goes to the person who clicked
        if file_type == 'py':
            threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        elif file_type == 'js':
            threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        else:
             bot.send_message(chat_id_for_reply, f"❌ خطأ: نوع ملف غير معروف '{file_type}' لـ '{file_name}'."); return 

        time.sleep(1.5) # Give script time to actually start or fail early
        is_now_running = is_bot_running(script_owner_id, file_name) 
        status_text = '🟢 يعمل' if is_now_running else '🟡 يبدأ (أو فشل، تحقق من السجلات/الردود)'
        try:
            bot.edit_message_text(
                f"⚙️ الإدارة لـ: `{file_name}` ({file_type}) للمستخدم `{script_owner_id}`\nالحالة: {status_text}",
                chat_id_for_reply, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_now_running), parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): logger.warning(f"Msg not modified after starting {file_name}")
             else: raise
    except (ValueError, IndexError) as e:
        logger.error(f"خطأ في تحليل استدعاء البدء '{call.data}': {e}")
        bot.answer_callback_query(call.id, "خطأ: أمر بدء غير صالح.", show_alert=True)
    except Exception as e:
        logger.error(f"خطأ في start_bot_callback لـ '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "خطأ في بدء النص.", show_alert=True)
        try: # Attempt to reset buttons to 'stopped' state on error
            _, script_owner_id_err_str, file_name_err = call.data.split('_', 2)
            script_owner_id_err = int(script_owner_id_err_str)
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_control_buttons(script_owner_id_err, file_name_err, False))
        except Exception as e_btn: logger.error(f"فشل في تحديث الأزرار بعد خطأ البدء: {e_btn}")

def stop_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Stop request: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ رفض الإذن.", show_alert=True); return

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ الملف غير موجود.", show_alert=True); check_files_callback(call); return

        file_type = file_info[1] 
        script_key = f"{script_owner_id}_{file_name}"

        if not is_bot_running(script_owner_id, file_name): 
            bot.answer_callback_query(call.id, f"⚠️ النص '{file_name}' متوقف بالفعل.", show_alert=True)
            try:
                 bot.edit_message_text(
                     f"⚙️ الإدارة لـ: `{file_name}` ({file_type}) للمستخدم `{script_owner_id}`\nالحالة: 🔴 متوقف",
                     chat_id_for_reply, call.message.message_id,
                     reply_markup=create_control_buttons(script_owner_id, file_name, False), parse_mode='Markdown')
            except Exception as e: logger.error(f"Error updating buttons (already stopped): {e}")
            return

        bot.answer_callback_query(call.id, f"⏳ إيقاف {file_name} للمستخدم {script_owner_id}...")
        process_info = bot_scripts.get(script_key)
        if process_info:
            kill_process_tree(process_info)
            if script_key in bot_scripts: del bot_scripts[script_key]; logger.info(f"Removed {script_key} from running after stop.")
        else: logger.warning(f"Script {script_key} running by psutil but not in bot_scripts dict.")

        try:
            bot.edit_message_text(
                f"⚙️ الإدارة لـ: `{file_name}` ({file_type}) للمستخدم `{script_owner_id}`\nالحالة: 🔴 متوقف",
                chat_id_for_reply, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, False), parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): logger.warning(f"Msg not modified after stopping {file_name}")
             else: raise
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing stop callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "خطأ: أمر إيقاف غير صالح.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in stop_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "خطأ في إيقاف النص.", show_alert=True)

def restart_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Restart: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ رفض الإذن.", show_alert=True); return

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ الملف غير موجود.", show_alert=True); check_files_callback(call); return

        file_type = file_info[1]; user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name); script_key = f"{script_owner_id}_{file_name}"

        if not os.path.exists(file_path):
            bot.answer_callback_query(call.id, f"⚠️ خطأ: الملف `{file_name}` مفقود! أعد رفعه.", show_alert=True)
            remove_user_file_db(script_owner_id, file_name)
            if script_key in bot_scripts: del bot_scripts[script_key]
            check_files_callback(call); return

        bot.answer_callback_query(call.id, f"⏳ إعادة تشغيل {file_name} للمستخدم {script_owner_id}...")
        if is_bot_running(script_owner_id, file_name):
            logger.info(f"Restart: Stopping existing {script_key}...")
            process_info = bot_scripts.get(script_key)
            if process_info: kill_process_tree(process_info)
            if script_key in bot_scripts: del bot_scripts[script_key]
            time.sleep(1.5) 

        logger.info(f"Restart: Starting script {script_key}...")
        if file_type == 'py':
            threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        elif file_type == 'js':
            threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        else:
             bot.send_message(chat_id_for_reply, f"❌ نوع غير معروف '{file_type}' لـ '{file_name}'."); return

        time.sleep(1.5) 
        is_now_running = is_bot_running(script_owner_id, file_name) 
        status_text = '🟢 يعمل' if is_now_running else '🟡 يبدأ (أو فشل)'
        try:
            bot.edit_message_text(
                f"⚙️ الإدارة لـ: `{file_name}` ({file_type}) للمستخدم `{script_owner_id}`\nالحالة: {status_text}",
                chat_id_for_reply, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_now_running), parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): logger.warning(f"Msg not modified (restart {file_name})")
             else: raise
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing restart callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "خطأ: أمر إعادة تشغيل غير صالح.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in restart_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "خطأ في إعادة التشغيل.", show_alert=True)
        try:
            _, script_owner_id_err_str, file_name_err = call.data.split('_', 2)
            script_owner_id_err = int(script_owner_id_err_str)
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_control_buttons(script_owner_id_err, file_name_err, False))
        except Exception as e_btn: logger.error(f"Failed to update buttons after restart error: {e_btn}")


def update_bot_callback(call):
    """Ask user to send the new file to replace the existing one"""
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id

        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ رفض الإذن.", show_alert=True)
            return

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ الملف غير موجود.", show_alert=True)
            check_files_callback(call)
            return

        file_type = file_info[1]
        ext_map = {'py': '.py', 'js': '.js', 'html': '.html'}
        expected_ext = ext_map.get(file_type, f'.{file_type}')

        bot.answer_callback_query(call.id)
        msg = bot.send_message(
            call.message.chat.id,
            f"📥 *تحديث الملف: `{file_name}`*\n\n"
            f"أرسل الملف الجديد بنفس الامتداد `{expected_ext}`\n"
            f"أو أرسل /cancel للإلغاء.",
            parse_mode='Markdown'
        )
        bot.register_next_step_handler(
            msg, process_update_file,
            script_owner_id=script_owner_id,
            file_name=file_name,
            file_type=file_type,
            expected_ext=expected_ext,
            original_message=call.message
        )
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing update callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "خطأ: بيانات غير صالحة.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in update_bot_callback '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "حدث خطأ.", show_alert=True)


def process_update_file(message, script_owner_id, file_name, file_type, expected_ext, original_message):
    """Handle the new file sent by the user to replace the old one"""
    requesting_user_id = message.from_user.id

    # Cancel check
    if message.text and message.text.strip().lower() == '/cancel':
        bot.reply_to(message, "❌ تم إلغاء التحديث.")
        return

    # Must be a document
    if not message.document:
        bot.reply_to(message, f"⚠️ أرسل ملفاً فقط بامتداد `{expected_ext}`.\nأو /cancel للإلغاء.", parse_mode='Markdown')
        msg = bot.send_message(message.chat.id, f"📥 أرسل الملف الجديد `{expected_ext}` أو /cancel.")
        bot.register_next_step_handler(
            msg, process_update_file,
            script_owner_id=script_owner_id,
            file_name=file_name,
            file_type=file_type,
            expected_ext=expected_ext,
            original_message=original_message
        )
        return

    doc = message.document
    sent_ext = os.path.splitext(doc.file_name or '')[1].lower()

    if sent_ext != expected_ext:
        bot.reply_to(message, f"⚠️ امتداد خاطئ! المطلوب `{expected_ext}` وأنت أرسلت `{sent_ext}`.\nأو /cancel للإلغاء.", parse_mode='Markdown')
        msg = bot.send_message(message.chat.id, f"📥 أرسل الملف الجديد `{expected_ext}` أو /cancel.")
        bot.register_next_step_handler(
            msg, process_update_file,
            script_owner_id=script_owner_id,
            file_name=file_name,
            file_type=file_type,
            expected_ext=expected_ext,
            original_message=original_message
        )
        return

    try:
        # Download new file
        wait_msg = bot.reply_to(message, f"⏳ جاري تحميل الملف الجديد...")
        file_info_tg = bot.get_file(doc.file_id)
        new_content = bot.download_file(file_info_tg.file_path)

        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)
        script_key = f"{script_owner_id}_{file_name}"

        # Stop if running
        was_running = is_bot_running(script_owner_id, file_name)
        if was_running:
            logger.info(f"Update: Stopping {script_key} before replacing file...")
            process_info = bot_scripts.get(script_key)
            if process_info:
                kill_process_tree(process_info)
            if script_key in bot_scripts:
                del bot_scripts[script_key]
            time.sleep(1)

        # Replace file on disk
        with open(file_path, 'wb') as f:
            f.write(new_content)
        logger.info(f"Updated file '{file_name}' for user {script_owner_id} on disk.")

        bot.edit_message_text(f"✅ تم استبدال الملف `{file_name}` بنجاح.", message.chat.id, wait_msg.message_id, parse_mode='Markdown')

        # Restart if was running
        if was_running:
            bot.send_message(message.chat.id, f"🔄 جاري إعادة تشغيل `{file_name}`...", parse_mode='Markdown')
            if file_type == 'py':
                threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
            elif file_type == 'js':
                threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
            time.sleep(1.5)

        is_now_running = is_bot_running(script_owner_id, file_name)
        status_text = '🟢 يعمل' if is_now_running else '🔴 متوقف'

        try:
            bot.edit_message_text(
                f"⚙️ الإدارة لـ: `{file_name}` ({file_type}) للمستخدم `{script_owner_id}`\nالحالة: {status_text}",
                original_message.chat.id, original_message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_now_running),
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Could not update original control message after file update: {e}")

    except Exception as e:
        logger.error(f"Error in process_update_file for {script_owner_id}/{file_name}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ خطأ أثناء تحديث الملف: {e}")


def chtoken_callback(call):
    """User clicks 🔑 تغيير التوكن from file control or after upload"""
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id

        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ رفض الإذن.", show_alert=True)
            return

        bot.answer_callback_query(call.id)
        msg = bot.send_message(
            call.message.chat.id,
            f"🔑 أرسل التوكن الجديد اللي عاوز تحطه جوه `{file_name}`\n\n"
            f"⚠️ البوت هيتوقف ويتشغل تاني بالتوكن الجديد تلقائياً.\n"
            f"/cancel للإلغاء.",
            parse_mode='Markdown'
        )
        bot.register_next_step_handler(msg, process_chtoken_file,
            script_owner_id=script_owner_id, file_name=file_name,
            original_message=call.message)
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing chtoken callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "خطأ: بيانات غير صالحة.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in chtoken_callback: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "حدث خطأ.", show_alert=True)


def process_chtoken_file(message, script_owner_id, file_name, original_message):
    """Replace TOKEN inside the uploaded bot file"""
    requesting_user_id = message.from_user.id

    if message.text and message.text.strip().lower() == '/cancel':
        bot.reply_to(message, "❌ تم إلغاء تغيير التوكن.")
        return

    if not message.text:
        bot.reply_to(message, "⚠️ أرسل التوكن كنص أو /cancel.")
        msg = bot.send_message(message.chat.id, "🔑 أرسل التوكن الجديد أو /cancel.")
        bot.register_next_step_handler(msg, process_chtoken_file,
            script_owner_id=script_owner_id, file_name=file_name,
            original_message=original_message)
        return

    new_token = message.text.strip()

    # Validate token format
    if not re.match(r'^\d+:[A-Za-z0-9_-]{35,}$', new_token):
        bot.reply_to(message, "⚠️ التوكن غير صالح. تأكد من نسخه صح من BotFather.\n/cancel للإلغاء.")
        msg = bot.send_message(message.chat.id, "🔑 أرسل التوكن الجديد أو /cancel.")
        bot.register_next_step_handler(msg, process_chtoken_file,
            script_owner_id=script_owner_id, file_name=file_name,
            original_message=original_message)
        return

    try:
        # Delete the token message for security
        try: bot.delete_message(message.chat.id, message.message_id)
        except: pass

        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)

        if not os.path.exists(file_path):
            bot.reply_to(message, f"❌ الملف `{file_name}` مش موجود على الديسك. أعد رفعه.", parse_mode='Markdown')
            return

        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        # Try replacing TOKEN in Python files: TOKEN = '...' or TOKEN = "..."
        new_content = re.sub(
            r"(TOKEN\s*=\s*['\"])([^'\"]+)(['\"])",
            lambda m: f"{m.group(1)}{new_token}{m.group(3)}",
            content
        )
        # Also handle JS: const TOKEN = '...' or token: '...'
        if new_content == content:
            new_content = re.sub(
                r"((?:const|let|var)?\s*[Tt][Oo][Kk][Ee][Nn]\s*=\s*['\"])([^'\"]+)(['\"])",
                lambda m: f"{m.group(1)}{new_token}{m.group(3)}",
                content
            )

        if new_content == content:
            bot.send_message(message.chat.id,
                "⚠️ ما لقيتش متغير `TOKEN` في الملف.\n"
                "تأكد إن الملف فيه سطر زي:\n`TOKEN = 'توكن_قديم'`",
                parse_mode='Markdown')
            return

        with open(file_path, 'w', encoding='utf-8', errors='ignore') as f:
            f.write(new_content)

        logger.info(f"Token replaced in '{file_name}' for user {script_owner_id}")

        # Stop and restart if running
        script_key = f"{script_owner_id}_{file_name}"
        was_running = is_bot_running(script_owner_id, file_name)
        if was_running:
            process_info = bot_scripts.get(script_key)
            if process_info: kill_process_tree(process_info)
            if script_key in bot_scripts: del bot_scripts[script_key]
            time.sleep(1)

        bot.send_message(message.chat.id, f"✅ تم تغيير التوكن في `{file_name}` بنجاح!", parse_mode='Markdown')

        if was_running:
            file_info = next((f for f in user_files.get(script_owner_id, []) if f[0] == file_name), None)
            if file_info:
                file_type = file_info[1]
                bot.send_message(message.chat.id, f"🔄 جاري إعادة تشغيل `{file_name}`...", parse_mode='Markdown')
                if file_type == 'py':
                    threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
                elif file_type == 'js':
                    threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()

    except Exception as e:
        logger.error(f"Error replacing token in '{file_name}': {e}", exc_info=True)
        bot.send_message(message.chat.id, f"❌ خطأ أثناء تغيير التوكن: {e}")


def delete_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Delete: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ رفض الإذن.", show_alert=True); return

        user_files_list = user_files.get(script_owner_id, [])
        if not any(f[0] == file_name for f in user_files_list):
            bot.answer_callback_query(call.id, "⚠️ الملف غير موجود.", show_alert=True); check_files_callback(call); return

        bot.answer_callback_query(call.id, f"🗑️ حذف {file_name} للمستخدم {script_owner_id}...")
        script_key = f"{script_owner_id}_{file_name}"
        if is_bot_running(script_owner_id, file_name):
            logger.info(f"Delete: Stopping {script_key}...")
            process_info = bot_scripts.get(script_key)
            if process_info: kill_process_tree(process_info)
            if script_key in bot_scripts: del bot_scripts[script_key]
            time.sleep(0.5) 

        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)
        log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        deleted_disk = []
        if os.path.exists(file_path):
            try: os.remove(file_path); deleted_disk.append(file_name); logger.info(f"Deleted file: {file_path}")
            except OSError as e: logger.error(f"Error deleting {file_path}: {e}")
        if os.path.exists(log_path):
            try: os.remove(log_path); deleted_disk.append(os.path.basename(log_path)); logger.info(f"Deleted log: {log_path}")
            except OSError as e: logger.error(f"Error deleting log {log_path}: {e}")

        remove_user_file_db(script_owner_id, file_name)
        deleted_str = ", ".join(f"`{f}`" for f in deleted_disk) if deleted_disk else "الملفات المرتبطة"
        try:
            bot.edit_message_text(
                f"🗑️ السجل `{file_name}` (المستخدم `{script_owner_id}`) و {deleted_str} محذوف!",
                chat_id_for_reply, call.message.message_id, reply_markup=None, parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Error editing msg after delete: {e}")
            bot.send_message(chat_id_for_reply, f"🗑️ السجل `{file_name}` محذوف.", parse_mode='Markdown')
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing delete callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "خطأ: أمر حذف غير صالح.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in delete_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "خطأ في الحذف.", show_alert=True)

def logs_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Logs: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ رفض الإذن.", show_alert=True); return

        user_files_list = user_files.get(script_owner_id, [])
        if not any(f[0] == file_name for f in user_files_list):
            bot.answer_callback_query(call.id, "⚠️ الملف غير موجود.", show_alert=True); check_files_callback(call); return

        user_folder = get_user_folder(script_owner_id)
        log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        if not os.path.exists(log_path):
            bot.answer_callback_query(call.id, f"⚠️ لا توجد سجلات لـ '{file_name}'.", show_alert=True); return

        bot.answer_callback_query(call.id) 
        try:
            log_content = ""; file_size = os.path.getsize(log_path)
            max_log_kb = 100; max_tg_msg = 4096
            if file_size == 0: log_content = "(السجل فارغ)"
            elif file_size > max_log_kb * 1024:
                 with open(log_path, 'rb') as f: f.seek(-max_log_kb * 1024, os.SEEK_END); log_bytes = f.read()
                 log_content = log_bytes.decode('utf-8', errors='ignore')
                 log_content = f"(آخر {max_log_kb} كيلوبايت)\n...\n" + log_content
            else:
                 with open(log_path, 'r', encoding='utf-8', errors='ignore') as f: log_content = f.read()

            if len(log_content) > max_tg_msg:
                log_content = log_content[-max_tg_msg:]
                first_nl = log_content.find('\n')
                if first_nl != -1: log_content = "...\n" + log_content[first_nl+1:]
                else: log_content = "...\n" + log_content 
            if not log_content.strip(): log_content = "(لا يوجد محتوى مرئي)"

            bot.send_message(chat_id_for_reply, f"📜 السجلات لـ `{file_name}` (المستخدم `{script_owner_id}`):\n```\n{log_content}\n```", parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Error reading/sending log {log_path}: {e}", exc_info=True)
            bot.send_message(chat_id_for_reply, f"❌ خطأ في قراءة السجل لـ `{file_name}`.")
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing logs callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "خطأ: أمر عرض السجلات غير صالح.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in logs_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "خطأ في جلب السجلات.", show_alert=True)

def speed_callback(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    start_cb_ping_time = time.time() 
    try:
        bot.edit_message_text("🏃 اختبار السرعة...", chat_id, call.message.message_id)
        bot.send_chat_action(chat_id, 'typing') 
        response_time = round((time.time() - start_cb_ping_time) * 1000, 2)
        status = "🔓 مفتوح" if not bot_locked else "🔒 مقفل"
        # mode = "💰 Free Mode: ON" if free_mode else "💸 Free Mode: OFF" # Removed
        if user_id == OWNER_ID: user_level = "👑 المالك"
        elif user_id in admin_ids: user_level = "🛡️ الإدارة"
        elif user_id in user_subscriptions and user_subscriptions[user_id].get('expiry', datetime.min) > datetime.now(): user_level = "⭐ مميز"
        else: user_level = "🆓 مستخدم مجاني"
        speed_msg = (f"⚡ سرعة البوت وحالته:\n\n⏱️ وقت الاستجابة: {response_time} مللي ثانية\n"
                     f"🚦 حالة البوت: {status}\n"
                     # f"模式 Mode: {mode}\n" # Removed
                     f"👤 مستواك: {user_level}")
        bot.answer_callback_query(call.id) 
        bot.edit_message_text(speed_msg, chat_id, call.message.message_id, reply_markup=create_main_menu_inline(user_id))
    except Exception as e:
         logger.error(f"Error during speed test (cb): {e}", exc_info=True)
         bot.answer_callback_query(call.id, "خطأ في اختبار السرعة.", show_alert=True)
         try: bot.edit_message_text("〽️ القائمة الرئيسية", chat_id, call.message.message_id, reply_markup=create_main_menu_inline(user_id))
         except Exception: pass

def back_to_main_callback(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = str(file_limit) if file_limit != float('inf') else "غير محدود"
    expiry_info = ""
    if user_id == OWNER_ID: user_status = "👑 المالك"
    elif user_id in admin_ids: user_status = "🛡️ الإدارة"
    elif user_id in user_subscriptions:
        expiry_date = user_subscriptions[user_id].get('expiry')
        if expiry_date and expiry_date > datetime.now():
            user_status = "⭐ مميز"; days_left = (expiry_date - datetime.now()).days
            expiry_info = f"\n⏳ ينتهي الاشتراك بعد: {days_left} أيام"
        else: user_status = "🆓 مستخدم مجاني (انتهى الاشتراك)" # Will be cleaned up by welcome if not already
    else: user_status = "🆓 مستخدم مجاني"
    main_menu_text = (f"〽️ مرحباً بك مرة أخرى، {call.from_user.first_name}!\n\n🆔 المعرف: `{user_id}`\n"
                      f"🔰 الحالة: {user_status}{expiry_info}\n📁 الملفات: {current_files} / {limit_str}\n\n"
                      f"👇 استخدم الأزرار أو اكتب الأوامر.")
    try:
        bot.answer_callback_query(call.id)
        bot.edit_message_text(main_menu_text, chat_id, call.message.message_id,
                              reply_markup=create_main_menu_inline(user_id), parse_mode='Markdown')
    except telebot.apihelper.ApiTelegramException as e:
         if "message is not modified" in str(e): logger.warning("Msg not modified (back_to_main).")
         else: logger.error(f"API error on back_to_main: {e}")
    except Exception as e: logger.error(f"Error handling back_to_main: {e}", exc_info=True)

# --- Admin Callback Implementations (for Inline Buttons) ---
def subscription_management_callback(call):
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text("💳 إدارة الاشتراكات\nاختر العملية:",
                              call.message.chat.id, call.message.message_id, reply_markup=create_subscription_menu())
    except Exception as e: logger.error(f"Error showing sub menu: {e}")

def stats_callback(call): # Called by user and admin
    bot.answer_callback_query(call.id)
    # The logic is now inside _logic_statistics which determines what to show based on user_id
    # We need to pass a message-like object to _logic_statistics
    # For callbacks, call.message can be used.
    _logic_statistics(call.message) 
    # To update the inline keyboard after showing stats, we need to edit the message
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id,
                                      reply_markup=create_main_menu_inline(call.from_user.id))
    except Exception as e:
        logger.error(f"Error updating menu after stats_callback: {e}")


def lock_bot_callback(call):
    global bot_locked; bot_locked = True
    logger.warning(f"Bot locked by Admin {call.from_user.id}")
    bot.answer_callback_query(call.id, "🔒 البوت مقفل.")
    try: bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_main_menu_inline(call.from_user.id))
    except Exception as e: logger.error(f"Error updating menu (lock): {e}")

def unlock_bot_callback(call):
    global bot_locked; bot_locked = False
    logger.warning(f"Bot unlocked by Admin {call.from_user.id}")
    bot.answer_callback_query(call.id, "🔓 البوت مفتوح.")
    try: bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_main_menu_inline(call.from_user.id))
    except Exception as e: logger.error(f"Error updating menu (unlock): {e}")

# def toggle_free_mode_callback(call): # Removed
#     pass

def run_all_scripts_callback(call): # Added
    _logic_run_all_scripts(call) # Pass the call object


def broadcast_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "📢 أرسل الرسالة لبثها.\n/cancel للإلغاء.")
    bot.register_next_step_handler(msg, process_broadcast_message)

def process_broadcast_message(message):
    user_id = message.from_user.id
    if user_id not in admin_ids: bot.reply_to(message, "⚠️ غير مصرح لك."); return
    if message.text and message.text.lower() == '/cancel': bot.reply_to(message, "تم إلغاء البث."); return

    broadcast_content = message.text # Can also handle photos, videos etc. if message.content_type is checked
    if not broadcast_content and not (message.photo or message.video or message.document or message.sticker or message.voice or message.audio): # If no text and no other media
         bot.reply_to(message, "⚠️ لا يمكن بث رسالة فارغة. أرسل نصاً أو وسائط، أو /cancel.")
         msg = bot.send_message(message.chat.id, "📢 أرسل رسالة البث أو /cancel.")
         bot.register_next_step_handler(msg, process_broadcast_message)
         return

    target_count = len(active_users)
    markup = types.InlineKeyboardMarkup()
    markup.row(types.InlineKeyboardButton("✅ تأكيد وإرسال", callback_data=f"confirm_broadcast_{message.message_id}"),
               types.InlineKeyboardButton("❌ إلغاء", callback_data="cancel_broadcast"))

    preview_text = broadcast_content[:1000].strip() if broadcast_content else "(رسالة وسائط)"
    bot.reply_to(message, f"⚠️ تأكيد البث:\n\n```\n{preview_text}\n```\n" 
                          f"إلى **{target_count}** مستخدم. هل أنت متأكد؟", reply_markup=markup, parse_mode='Markdown')

def handle_confirm_broadcast(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    if user_id not in admin_ids: bot.answer_callback_query(call.id, "⚠️ للإدارة فقط.", show_alert=True); return
    try:
        original_message = call.message.reply_to_message
        if not original_message: raise ValueError("لم يتمكن من جلب الرسالة الأصلية.")

        # Check content type and get content
        broadcast_text = None
        broadcast_photo_id = None
        broadcast_video_id = None
        # Add other types as needed: document, sticker, voice, audio

        if original_message.text:
            broadcast_text = original_message.text
        elif original_message.photo:
            broadcast_photo_id = original_message.photo[-1].file_id # Get highest quality
        elif original_message.video:
            broadcast_video_id = original_message.video.file_id
        # Add more elif for other content types
        else:
            raise ValueError("الرسالة لا تحتوي على نص أو وسائط مدعومة للبث.")

        bot.answer_callback_query(call.id, "🚀 بدء البث...")
        bot.edit_message_text(f"📢 بث إلى {len(active_users)} مستخدم...",
                              chat_id, call.message.message_id, reply_markup=None)
        # Pass all potential content types to execute_broadcast
        thread = threading.Thread(target=execute_broadcast, args=(
            broadcast_text, broadcast_photo_id, broadcast_video_id, 
            original_message.caption if (broadcast_photo_id or broadcast_video_id) else None, # Pass caption
            chat_id))
        thread.start()
    except ValueError as ve: 
        logger.error(f"Error retrieving msg for broadcast confirm: {ve}")
        bot.edit_message_text(f"❌ خطأ في بدء البث: {ve}", chat_id, call.message.message_id, reply_markup=None)
    except Exception as e:
        logger.error(f"Error in handle_confirm_broadcast: {e}", exc_info=True)
        bot.edit_message_text("❌ خطأ غير متوقع أثناء تأكيد البث.", chat_id, call.message.message_id, reply_markup=None)

def handle_cancel_broadcast(call):
    bot.answer_callback_query(call.id, "تم إلغاء البث.")
    bot.delete_message(call.message.chat.id, call.message.message_id)
    # Optionally delete the original message too if call.message.reply_to_message exists
    if call.message.reply_to_message:
        try: bot.delete_message(call.message.chat.id, call.message.reply_to_message.message_id)
        except: pass


def execute_broadcast(broadcast_text, photo_id, video_id, caption, admin_chat_id):
    sent_count = 0; failed_count = 0; blocked_count = 0
    start_exec_time = time.time() 
    users_to_broadcast = list(active_users); total_users = len(users_to_broadcast)
    logger.info(f"Executing broadcast to {total_users} users.")
    batch_size = 25; delay_batches = 1.5

    for i, user_id_bc in enumerate(users_to_broadcast): # Renamed
        try:
            if broadcast_text:
                bot.send_message(user_id_bc, broadcast_text, parse_mode='Markdown')
            elif photo_id:
                bot.send_photo(user_id_bc, photo_id, caption=caption, parse_mode='Markdown' if caption else None)
            elif video_id:
                bot.send_video(user_id_bc, video_id, caption=caption, parse_mode='Markdown' if caption else None)
            # Add other send methods for other types
            sent_count += 1
        except telebot.apihelper.ApiTelegramException as e:
            err_desc = str(e).lower()
            if any(s in err_desc for s in ["bot was blocked", "user is deactivated", "chat not found", "kicked from", "restricted"]): 
                logger.warning(f"Broadcast failed to {user_id_bc}: User blocked/inactive.")
                blocked_count += 1
            elif "flood control" in err_desc or "too many requests" in err_desc:
                retry_after = 5; match = re.search(r"retry after (\d+)", err_desc)
                if match: retry_after = int(match.group(1)) + 1 
                logger.warning(f"Flood control. Sleeping {retry_after}s...")
                time.sleep(retry_after)
                try: # Retry once
                    if broadcast_text: bot.send_message(user_id_bc, broadcast_text, parse_mode='Markdown')
                    elif photo_id: bot.send_photo(user_id_bc, photo_id, caption=caption, parse_mode='Markdown' if caption else None)
                    elif video_id: bot.send_video(user_id_bc, video_id, caption=caption, parse_mode='Markdown' if caption else None)
                    sent_count += 1
                except Exception as e_retry: logger.error(f"Broadcast retry failed to {user_id_bc}: {e_retry}"); failed_count +=1
            else: logger.error(f"Broadcast failed to {user_id_bc}: {e}"); failed_count += 1
        except Exception as e: logger.error(f"Unexpected error broadcasting to {user_id_bc}: {e}"); failed_count += 1

        if (i + 1) % batch_size == 0 and i < total_users - 1:
            logger.info(f"Broadcast batch {i//batch_size + 1} sent. Sleeping {delay_batches}s...")
            time.sleep(delay_batches)
        elif i % 5 == 0: time.sleep(0.2) 

    duration = round(time.time() - start_exec_time, 2)
    result_msg = (f"📢 انتهى البث!\n\n✅ مرسل: {sent_count}\n❌ فشل: {failed_count}\n"
                  f"🚫 محظور/غير نشط: {blocked_count}\n👥 الأهداف: {total_users}\n⏱️ المدة: {duration} ثانية")
    logger.info(result_msg)
    try: bot.send_message(admin_chat_id, result_msg)
    except Exception as e: logger.error(f"Failed to send broadcast result to admin {admin_chat_id}: {e}")

def refresh_file_callback(call):
    """Admin clicks 🔄 تحديث الملف from admin panel - updates the main bot file itself"""
    user_id = call.from_user.id
    if user_id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ للأدمن فقط.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    msg = bot.send_message(
        call.message.chat.id,
        "📥 *تحديث البوت الرئيسي*\n\n"
        "أرسل ملف `.py` الجديد للبوت وسيتم استبداله وإعادة التشغيل تلقائياً.\n"
        "أو أرسل /cancel للإلغاء.",
        parse_mode='Markdown'
    )
    bot.register_next_step_handler(msg, process_self_update, original_message=call.message)

def process_self_update(message, original_message):
    """Handle the new bot file sent by admin to replace the main bot script"""
    user_id = message.from_user.id
    if user_id not in admin_ids:
        bot.reply_to(message, "⚠️ غير مصرح لك.")
        return

    if message.text and message.text.strip().lower() == '/cancel':
        bot.reply_to(message, "❌ تم إلغاء التحديث.")
        return

    if not message.document:
        bot.reply_to(message, "⚠️ أرسل ملف `.py` فقط أو /cancel للإلغاء.", parse_mode='Markdown')
        msg = bot.send_message(message.chat.id, "📥 أرسل الملف الجديد أو /cancel.")
        bot.register_next_step_handler(msg, process_self_update, original_message=original_message)
        return

    doc = message.document
    sent_ext = os.path.splitext(doc.file_name or '')[1].lower()
    if sent_ext != '.py':
        bot.reply_to(message, f"⚠️ الملف يجب أن يكون `.py` وليس `{sent_ext}`.\nأو /cancel للإلغاء.", parse_mode='Markdown')
        msg = bot.send_message(message.chat.id, "📥 أرسل ملف `.py` أو /cancel.")
        bot.register_next_step_handler(msg, process_self_update, original_message=original_message)
        return

    try:
        wait_msg = bot.reply_to(message, "⏳ جاري تحميل الملف الجديد...")
        file_info_tg = bot.get_file(doc.file_id)
        new_content = bot.download_file(file_info_tg.file_path)

        # Path of the current running bot script
        bot_script_path = os.path.abspath(__file__)

        # Write new file content
        with open(bot_script_path, 'wb') as f:
            f.write(new_content)

        logger.info(f"Main bot file updated by admin {user_id}. File: {bot_script_path}")
        bot.edit_message_text("✅ تم استبدال ملف البوت بنجاح!\n🔄 جاري إعادة التشغيل...", message.chat.id, wait_msg.message_id)

        # Restart the bot process
        def do_restart():
            time.sleep(2)
            os.execv(sys.executable, [sys.executable, bot_script_path])

        threading.Thread(target=do_restart, daemon=True).start()

    except Exception as e:
        logger.error(f"Error in process_self_update by admin {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ خطأ أثناء التحديث: {e}")

def lock_bot_panel_callback(call):
    """Admin clicks 🔒 قفل البوت from admin panel"""
    global bot_locked
    if bot_locked:
        bot.answer_callback_query(call.id, "⚠️ البوت مقفل بالفعل.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    bot_locked = True
    logger.warning(f"البوت مقفل 🔒 من قبل الإدارة {call.from_user.id} عبر لوحة الأدمن.")
    try:
        bot.edit_message_text("🔒 تم قفل البوت بنجاح.", call.message.chat.id, call.message.message_id, reply_markup=create_admin_panel())
    except Exception:
        bot.send_message(call.message.chat.id, "🔒 تم قفل البوت بنجاح.", reply_markup=create_admin_panel())

def unlock_bot_panel_callback(call):
    """Admin clicks 🔓 فتح البوت from admin panel"""
    global bot_locked
    if not bot_locked:
        bot.answer_callback_query(call.id, "⚠️ البوت مفتوح بالفعل.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    bot_locked = False
    logger.warning(f"البوت مفتوح 🔓 من قبل الإدارة {call.from_user.id} عبر لوحة الأدمن.")
    try:
        bot.edit_message_text("🔓 تم فتح البوت بنجاح.", call.message.chat.id, call.message.message_id, reply_markup=create_admin_panel())
    except Exception:
        bot.send_message(call.message.chat.id, "🔓 تم فتح البوت بنجاح.", reply_markup=create_admin_panel())

def run_all_scripts_panel_callback(call):
    """Admin clicks 🟢 تشغيل كل الأكواد from admin panel"""
    bot.answer_callback_query(call.id)
    _logic_run_all_scripts(call)

def ban_user_panel_callback(call):
    """Admin clicks 🚫 حظر مستخدم from admin panel"""
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "🚫 أرسل معرف المستخدم لحظره.\n/cancel للإلغاء.")
    bot.register_next_step_handler(msg, _process_ban_user_from_panel)

def _process_ban_user_from_panel(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ غير مصرح لك."); return
    if message.text.lower() == '/cancel':
        bot.reply_to(message, "تم إلغاء الحظر.", reply_markup=create_admin_panel()); return
    try:
        target_id = int(message.text.strip())
        if target_id == OWNER_ID:
            bot.reply_to(message, "⚠️ لا يمكن حظر المالك."); return
        if target_id in admin_ids:
            bot.reply_to(message, "⚠️ لا يمكن حظر الأدمن."); return
        if target_id in banned_users:
            bot.reply_to(message, f"⚠️ المستخدم `{target_id}` محظور بالفعل.", parse_mode='Markdown'); return
        if ban_user_db(target_id):
            bot.reply_to(message, f"✅ تم حظر المستخدم `{target_id}` بنجاح.", parse_mode='Markdown', reply_markup=create_admin_panel())
            try: bot.send_message(target_id, "🚫 تم حظرك من استخدام هذا البوت.")
            except: pass
        else:
            bot.reply_to(message, "❌ فشل في الحظر.", reply_markup=create_admin_panel())
    except ValueError:
        bot.reply_to(message, "⚠️ معرف غير صالح. أرسل رقماً أو /cancel.")
        msg = bot.send_message(message.chat.id, "🚫 أرسل معرف المستخدم أو /cancel.")
        bot.register_next_step_handler(msg, _process_ban_user_from_panel)

def unban_user_panel_callback(call):
    """Admin clicks ✅ فك حظر مستخدم from admin panel"""
    bot.answer_callback_query(call.id)
    if not banned_users:
        bot.send_message(call.message.chat.id, "✅ لا يوجد مستخدمون محظورون.", reply_markup=create_admin_panel()); return
    banned_list = "\n".join(f"• `{uid}`" for uid in sorted(banned_users))
    msg = bot.send_message(call.message.chat.id, f"✅ أرسل معرف المستخدم لفك حظره:\n\n{banned_list}\n\n/cancel للإلغاء.", parse_mode='Markdown')
    bot.register_next_step_handler(msg, _process_unban_user_from_panel)

def _process_unban_user_from_panel(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ غير مصرح لك."); return
    if message.text.lower() == '/cancel':
        bot.reply_to(message, "تم إلغاء فك الحظر.", reply_markup=create_admin_panel()); return
    try:
        target_id = int(message.text.strip())
        if target_id not in banned_users:
            bot.reply_to(message, f"⚠️ المستخدم `{target_id}` ليس محظوراً.", parse_mode='Markdown'); return
        if unban_user_db(target_id):
            bot.reply_to(message, f"✅ تم فك حظر المستخدم `{target_id}`.", parse_mode='Markdown', reply_markup=create_admin_panel())
            try: bot.send_message(target_id, "✅ تم فك حظرك. يمكنك استخدام البوت الآن.")
            except: pass
        else:
            bot.reply_to(message, "❌ فشل في فك الحظر.", reply_markup=create_admin_panel())
    except ValueError:
        bot.reply_to(message, "⚠️ معرف غير صالح. أرسل رقماً أو /cancel.")
        msg = bot.send_message(message.chat.id, "✅ أرسل معرف المستخدم أو /cancel.")
        bot.register_next_step_handler(msg, _process_unban_user_from_panel)

def change_token_callback(call):
    """Owner clicks 🔑 تغيير التوكن from admin panel"""
    bot.answer_callback_query(call.id)
    msg = bot.send_message(
        call.message.chat.id,
        "🔑 أرسل التوكن الجديد للبوت.\n\n⚠️ التوكن هيتحفظ في الذاكرة فقط حتى إعادة التشغيل.\nلتغيير دائم، حدّث متغير `TOKEN` في Railway.\n\n/cancel للإلغاء."
    )
    bot.register_next_step_handler(msg, process_change_token)

def process_change_token(message):
    if message.from_user.id != OWNER_ID:
        bot.reply_to(message, "⚠️ للمالك فقط."); return
    if message.text.strip().lower() == '/cancel':
        bot.reply_to(message, "❌ تم إلغاء تغيير التوكن."); return
    new_token = message.text.strip()
    if not re.match(r'^\d+:[A-Za-z0-9_-]{35,}$', new_token):
        bot.reply_to(message, "⚠️ التوكن غير صالح. تأكد من نسخه صح من BotFather.\n/cancel للإلغاء.")
        msg = bot.send_message(message.chat.id, "🔑 أرسل التوكن الجديد أو /cancel.")
        bot.register_next_step_handler(msg, process_change_token)
        return
    global TOKEN
    TOKEN = new_token
    bot.token = new_token
    logger.warning(f"Token changed by Owner {message.from_user.id}")
    try:
        # Delete the token message for security
        bot.delete_message(message.chat.id, message.message_id)
    except: pass
    bot.send_message(
        message.chat.id,
        "✅ تم تغيير التوكن في الذاكرة.\n\n"
        "⚠️ لتغيير دائم بعد إعادة التشغيل، روح:\n"
        "Railway → Variables → `TOKEN` → غيّر القيمة.",
    )

def admin_panel_callback(call):
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text("👑 لوحة الإدارة\nإدارة الإداريين (قد تكون بعض إجراءات المالك مقيدة).",
                              call.message.chat.id, call.message.message_id, reply_markup=create_admin_panel())
    except Exception as e: logger.error(f"Error showing admin panel: {e}")

def add_admin_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "👑 أدخل معرف المستخدم لترقيته إلى إداري.\n/cancel للإلغاء.")
    bot.register_next_step_handler(msg, process_add_admin_id)
def process_add_admin_id(message):
    owner_id_check = message.from_user.id 
    if owner_id_check != OWNER_ID: bot.reply_to(message, "⚠️ للمالك فقط."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "تم إلغاء ترقية الإداري."); return
    try:
        new_admin_id = int(message.text.strip())
        if new_admin_id <= 0: raise ValueError("ID must be positive")
        if new_admin_id == OWNER_ID: bot.reply_to(message, "⚠️ المالك هو المالك بالفعل."); return
        if new_admin_id in admin_ids: bot.reply_to(message, f"⚠️ المستخدم `{new_admin_id}` إداري بالفعل."); return
        add_admin_db(new_admin_id) 
        logger.warning(f"Admin {new_admin_id} added by Owner {owner_id_check}.")
        bot.reply_to(message, f"✅ تم ترقية المستخدم `{new_admin_id}` إلى إداري.")
        try: bot.send_message(new_admin_id, "🎉 تهانينا! أنت الآن إداري.")
        except Exception as e: logger.error(f"Failed to notify new admin {new_admin_id}: {e}")
    except ValueError:
        bot.reply_to(message, "⚠️ معرف غير صالح. أرسل معرفاً رقمياً أو /cancel.")
        msg = bot.send_message(message.chat.id, "👑 أدخل معرف المستخدم للترقية أو /cancel.")
        bot.register_next_step_handler(msg, process_add_admin_id)
    except Exception as e: logger.error(f"Error processing add admin: {e}", exc_info=True); bot.reply_to(message, "خطأ.")

def remove_admin_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "👑 أدخل معرف الإداري لإزالته.\n/cancel للإلغاء.")
    bot.register_next_step_handler(msg, process_remove_admin_id)

def process_remove_admin_id(message):
    owner_id_check = message.from_user.id
    if owner_id_check != OWNER_ID: bot.reply_to(message, "⚠️ للمالك فقط."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "تم إلغاء إزالة الإداري."); return
    try:
        admin_id_remove = int(message.text.strip()) # Renamed
        if admin_id_remove <= 0: raise ValueError("ID must be positive")
        if admin_id_remove == OWNER_ID: bot.reply_to(message, "⚠️ المالك لا يمكنه إزالة نفسه."); return
        if admin_id_remove not in admin_ids: bot.reply_to(message, f"⚠️ المستخدم `{admin_id_remove}` ليس إدارياً."); return
        if remove_admin_db(admin_id_remove): 
            logger.warning(f"Admin {admin_id_remove} removed by Owner {owner_id_check}.")
            bot.reply_to(message, f"✅ تم إزالة الإداري `{admin_id_remove}`.")
            try: bot.send_message(admin_id_remove, "ℹ️ لم تعد إدارياً.")
            except Exception as e: logger.error(f"Failed to notify removed admin {admin_id_remove}: {e}")
        else: bot.reply_to(message, f"❌ فشل في إزالة الإداري `{admin_id_remove}`. تحقق من السجلات.")
    except ValueError:
        bot.reply_to(message, "⚠️ معرف غير صالح. أرسل معرفاً رقمياً أو /cancel.")
        msg = bot.send_message(message.chat.id, "👑 أدخل معرف الإداري للإزالة أو /cancel.")
        bot.register_next_step_handler(msg, process_remove_admin_id)
    except Exception as e: logger.error(f"Error processing remove admin: {e}", exc_info=True); bot.reply_to(message, "خطأ.")

def list_admins_callback(call):
    bot.answer_callback_query(call.id)
    try:
        admin_list_str = "\n".join(f"- `{aid}` {'(المالك)' if aid == OWNER_ID else ''}" for aid in sorted(list(admin_ids)))
        if not admin_list_str: admin_list_str = "(لا توجد إداريين/مالك محددين!)"
        bot.edit_message_text(f"👑 الإداريون الحاليون:\n\n{admin_list_str}", call.message.chat.id,
                              call.message.message_id, reply_markup=create_admin_panel(), parse_mode='Markdown')
    except Exception as e: logger.error(f"Error listing admins: {e}")

def add_subscription_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "💳 أدخل معرف المستخدم وعدد الأيام (مثال: `12345678 30`).\n/cancel للإلغاء.")
    bot.register_next_step_handler(msg, process_add_subscription_details)

def process_add_subscription_details(message):
    admin_id_check = message.from_user.id 
    if admin_id_check not in admin_ids: bot.reply_to(message, "⚠️ غير مصرح لك."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "تم إلغاء إضافة الاشتراك."); return
    try:
        parts = message.text.split();
        if len(parts) != 2: raise ValueError("Incorrect format")
        sub_user_id = int(parts[0].strip()); days = int(parts[1].strip())
        if sub_user_id <= 0 or days <= 0: raise ValueError("User ID/days must be positive")

        current_expiry = user_subscriptions.get(sub_user_id, {}).get('expiry')
        start_date_new_sub = datetime.now() # Renamed
        if current_expiry and current_expiry > start_date_new_sub: start_date_new_sub = current_expiry
        new_expiry = start_date_new_sub + timedelta(days=days)
        save_subscription(sub_user_id, new_expiry)

        logger.info(f"Sub for {sub_user_id} by admin {admin_id_check}. Expiry: {new_expiry:%Y-%m-%d}")
        bot.reply_to(message, f"✅ اشتراك لـ `{sub_user_id}` لـ {days} أيام.\nانتهاء جديد: {new_expiry:%Y-%m-%d}")
        try: bot.send_message(sub_user_id, f"🎉 تم تفعيل/تمديد الاشتراك لـ {days} أيام! ينتهي: {new_expiry:%Y-%m-%d}.")
        except Exception as e: logger.error(f"Failed to notify {sub_user_id} of new sub: {e}")
    except ValueError as e:
        bot.reply_to(message, f"⚠️ غير صالح: {e}. الصيغة: `المعرف الأيام` أو /cancel.")
        msg = bot.send_message(message.chat.id, "💳 أدخل معرف المستخدم وعدد الأيام، أو /cancel.")
        bot.register_next_step_handler(msg, process_add_subscription_details)
    except Exception as e: logger.error(f"Error processing add sub: {e}", exc_info=True); bot.reply_to(message, "خطأ.")

def remove_subscription_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "💳 أدخل معرف المستخدم لإزالة الاشتراك.\n/cancel للإلغاء.")
    bot.register_next_step_handler(msg, process_remove_subscription_id)

def process_remove_subscription_id(message):
    admin_id_check = message.from_user.id
    if admin_id_check not in admin_ids: bot.reply_to(message, "⚠️ غير مصرح لك."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "تم إلغاء إزالة الاشتراك."); return
    try:
        sub_user_id_remove = int(message.text.strip()) # Renamed
        if sub_user_id_remove <= 0: raise ValueError("ID must be positive")
        if sub_user_id_remove not in user_subscriptions:
            bot.reply_to(message, f"⚠️ المستخدم `{sub_user_id_remove}` لا يوجد اشتراك نشط في الذاكرة."); return
        remove_subscription_db(sub_user_id_remove) 
        logger.warning(f"Sub removed for {sub_user_id_remove} by admin {admin_id_check}.")
        bot.reply_to(message, f"✅ تم إزالة الاشتراك لـ `{sub_user_id_remove}`.")
        try: bot.send_message(sub_user_id_remove, "ℹ️ تم إزالة اشتراكك من قبل الإدارة.")
        except Exception as e: logger.error(f"Failed to notify {sub_user_id_remove} of sub removal: {e}")
    except ValueError:
        bot.reply_to(message, "⚠️ معرف غير صالح. أرسل معرفاً رقمياً أو /cancel.")
        msg = bot.send_message(message.chat.id, "💳 أدخل معرف المستخدم لإزالة الاشتراك منه، أو /cancel.")
        bot.register_next_step_handler(msg, process_remove_subscription_id)
    except Exception as e: logger.error(f"Error processing remove sub: {e}", exc_info=True); bot.reply_to(message, "خطأ.")

def check_subscription_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "💳 أدخل معرف المستخدم لفحص الاشتراك.\n/cancel للإلغاء.")
    bot.register_next_step_handler(msg, process_check_subscription_id)

def process_check_subscription_id(message):
    admin_id_check = message.from_user.id
    if admin_id_check not in admin_ids: bot.reply_to(message, "⚠️ غير مصرح لك."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "تم إلغاء فحص الاشتراك."); return
    try:
        sub_user_id_check = int(message.text.strip()) # Renamed
        if sub_user_id_check <= 0: raise ValueError("ID must be positive")
        if sub_user_id_check in user_subscriptions:
            expiry_dt = user_subscriptions[sub_user_id_check].get('expiry')
            if expiry_dt:
                if expiry_dt > datetime.now():
                    days_left = (expiry_dt - datetime.now()).days
                    bot.reply_to(message, f"✅ اشتراك نشط للمستخدم `{sub_user_id_check}`.\nينتهي: {expiry_dt:%Y-%m-%d %H:%M:%S} ({days_left} أيام متبقية).")
                else:
                    bot.reply_to(message, f"⚠️ اشتراك منتهي للمستخدم `{sub_user_id_check}` (في: {expiry_dt:%Y-%m-%d %H:%M:%S}).")
                    remove_subscription_db(sub_user_id_check) # Clean up
            else: bot.reply_to(message, f"⚠️ المستخدم `{sub_user_id_check}` في قائمة الاشتراكات، لكن تاريخ الانتهاء مفقود. أعد الإضافة إذا لزم الأمر.")
        else: bot.reply_to(message, f"ℹ️ لا يوجد سجل اشتراك نشط للمستخدم `{sub_user_id_check}`.")
    except ValueError:
        bot.reply_to(message, "⚠️ معرف غير صالح. أرسل معرفاً رقمياً أو /cancel.")
        msg = bot.send_message(message.chat.id, "💳 أدخل معرف المستخدم للفحص، أو /cancel.")
        bot.register_next_step_handler(msg, process_check_subscription_id)
    except Exception as e: logger.error(f"Error processing check sub: {e}", exc_info=True); bot.reply_to(message, "خطأ.")

# --- End Callback Query Handlers ---

# --- Cleanup Function ---
def cleanup():
    logger.warning("Shutdown. Cleaning up processes...")
    script_keys_to_stop = list(bot_scripts.keys()) 
    if not script_keys_to_stop: logger.info("No scripts running. Exiting."); return
    logger.info(f"Stopping {len(script_keys_to_stop)} scripts...")
    for key in script_keys_to_stop:
        if key in bot_scripts: logger.info(f"Stopping: {key}"); kill_process_tree(bot_scripts[key])
        else: logger.info(f"Script {key} already removed.")
    logger.warning("Cleanup finished.")
atexit.register(cleanup)

# --- Main Execution ---
if __name__ == '__main__':
    logger.info("="*40 + "\n🤖 Bot Starting Up...\n" + f"🐍 Python: {sys.version.split()[0]}\n" +
                f"🔧 Base Dir: {BASE_DIR}\n📁 Upload Dir: {UPLOAD_BOTS_DIR}\n" +
                f"📊 Data Dir: {IROTECH_DIR}\n🔑 Owner ID: {OWNER_ID}\n🛡️ Admins: {admin_ids}\n" + "="*40)
    keep_alive()
    print("\n" + "="*45)
    print("✅  البوت يعمل بنجاح!")
    print(f"👑  المالك: {YOUR_USERNAME}  |  ID: {OWNER_ID}")
    print(f"🤖  التوكن: {TOKEN[:20]}...")
    print("="*45 + "\n")
    logger.info("🚀 Starting polling...")
    while True:
        try:
            bot.infinity_polling(logger_level=logging.INFO, timeout=60, long_polling_timeout=30)
        except requests.exceptions.ReadTimeout: logger.warning("Polling ReadTimeout. Restarting in 5s..."); time.sleep(5)
        except requests.exceptions.ConnectionError as ce: logger.error(f"Polling ConnectionError: {ce}. Retrying in 15s..."); time.sleep(15)
        except Exception as e:
            logger.critical(f"💥 Unrecoverable polling error: {e}", exc_info=True)
            logger.info("Restarting polling in 30s due to critical error..."); time.sleep(30)
        finally: logger.warning("Polling attempt finished. Will restart if in loop."); time.sleep(1)
