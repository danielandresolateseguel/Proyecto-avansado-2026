import secrets
from flask import Blueprint, request, jsonify, session
from werkzeug.security import check_password_hash, generate_password_hash
from app.database import get_db, is_postgres
from app.utils import is_authed, check_csrf, get_csrf_token

bp = Blueprint('auth', __name__, url_prefix='/api')

def ensure_master_users_table(db, cur):
    if is_postgres():
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS master_users (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
    else:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS master_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
    db.commit()

@bp.route('/auth/login', methods=['POST'])
def auth_login():
    payload = request.get_json(silent=True) or {}
    username = str(payload.get('username') or '')
    password = str(payload.get('password') or '')
    tenant_slug = str(payload.get('tenant_slug') or '')
    if not username or not password or not tenant_slug:
        return jsonify({'error': 'credenciales incompletas'}), 400
    
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT password_hash FROM admin_users WHERE tenant_slug = ? AND username = ?", (tenant_slug, username))
    row = cur.fetchone()
    
    if not row or not check_password_hash(row[0], password):
        return jsonify({'error': 'usuario o contraseña inválidos'}), 401
    
    session['admin_auth'] = True
    session['admin_user'] = username
    session['tenant_slug'] = tenant_slug
    session['csrf_token'] = secrets.token_urlsafe(32)
    return jsonify({'ok': True, 'tenant_slug': tenant_slug})

@bp.route('/auth/login_dev', methods=['POST'])
def auth_login_dev():
    payload = request.get_json(silent=True) or {}
    u = str(payload.get('username') or '')
    t = str(payload.get('tenant_slug') or '')
    session['admin_auth'] = True
    session['admin_user'] = u or 'admin'
    if t:
        session['tenant_slug'] = t
    session['csrf_token'] = secrets.token_urlsafe(32)
    return jsonify({'ok': True, 'dev': True, 'tenant_slug': t or None})

@bp.route('/auth/logout', methods=['POST'])
def auth_logout():
    session.clear()
    return jsonify({'ok': True})

@bp.route('/auth/me', methods=['GET'])
def auth_me():
    return jsonify({'authenticated': is_authed(), 'user': session.get('admin_user') or '', 'tenant_slug': session.get('tenant_slug') or ''})

@bp.route('/auth/csrf', methods=['GET'])
def auth_csrf():
    # Allow fetching CSRF token even if not authenticated (for login page, etc)
    return jsonify({'token': get_csrf_token()})

@bp.route('/auth/master_status', methods=['GET'])
def master_status():
    return jsonify({'authenticated': bool(session.get('master_auth')), 'user': session.get('admin_user') or ''})

@bp.route('/auth/master_bootstrap', methods=['POST'])
def master_bootstrap():
    # Allows creating the first master user if none exists
    payload = request.get_json(silent=True) or {}
    username = (payload.get('username') or '').strip()
    password = str(payload.get('password') or '')
    if not username or not password:
        return jsonify({'error': 'datos incompletos'}), 400
    db = get_db()
    cur = db.cursor()
    try:
        ensure_master_users_table(db, cur)
    except Exception:
        return jsonify({'error': 'base de datos no disponible'}), 503

    try:
        cur.execute("SELECT COUNT(*) FROM master_users")
        row = cur.fetchone()
    except Exception:
        return jsonify({'error': 'base de datos no disponible'}), 503
    count = int(row[0]) if row else 0
    if count > 0:
        return jsonify({'error': 'ya existe un usuario master'}), 409
    ph = generate_password_hash(password)
    import datetime
    try:
        if is_postgres():
            cur.execute(
                "INSERT INTO master_users (username, password_hash, created_at) VALUES (%s, %s, %s)",
                (username, ph, datetime.datetime.utcnow().isoformat())
            )
        else:
            cur.execute(
                "INSERT INTO master_users (username, password_hash, created_at) VALUES (?, ?, ?)",
                (username, ph, datetime.datetime.utcnow().isoformat())
            )
        db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return jsonify({'error': 'base de datos no disponible'}), 503
    return jsonify({'ok': True, 'username': username})

@bp.route('/auth/master_login', methods=['POST'])
def master_login():
    payload = request.get_json(silent=True) or {}
    username = (payload.get('username') or '').strip()
    password = str(payload.get('password') or '')
    if not username or not password:
        return jsonify({'error': 'datos incompletos'}), 400
    db = get_db()
    cur = db.cursor()
    try:
        ensure_master_users_table(db, cur)
    except Exception:
        return jsonify({'error': 'base de datos no disponible'}), 503

    try:
        if is_postgres():
            cur.execute("SELECT password_hash FROM master_users WHERE username = %s", (username,))
        else:
            cur.execute("SELECT password_hash FROM master_users WHERE username = ?", (username,))
        row = cur.fetchone()
    except Exception:
        return jsonify({'error': 'base de datos no disponible'}), 503
    if not row or not check_password_hash(row[0], password):
        return jsonify({'error': 'usuario o contraseña inválidos'}), 401
    import secrets as _secrets
    session['master_auth'] = True
    # Also set admin_auth to reuse existing protections for create_demo
    session['admin_auth'] = True
    session['admin_user'] = username
    session['csrf_token'] = _secrets.token_urlsafe(32)
    return jsonify({'ok': True, 'user': username})

@bp.route('/auth/master_logout', methods=['POST'])
def master_logout():
    session.clear()
    return jsonify({'ok': True})

@bp.route('/admin_users', methods=['GET'])
def admin_users_list():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    tenant_slug = request.args.get('tenant_slug') or session.get('tenant_slug') or ''
    if session.get('tenant_slug') and tenant_slug and session.get('tenant_slug') != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT username FROM admin_users WHERE tenant_slug = ? ORDER BY username ASC", (tenant_slug,))
    rows = cur.fetchall()
    
    return jsonify({'tenant_slug': tenant_slug, 'users': [r[0] for r in rows]})

@bp.route('/admin_users', methods=['POST'])
def admin_users_create():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    payload = request.get_json(silent=True) or {}
    username = str(payload.get('username') or '').strip()
    password = str(payload.get('password') or '')
    tenant_slug = str(payload.get('tenant_slug') or session.get('tenant_slug') or '').strip()
    if not username or not password or not tenant_slug:
        return jsonify({'error': 'datos incompletos'}), 400
    if session.get('tenant_slug') and session.get('tenant_slug') != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT 1 FROM admin_users WHERE tenant_slug = ? AND username = ?", (tenant_slug, username))
    exists = cur.fetchone()
    if exists:
        return jsonify({'error': 'usuario ya existe'}), 409
    
    ph = generate_password_hash(password)
    cur.execute("INSERT INTO admin_users (tenant_slug, username, password_hash) VALUES (?, ?, ?)", (tenant_slug, username, ph))
    db.commit()
    
    return jsonify({'ok': True, 'username': username, 'tenant_slug': tenant_slug})
