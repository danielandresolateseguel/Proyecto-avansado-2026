import secrets
import os
from flask import Blueprint, request, jsonify, session
from werkzeug.security import check_password_hash, generate_password_hash
from app.database import get_db, is_postgres
from app.utils import is_authed, check_csrf, get_csrf_token
from datetime import datetime, timezone, timedelta
import time

bp = Blueprint('auth', __name__, url_prefix='/api')

def _norm_slug(v):
    return str(v or '').strip().lower()

def _norm_user(v):
    return str(v or '').strip()

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

def ensure_admin_users_last_seen_column(db, cur):
    if is_postgres():
        try:
            cur.execute("ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS last_seen_at TEXT")
            db.commit()
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
        return
    try:
        cur.execute("ALTER TABLE admin_users ADD COLUMN last_seen_at TEXT")
        db.commit()
    except Exception:
        return

def touch_admin_user_last_seen(db, cur, tenant_slug, username):
    if not tenant_slug or not username:
        return
    try:
        now = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace('+00:00', 'Z')
        cur.execute(
            "UPDATE admin_users SET last_seen_at = ? WHERE tenant_slug = ? AND username = ?",
            (now, tenant_slug, username)
        )
        db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass

@bp.before_app_request
def touch_last_seen_on_activity():
    try:
        if not is_authed():
            return
        path = str(getattr(request, 'path', '') or '')
        if not path.startswith('/api/'):
            return
        now_s = int(time.time())
        prev = int(session.get('_last_seen_touch_s') or 0)
        if prev and (now_s - prev) < 30:
            return
        session['_last_seen_touch_s'] = now_s
        db = get_db()
        cur = db.cursor()
        touch_admin_user_last_seen(db, cur, session.get('tenant_slug') or '', session.get('admin_user') or '')
    except Exception:
        return

@bp.route('/auth/login', methods=['POST'])
def auth_login():
    payload = request.get_json(silent=True) or {}
    username = _norm_user(payload.get('username'))
    password = str(payload.get('password') or '')
    tenant_slug = _norm_slug(payload.get('tenant_slug'))
    if not username or not password or not tenant_slug:
        return jsonify({'error': 'credenciales incompletas'}), 400
    
    db = get_db()
    cur = db.cursor()
    cur.execute(
        "SELECT username, password_hash FROM admin_users WHERE tenant_slug = ? AND lower(username) = lower(?)",
        (tenant_slug, username)
    )
    row = cur.fetchone()
    
    if not row or not check_password_hash(row[1], password):
        return jsonify({'error': 'usuario o contraseña inválidos'}), 401
    
    real_username = str(row[0] or '').strip() or username
    touch_admin_user_last_seen(db, cur, tenant_slug, real_username)
    session['admin_auth'] = True
    session['admin_user'] = real_username
    session['tenant_slug'] = tenant_slug
    session['csrf_token'] = secrets.token_urlsafe(32)
    return jsonify({'ok': True, 'tenant_slug': tenant_slug})

@bp.route('/auth/login_dev', methods=['POST'])
def auth_login_dev():
    allow_dev = str(os.environ.get('ALLOW_DEV_LOGIN') or '').strip().lower() in ('1', 'true', 'yes')
    if not session.get('master_auth') and not allow_dev:
        return jsonify({'error': 'no autorizado'}), 401
    payload = request.get_json(silent=True) or {}
    u = _norm_user(payload.get('username'))
    t = _norm_slug(payload.get('tenant_slug'))
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
    if is_authed():
        try:
            db = get_db()
            cur = db.cursor()
            touch_admin_user_last_seen(db, cur, session.get('tenant_slug') or '', session.get('admin_user') or '')
        except Exception:
            pass
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

@bp.route('/master/admin_users', methods=['GET'])
def master_admin_users_list():
    if not session.get('master_auth'):
        return jsonify({'error': 'no autorizado'}), 401
    tenant_slug = _norm_slug(request.args.get('tenant_slug') or request.args.get('slug'))
    if not tenant_slug:
        return jsonify({'error': 'tenant_slug requerido'}), 400
    db = get_db()
    cur = db.cursor()
    try:
        ensure_admin_users_last_seen_column(db, cur)
    except Exception:
        pass
    cur.execute(
        "SELECT username, COALESCE(last_seen_at, '') AS last_seen_at FROM admin_users WHERE tenant_slug = ? ORDER BY username ASC",
        (tenant_slug,)
    )
    rows = cur.fetchall() or []
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    users = []
    for r in rows:
        u = str(r[0] or '')
        last_seen = str(r[1] or '')
        is_online = False
        if last_seen:
            try:
                ts = last_seen.replace('Z', '+00:00')
                dt = datetime.fromisoformat(ts)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                is_online = (now - dt) <= timedelta(minutes=2)
            except Exception:
                is_online = False
        users.append({'username': u, 'last_seen_at': last_seen, 'online': bool(is_online)})
    return jsonify({'tenant_slug': tenant_slug, 'users': users})

@bp.route('/master/admin_users', methods=['POST'])
def master_admin_users_create():
    if not session.get('master_auth'):
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    payload = request.get_json(silent=True) or {}
    tenant_slug = _norm_slug(payload.get('tenant_slug') or payload.get('slug'))
    username = _norm_user(payload.get('username'))
    password = str(payload.get('password') or '')
    if not tenant_slug or not username or not password:
        return jsonify({'error': 'datos incompletos'}), 400
    db = get_db()
    cur = db.cursor()
    try:
        ensure_admin_users_last_seen_column(db, cur)
    except Exception:
        pass
    try:
        cur.execute("SELECT 1 FROM admin_users WHERE tenant_slug = ? AND lower(username) = lower(?)", (tenant_slug, username))
        if cur.fetchone():
            return jsonify({'error': 'usuario ya existe'}), 409
        ph = generate_password_hash(password)
        cur.execute("INSERT INTO admin_users (tenant_slug, username, password_hash) VALUES (?, ?, ?)", (tenant_slug, username, ph))
        db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return jsonify({'error': 'no se pudo crear el usuario'}), 500
    try:
        touch_admin_user_last_seen(db, cur, tenant_slug, username)
    except Exception:
        pass
    return jsonify({'ok': True, 'tenant_slug': tenant_slug, 'username': username})

@bp.route('/master/admin_users', methods=['PATCH'])
def master_admin_users_update():
    if not session.get('master_auth'):
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    payload = request.get_json(silent=True) or {}
    tenant_slug = _norm_slug(payload.get('tenant_slug') or payload.get('slug'))
    username = _norm_user(payload.get('username'))
    new_username = _norm_user(payload.get('new_username'))
    new_password = str(payload.get('new_password') or '')
    if not tenant_slug or not username:
        return jsonify({'error': 'tenant_slug y username requeridos'}), 400
    if not new_username and not new_password:
        return jsonify({'error': 'nada para actualizar'}), 400
    db = get_db()
    cur = db.cursor()
    try:
        ensure_admin_users_last_seen_column(db, cur)
    except Exception:
        pass
    try:
        cur.execute("SELECT 1 FROM admin_users WHERE tenant_slug = ? AND lower(username) = lower(?)", (tenant_slug, username))
        if not cur.fetchone():
            return jsonify({'error': 'usuario no encontrado'}), 404
        if new_username:
            cur.execute("SELECT 1 FROM admin_users WHERE tenant_slug = ? AND lower(username) = lower(?)", (tenant_slug, new_username))
            if cur.fetchone():
                return jsonify({'error': 'el nuevo usuario ya existe'}), 409
            cur.execute("UPDATE admin_users SET username = ? WHERE tenant_slug = ? AND lower(username) = lower(?)", (new_username, tenant_slug, username))
            username = new_username
        if new_password:
            ph = generate_password_hash(new_password)
            cur.execute("UPDATE admin_users SET password_hash = ? WHERE tenant_slug = ? AND lower(username) = lower(?)", (ph, tenant_slug, username))
        db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return jsonify({'error': 'no se pudo actualizar el usuario'}), 500
    try:
        touch_admin_user_last_seen(db, cur, tenant_slug, username)
    except Exception:
        pass
    return jsonify({'ok': True, 'tenant_slug': tenant_slug, 'username': username})

@bp.route('/admin_users', methods=['GET'])
def admin_users_list():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    tenant_slug = _norm_slug(request.args.get('tenant_slug') or session.get('tenant_slug'))
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
    username = _norm_user(payload.get('username'))
    password = str(payload.get('password') or '')
    tenant_slug = _norm_slug(payload.get('tenant_slug') or session.get('tenant_slug'))
    if not username or not password or not tenant_slug:
        return jsonify({'error': 'datos incompletos'}), 400
    if session.get('tenant_slug') and session.get('tenant_slug') != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT 1 FROM admin_users WHERE tenant_slug = ? AND lower(username) = lower(?)", (tenant_slug, username))
    exists = cur.fetchone()
    if exists:
        return jsonify({'error': 'usuario ya existe'}), 409
    
    ph = generate_password_hash(password)
    cur.execute("INSERT INTO admin_users (tenant_slug, username, password_hash) VALUES (?, ?, ?)", (tenant_slug, username, ph))
    db.commit()
    
    return jsonify({'ok': True, 'username': username, 'tenant_slug': tenant_slug})
