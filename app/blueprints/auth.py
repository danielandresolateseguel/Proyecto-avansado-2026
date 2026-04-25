import secrets
import os
from flask import Blueprint, request, jsonify, session
from werkzeug.security import check_password_hash, generate_password_hash
from app.database import get_db, is_postgres
from app.utils import is_authed, check_csrf, get_csrf_token
from datetime import datetime, timezone, timedelta
import time

bp = Blueprint('auth', __name__, url_prefix='/api')
_LOGIN_RATE_LIMIT = {}
_LOGIN_WINDOW_S = 300
_LOGIN_LOCKOUT_S = 900
_LOGIN_MAX_ATTEMPTS = 5
_BOOTSTRAP_MAX_ATTEMPTS = 3

def _norm_slug(v):
    return str(v or '').strip().lower()

def _norm_user(v):
    return str(v or '').strip()

def _is_local_dev_request():
    remote_addr = str(request.remote_addr or '').strip().lower()
    host = str(request.host or '').strip().lower()
    return remote_addr in ('127.0.0.1', '::1', '::ffff:127.0.0.1') or host.startswith('localhost:') or host.startswith('127.0.0.1:')

def _client_ip():
    forwarded = str(request.headers.get('X-Forwarded-For') or '').split(',', 1)[0].strip().lower()
    if forwarded:
        return forwarded
    return str(request.remote_addr or '').strip().lower()

def _rate_limit_key(scope, identity=''):
    return f"{scope}:{_client_ip()}:{str(identity or '').strip().lower()}"

def _rate_limit_remaining(scope, identity='', max_attempts=_LOGIN_MAX_ATTEMPTS, window_s=_LOGIN_WINDOW_S, lockout_s=_LOGIN_LOCKOUT_S):
    now = time.time()
    key = _rate_limit_key(scope, identity)
    state = _LOGIN_RATE_LIMIT.get(key) or {}
    blocked_until = float(state.get('blocked_until') or 0)
    if blocked_until > now:
        return max(1, int(blocked_until - now))
    attempts = [ts for ts in (state.get('attempts') or []) if (now - float(ts)) < window_s]
    _LOGIN_RATE_LIMIT[key] = {'attempts': attempts, 'blocked_until': 0}
    if len(attempts) >= max_attempts:
        blocked_until = now + lockout_s
        _LOGIN_RATE_LIMIT[key] = {'attempts': attempts, 'blocked_until': blocked_until}
        return max(1, int(lockout_s))
    return 0

def _rate_limit_fail(scope, identity='', max_attempts=_LOGIN_MAX_ATTEMPTS, window_s=_LOGIN_WINDOW_S, lockout_s=_LOGIN_LOCKOUT_S):
    now = time.time()
    key = _rate_limit_key(scope, identity)
    attempts = [ts for ts in (_LOGIN_RATE_LIMIT.get(key, {}).get('attempts') or []) if (now - float(ts)) < window_s]
    attempts.append(now)
    blocked_until = 0
    if len(attempts) >= max_attempts:
        blocked_until = now + lockout_s
    _LOGIN_RATE_LIMIT[key] = {'attempts': attempts, 'blocked_until': blocked_until}
    if blocked_until > now:
        return max(1, int(blocked_until - now))
    return 0

def _rate_limit_reset(scope, identity=''):
    _LOGIN_RATE_LIMIT.pop(_rate_limit_key(scope, identity), None)

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

def ensure_tenants_status_message_column(db, cur):
    if is_postgres():
        try:
            cur.execute("ALTER TABLE tenants ADD COLUMN IF NOT EXISTS status_message TEXT DEFAULT ''")
            db.commit()
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
        return
    try:
        cur.execute("PRAGMA table_info(tenants)")
        cols = [r[1] for r in cur.fetchall()]
        if 'status_message' not in cols:
            cur.execute("ALTER TABLE tenants ADD COLUMN status_message TEXT DEFAULT ''")
            db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass

def ensure_tenants_plan_columns(db, cur):
    if is_postgres():
        try:
            cur.execute("ALTER TABLE tenants ADD COLUMN IF NOT EXISTS plan TEXT NOT NULL DEFAULT 'standard'")
            db.commit()
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
        try:
            cur.execute("ALTER TABLE tenants ADD COLUMN IF NOT EXISTS max_users INTEGER NOT NULL DEFAULT 3")
            db.commit()
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
        return
    try:
        cur.execute("PRAGMA table_info(tenants)")
        cols = [r[1] for r in cur.fetchall()]
        changed = False
        if 'plan' not in cols:
            cur.execute("ALTER TABLE tenants ADD COLUMN plan TEXT NOT NULL DEFAULT 'standard'")
            changed = True
        if 'max_users' not in cols:
            cur.execute("ALTER TABLE tenants ADD COLUMN max_users INTEGER NOT NULL DEFAULT 3")
            changed = True
        if changed:
            db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass

def ensure_admin_users_rbac_columns(db, cur):
    if is_postgres():
        try:
            cur.execute("ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'admin'")
            db.commit()
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
        try:
            cur.execute("ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS permissions_json TEXT DEFAULT ''")
            db.commit()
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
        try:
            cur.execute("ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS is_owner INTEGER NOT NULL DEFAULT 0")
            db.commit()
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
        return
    try:
        cur.execute("PRAGMA table_info(admin_users)")
        cols = [r[1] for r in cur.fetchall()]
        changed = False
        if 'role' not in cols:
            cur.execute("ALTER TABLE admin_users ADD COLUMN role TEXT NOT NULL DEFAULT 'admin'")
            changed = True
        if 'permissions_json' not in cols:
            cur.execute("ALTER TABLE admin_users ADD COLUMN permissions_json TEXT DEFAULT ''")
            changed = True
        if 'is_owner' not in cols:
            cur.execute("ALTER TABLE admin_users ADD COLUMN is_owner INTEGER NOT NULL DEFAULT 0")
            changed = True
        if changed:
            db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass

def _role_defaults(role):
    r = str(role or '').strip().lower()
    if r in ('mozo', 'cocina', 'caja', 'repartidor', 'admin'):
        role = r
    else:
        role = 'admin'
    perms = {}
    if role == 'admin':
        perms = {
            'orders_view': True,
            'orders_update_status': True,
            'orders_cancel': True,
            'orders_create': True,
            'tables_manage': True,
            'cash_view': True,
            'cash_manage': True,
            'products_manage': True,
            'carousel_manage': True,
            'reports_view': True,
            'users_manage': True
        }
    elif role == 'mozo':
        perms = {
            'orders_view': True,
            'orders_update_status': True,
            'orders_create': True,
            'tables_manage': True,
            'cash_view': True,
            'cash_manage': True
        }
    elif role == 'cocina':
        perms = {
            'orders_view': True,
            'orders_update_status': True
        }
    elif role == 'caja':
        perms = {
            'orders_view': True,
            'orders_update_status': True,
            'orders_create': True,
            'cash_view': True,
            'cash_manage': True
        }
    elif role == 'repartidor':
        perms = {
            'orders_view': True,
            'orders_update_status': True,
            'delivery_manage': True,
            'cash_view': True,
            'cash_manage': True
        }
    return role, perms

def _parse_perms_json(s):
    import json as _json
    if not s:
        return {}
    try:
        v = _json.loads(s)
        if isinstance(v, dict):
            return {str(k): bool(v[k]) for k in v.keys()}
        if isinstance(v, list):
            out = {}
            for it in v:
                k = str(it or '').strip()
                if k:
                    out[k] = True
            return out
    except Exception:
        return {}
    return {}

def _tenant_plan_limit(db, cur, tenant_slug):
    plan = 'standard'
    max_users = 3
    try:
        ensure_tenants_plan_columns(db, cur)
    except Exception:
        pass
    try:
        cur.execute("SELECT COALESCE(plan, 'standard') AS plan, COALESCE(max_users, 3) AS max_users FROM tenants WHERE tenant_slug = ?", (tenant_slug,))
        row = cur.fetchone()
        if row:
            plan = str(row[0] or 'standard').strip().lower() or 'standard'
            try:
                max_users = int(row[1] or 0)
            except Exception:
                max_users = 0
    except Exception:
        plan = 'standard'
        max_users = 0
    if max_users <= 0:
        max_users = 6 if plan == 'pro' else 3
    if plan not in ('standard', 'pro'):
        plan = 'standard'
    return plan, max_users

def _tenant_owner_limit():
    return 2

def _count_tenant_owners(cur, tenant_slug, exclude_username=None):
    if not tenant_slug:
        return 0
    try:
        if exclude_username:
            cur.execute(
                "SELECT COUNT(*) FROM admin_users WHERE tenant_slug = ? AND COALESCE(is_owner, 0) = 1 AND lower(username) != lower(?)",
                (tenant_slug, exclude_username),
            )
        else:
            cur.execute(
                "SELECT COUNT(*) FROM admin_users WHERE tenant_slug = ? AND COALESCE(is_owner, 0) = 1",
                (tenant_slug,),
            )
        row = cur.fetchone()
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0

def _can_manage_users():
    if not is_authed():
        return False
    if session.get('admin_owner'):
        return True
    role = str(session.get('admin_role') or '').strip().lower()
    if role == 'admin':
        return True
    perms = _parse_perms_json(session.get('admin_perms') or '')
    return bool(perms.get('users_manage'))

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
    identity = f"{tenant_slug}:{username}"
    retry_after = _rate_limit_remaining('admin_login', identity)
    if retry_after > 0:
        return jsonify({'error': 'demasiados intentos, probá más tarde', 'retry_after_seconds': retry_after}), 429
    
    db = get_db()
    cur = db.cursor()
    try:
        ensure_admin_users_rbac_columns(db, cur)
    except Exception:
        pass
    cur.execute(
        "SELECT username, password_hash, COALESCE(role, 'admin') AS role, COALESCE(permissions_json, '') AS permissions_json, COALESCE(is_owner, 0) AS is_owner FROM admin_users WHERE tenant_slug = ? AND lower(username) = lower(?)",
        (tenant_slug, username)
    )
    row = cur.fetchone()
    
    if not row or not check_password_hash(row[1], password):
        retry_after = _rate_limit_fail('admin_login', identity)
        if retry_after > 0:
            return jsonify({'error': 'demasiados intentos, probá más tarde', 'retry_after_seconds': retry_after}), 429
        return jsonify({'error': 'usuario o contraseña inválidos'}), 401
    
    tenant_status = 'active'
    tenant_message = ''
    try:
        ensure_tenants_status_message_column(db, cur)
    except Exception:
        pass
    try:
        cur.execute("SELECT status, COALESCE(status_message, '') AS status_message FROM tenants WHERE tenant_slug = ?", (tenant_slug,))
        trow = cur.fetchone()
        if trow:
            tenant_status = str(trow[0] or 'active').strip().lower() or 'active'
            tenant_message = str(trow[1] or '').strip()
    except Exception:
        tenant_status = 'active'
        tenant_message = ''
    if tenant_status == 'suspended':
        msg = tenant_message or 'Servicio suspendido. Por favor, regulariza tu situación para reactivar el acceso.'
        return jsonify({'error': msg, 'tenant_slug': tenant_slug, 'tenant_status': tenant_status}), 403

    real_username = str(row[0] or '').strip() or username
    role = str(row[2] or 'admin').strip().lower() or 'admin'
    perms_json = str(row[3] or '').strip()
    is_owner = bool(int(row[4] or 0))
    perms = _parse_perms_json(perms_json)
    role, defaults = _role_defaults(role)
    if not perms:
        perms = defaults
    else:
        try:
            merged = dict(defaults or {})
            merged.update(perms)
            perms = merged
        except Exception:
            pass
    try:
        import json as _json
        perms_json = _json.dumps(perms, ensure_ascii=False)
    except Exception:
        perms_json = ''
    touch_admin_user_last_seen(db, cur, tenant_slug, real_username)
    _rate_limit_reset('admin_login', identity)
    session.clear()
    session['admin_auth'] = True
    session['admin_user'] = real_username
    session['tenant_slug'] = tenant_slug
    session['admin_role'] = role
    session['admin_perms'] = perms_json
    session['admin_owner'] = bool(is_owner)
    session['csrf_token'] = secrets.token_urlsafe(32)
    return jsonify({'ok': True, 'tenant_slug': tenant_slug, 'tenant_status': tenant_status, 'tenant_message': tenant_message, 'role': role, 'permissions': perms})

@bp.route('/auth/login_dev', methods=['POST'])
def auth_login_dev():
    allow_dev = str(os.environ.get('ALLOW_DEV_LOGIN') or '').strip().lower() in ('1', 'true', 'yes')
    env_name = str(os.environ.get('FLASK_ENV') or '').strip().lower()
    can_use_dev_login = allow_dev and env_name != 'production' and _is_local_dev_request()
    if not session.get('master_auth') and not can_use_dev_login:
        return jsonify({'error': 'no autorizado'}), 401
    payload = request.get_json(silent=True) or {}
    u = _norm_user(payload.get('username'))
    t = _norm_slug(payload.get('tenant_slug'))
    session.pop('admin_auth', None)
    session.pop('admin_user', None)
    session.pop('tenant_slug', None)
    session.pop('_last_seen_touch_s', None)
    session.pop('admin_role', None)
    session.pop('admin_perms', None)
    session.pop('admin_owner', None)
    session['admin_auth'] = True
    session['admin_user'] = u or 'admin'
    if t:
        session['tenant_slug'] = t
    session['admin_role'] = 'admin'
    session['admin_perms'] = ''
    session['admin_owner'] = True
    session['csrf_token'] = secrets.token_urlsafe(32)
    return jsonify({'ok': True, 'dev': True, 'tenant_slug': t or None})

@bp.route('/auth/logout', methods=['POST'])
def auth_logout():
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    if session.get('master_auth'):
        session.pop('admin_auth', None)
        session.pop('admin_user', None)
        session.pop('tenant_slug', None)
        session.pop('_last_seen_touch_s', None)
        session.pop('admin_role', None)
        session.pop('admin_perms', None)
        session.pop('admin_owner', None)
    else:
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
    role_raw = str(session.get('admin_role') or '').strip().lower()
    effective_role, defaults = _role_defaults(role_raw)
    perms_raw = _parse_perms_json(session.get('admin_perms') or '')
    if not perms_raw:
        perms = defaults
    else:
        try:
            perms = dict(defaults or {})
            perms.update(perms_raw)
        except Exception:
            perms = perms_raw
    try:
        import json as _json
        session['admin_role'] = effective_role
        session['admin_perms'] = _json.dumps(perms, ensure_ascii=False)
    except Exception:
        pass
    tenant_status = ''
    tenant_message = ''
    suspended = False
    if is_authed():
        try:
            db = get_db()
            cur = db.cursor()
            try:
                ensure_tenants_status_message_column(db, cur)
            except Exception:
                pass
            slug = str(session.get('tenant_slug') or '').strip().lower()
            if slug:
                cur.execute("SELECT status, COALESCE(status_message, '') AS status_message FROM tenants WHERE tenant_slug = ?", (slug,))
                trow = cur.fetchone()
                if trow:
                    tenant_status = str(trow[0] or '').strip().lower()
                    tenant_message = str(trow[1] or '').strip()
                    suspended = tenant_status == 'suspended'
        except Exception:
            tenant_status = ''
            tenant_message = ''
            suspended = False
    return jsonify({
        'authenticated': is_authed(),
        'user': session.get('admin_user') or '',
        'tenant_slug': session.get('tenant_slug') or '',
        'role': session.get('admin_role') or '',
        'permissions': perms,
        'is_owner': bool(session.get('admin_owner')),
        'tenant_status': tenant_status,
        'tenant_message': tenant_message,
        'suspended': bool(suspended)
    })

@bp.route('/auth/csrf', methods=['GET'])
def auth_csrf():
    # Allow fetching CSRF token even if not authenticated (for login page, etc)
    return jsonify({'token': get_csrf_token()})

@bp.route('/auth/master_status', methods=['GET'])
def master_status():
    return jsonify({'authenticated': bool(session.get('master_auth')), 'user': session.get('master_user') or ''})

@bp.route('/auth/master_bootstrap', methods=['POST'])
def master_bootstrap():
    # Allows creating the first master user if none exists
    allow_remote_bootstrap = str(os.environ.get('ALLOW_MASTER_BOOTSTRAP') or '').strip().lower() in ('1', 'true', 'yes')
    if not _is_local_dev_request() and not allow_remote_bootstrap:
        return jsonify({'error': 'bootstrap solo permitido en local o con ALLOW_MASTER_BOOTSTRAP'}), 403
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    payload = request.get_json(silent=True) or {}
    username = (payload.get('username') or '').strip()
    password = str(payload.get('password') or '')
    if not username or not password:
        return jsonify({'error': 'datos incompletos'}), 400
    retry_after = _rate_limit_remaining('master_bootstrap', username, max_attempts=_BOOTSTRAP_MAX_ATTEMPTS)
    if retry_after > 0:
        return jsonify({'error': 'demasiados intentos, probá más tarde', 'retry_after_seconds': retry_after}), 429
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
        _rate_limit_fail('master_bootstrap', username, max_attempts=_BOOTSTRAP_MAX_ATTEMPTS)
        try:
            db.rollback()
        except Exception:
            pass
        return jsonify({'error': 'base de datos no disponible'}), 503
    _rate_limit_reset('master_bootstrap', username)
    return jsonify({'ok': True, 'username': username})

@bp.route('/auth/master_login', methods=['POST'])
def master_login():
    payload = request.get_json(silent=True) or {}
    username = (payload.get('username') or '').strip()
    password = str(payload.get('password') or '')
    if not username or not password:
        return jsonify({'error': 'datos incompletos'}), 400
    retry_after = _rate_limit_remaining('master_login', username)
    if retry_after > 0:
        return jsonify({'error': 'demasiados intentos, probá más tarde', 'retry_after_seconds': retry_after}), 429
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
        _rate_limit_fail('master_login', username)
        return jsonify({'error': 'base de datos no disponible'}), 503
    if not row or not check_password_hash(row[0], password):
        retry_after = _rate_limit_fail('master_login', username)
        if retry_after > 0:
            return jsonify({'error': 'demasiados intentos, probá más tarde', 'retry_after_seconds': retry_after}), 429
        return jsonify({'error': 'usuario o contraseña inválidos'}), 401
    import secrets as _secrets
    _rate_limit_reset('master_login', username)
    session.clear()
    session['master_auth'] = True
    session['master_user'] = username
    session['csrf_token'] = _secrets.token_urlsafe(32)
    return jsonify({'ok': True, 'user': username})

@bp.route('/auth/master_logout', methods=['POST'])
def master_logout():
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    if session.get('admin_auth'):
        session.pop('master_auth', None)
        session.pop('master_user', None)
    else:
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
    try:
        ensure_admin_users_rbac_columns(db, cur)
    except Exception:
        pass
    cur.execute(
        "SELECT username, COALESCE(last_seen_at, '') AS last_seen_at, COALESCE(role, 'admin') AS role, COALESCE(permissions_json, '') AS permissions_json, COALESCE(is_owner, 0) AS is_owner FROM admin_users WHERE tenant_slug = ? ORDER BY username ASC",
        (tenant_slug,)
    )
    rows = cur.fetchall() or []
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    users = []
    for r in rows:
        u = str(r[0] or '')
        last_seen = str(r[1] or '')
        role = str(r[2] or 'admin').strip().lower() or 'admin'
        perms_json = str(r[3] or '').strip()
        is_owner = bool(int(r[4] or 0))
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
        users.append({'username': u, 'last_seen_at': last_seen, 'online': bool(is_online), 'role': role, 'permissions_json': perms_json, 'is_owner': bool(is_owner)})
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
    role = str(payload.get('role') or 'admin').strip().lower()
    permissions = payload.get('permissions')
    permissions_json = str(payload.get('permissions_json') or '').strip()
    is_owner = bool(payload.get('is_owner') or False)
    if not tenant_slug or not username or not password:
        return jsonify({'error': 'datos incompletos'}), 400
    db = get_db()
    cur = db.cursor()
    try:
        ensure_admin_users_last_seen_column(db, cur)
    except Exception:
        pass
    try:
        ensure_admin_users_rbac_columns(db, cur)
    except Exception:
        pass
    plan, max_users = _tenant_plan_limit(db, cur, tenant_slug)
    try:
        cur.execute("SELECT COUNT(*) FROM admin_users WHERE tenant_slug = ?", (tenant_slug,))
        c = cur.fetchone()
        count_users = int(c[0] or 0) if c else 0
    except Exception:
        count_users = 0
    if count_users >= max_users:
        return jsonify({'error': f'límite de usuarios alcanzado (plan {plan}: {max_users})'}), 403
    if is_owner:
        owner_limit = _tenant_owner_limit()
        owners = _count_tenant_owners(cur, tenant_slug)
        if owners >= owner_limit:
            return jsonify({'error': f'límite de owners alcanzado (máx {owner_limit})'}), 403
    try:
        cur.execute("SELECT 1 FROM admin_users WHERE tenant_slug = ? AND lower(username) = lower(?)", (tenant_slug, username))
        if cur.fetchone():
            return jsonify({'error': 'usuario ya existe'}), 409
        ph = generate_password_hash(password)
        if permissions is not None and not permissions_json:
            try:
                import json as _json
                if isinstance(permissions, dict) or isinstance(permissions, list):
                    permissions_json = _json.dumps(permissions, ensure_ascii=False)
            except Exception:
                permissions_json = ''
        role, defaults = _role_defaults(role)
        if role != 'admin':
            is_owner = False
        if not permissions_json:
            try:
                import json as _json
                permissions_json = _json.dumps(defaults, ensure_ascii=False)
            except Exception:
                permissions_json = ''
        cur.execute("INSERT INTO admin_users (tenant_slug, username, password_hash, role, permissions_json, is_owner) VALUES (?, ?, ?, ?, ?, ?)", (tenant_slug, username, ph, role, permissions_json, 1 if is_owner else 0))
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
    role = str(payload.get('role') or '').strip().lower()
    permissions = payload.get('permissions')
    permissions_json = str(payload.get('permissions_json') or '').strip()
    is_owner = payload.get('is_owner')
    if not tenant_slug or not username:
        return jsonify({'error': 'tenant_slug y username requeridos'}), 400
    if not new_username and not new_password and not role and permissions is None and not permissions_json and is_owner is None:
        return jsonify({'error': 'nada para actualizar'}), 400
    db = get_db()
    cur = db.cursor()
    try:
        ensure_admin_users_last_seen_column(db, cur)
    except Exception:
        pass
    try:
        ensure_admin_users_rbac_columns(db, cur)
    except Exception:
        pass
    try:
        cur.execute("SELECT COALESCE(role, 'admin') AS role FROM admin_users WHERE tenant_slug = ? AND lower(username) = lower(?)", (tenant_slug, username))
        r0 = cur.fetchone()
        if not r0:
            return jsonify({'error': 'usuario no encontrado'}), 404
        current_role = str(r0[0] or 'admin').strip().lower() or 'admin'
        effective_role = current_role
        if role:
            effective_role, _ = _role_defaults(role)
        if is_owner is not None and bool(is_owner) and effective_role != 'admin':
            try:
                db.rollback()
            except Exception:
                pass
            return jsonify({'error': 'solo un usuario con rol admin puede ser owner'}), 400
        if is_owner is not None and bool(is_owner):
            owner_limit = _tenant_owner_limit()
            owners = _count_tenant_owners(cur, tenant_slug, exclude_username=username)
            if owners >= owner_limit:
                try:
                    db.rollback()
                except Exception:
                    pass
                return jsonify({'error': f'límite de owners alcanzado (máx {owner_limit})'}), 403
        if new_username:
            cur.execute("SELECT 1 FROM admin_users WHERE tenant_slug = ? AND lower(username) = lower(?)", (tenant_slug, new_username))
            if cur.fetchone():
                return jsonify({'error': 'el nuevo usuario ya existe'}), 409
            cur.execute("UPDATE admin_users SET username = ? WHERE tenant_slug = ? AND lower(username) = lower(?)", (new_username, tenant_slug, username))
            username = new_username
        if new_password:
            ph = generate_password_hash(new_password)
            cur.execute("UPDATE admin_users SET password_hash = ? WHERE tenant_slug = ? AND lower(username) = lower(?)", (ph, tenant_slug, username))
        if role:
            role, _ = _role_defaults(role)
            cur.execute("UPDATE admin_users SET role = ? WHERE tenant_slug = ? AND lower(username) = lower(?)", (role, tenant_slug, username))
            if is_owner is not None and bool(is_owner) and role != 'admin':
                try:
                    db.rollback()
                except Exception:
                    pass
                return jsonify({'error': 'solo un usuario con rol admin puede ser owner'}), 400
            if role != 'admin':
                cur.execute("UPDATE admin_users SET is_owner = 0 WHERE tenant_slug = ? AND lower(username) = lower(?)", (tenant_slug, username))
        if permissions is not None and not permissions_json:
            try:
                import json as _json
                if isinstance(permissions, dict) or isinstance(permissions, list):
                    permissions_json = _json.dumps(permissions, ensure_ascii=False)
            except Exception:
                permissions_json = ''
        if permissions_json:
            cur.execute("UPDATE admin_users SET permissions_json = ? WHERE tenant_slug = ? AND lower(username) = lower(?)", (permissions_json, tenant_slug, username))
        if is_owner is not None:
            cur.execute("UPDATE admin_users SET is_owner = ? WHERE tenant_slug = ? AND lower(username) = lower(?)", (1 if bool(is_owner) else 0, tenant_slug, username))
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
    if not _can_manage_users():
        return jsonify({'error': 'sin permisos'}), 403
    tenant_slug = _norm_slug(request.args.get('tenant_slug') or session.get('tenant_slug'))
    if session.get('tenant_slug') and tenant_slug and session.get('tenant_slug') != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    
    db = get_db()
    cur = db.cursor()
    try:
        ensure_admin_users_rbac_columns(db, cur)
    except Exception:
        pass
    plan, max_users = _tenant_plan_limit(db, cur, tenant_slug)
    try:
        cur.execute("SELECT COUNT(*) FROM admin_users WHERE tenant_slug = ?", (tenant_slug,))
        c = cur.fetchone()
        count_users = int(c[0] or 0) if c else 0
    except Exception:
        count_users = 0
    cur.execute("SELECT username, COALESCE(role, 'admin') AS role, COALESCE(permissions_json, '') AS permissions_json, COALESCE(is_owner, 0) AS is_owner FROM admin_users WHERE tenant_slug = ? ORDER BY username ASC", (tenant_slug,))
    rows = cur.fetchall()
    
    users = []
    for r in rows or []:
        users.append({'username': r[0], 'role': r[1], 'permissions_json': r[2] or '', 'is_owner': bool(int(r[3] or 0))})
    return jsonify({'tenant_slug': tenant_slug, 'users': users, 'plan': plan, 'max_users': int(max_users), 'count_users': int(count_users)})

@bp.route('/admin_users', methods=['POST'])
def admin_users_create():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    if not _can_manage_users():
        return jsonify({'error': 'sin permisos'}), 403
    payload = request.get_json(silent=True) or {}
    username = _norm_user(payload.get('username'))
    password = str(payload.get('password') or '')
    tenant_slug = _norm_slug(payload.get('tenant_slug') or session.get('tenant_slug'))
    role = str(payload.get('role') or 'admin').strip().lower()
    permissions = payload.get('permissions')
    permissions_json = str(payload.get('permissions_json') or '').strip()
    is_owner = bool(payload.get('is_owner') or False)
    if not username or not password or not tenant_slug:
        return jsonify({'error': 'datos incompletos'}), 400
    if session.get('tenant_slug') and session.get('tenant_slug') != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    
    db = get_db()
    cur = db.cursor()
    try:
        ensure_admin_users_rbac_columns(db, cur)
    except Exception:
        pass
    plan, max_users = _tenant_plan_limit(db, cur, tenant_slug)
    try:
        cur.execute("SELECT COUNT(*) FROM admin_users WHERE tenant_slug = ?", (tenant_slug,))
        c = cur.fetchone()
        count_users = int(c[0] or 0) if c else 0
    except Exception:
        count_users = 0
    if count_users >= max_users:
        return jsonify({'error': f'límite de usuarios alcanzado (plan {plan}: {max_users})'}), 403
    if is_owner:
        owner_limit = _tenant_owner_limit()
        owners = _count_tenant_owners(cur, tenant_slug)
        if owners >= owner_limit:
            return jsonify({'error': f'límite de owners alcanzado (máx {owner_limit})'}), 403
    cur.execute("SELECT 1 FROM admin_users WHERE tenant_slug = ? AND lower(username) = lower(?)", (tenant_slug, username))
    exists = cur.fetchone()
    if exists:
        return jsonify({'error': 'usuario ya existe'}), 409
    
    ph = generate_password_hash(password)
    if permissions is not None and not permissions_json:
        try:
            import json as _json
            if isinstance(permissions, dict) or isinstance(permissions, list):
                permissions_json = _json.dumps(permissions, ensure_ascii=False)
        except Exception:
            permissions_json = ''
    role, defaults = _role_defaults(role)
    if role != 'admin':
        is_owner = False
    if not permissions_json:
        try:
            import json as _json
            permissions_json = _json.dumps(defaults, ensure_ascii=False)
        except Exception:
            permissions_json = ''
    cur.execute("INSERT INTO admin_users (tenant_slug, username, password_hash, role, permissions_json, is_owner) VALUES (?, ?, ?, ?, ?, ?)", (tenant_slug, username, ph, role, permissions_json, 1 if is_owner else 0))
    db.commit()
    
    return jsonify({'ok': True, 'username': username, 'tenant_slug': tenant_slug, 'role': role})

@bp.route('/admin_users', methods=['PATCH'])
def admin_users_update():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    if not _can_manage_users():
        return jsonify({'error': 'sin permisos'}), 403
    payload = request.get_json(silent=True) or {}
    tenant_slug = _norm_slug(payload.get('tenant_slug') or session.get('tenant_slug'))
    username = _norm_user(payload.get('username'))
    new_username = _norm_user(payload.get('new_username'))
    new_password = str(payload.get('new_password') or '')
    role = str(payload.get('role') or '').strip().lower()
    permissions = payload.get('permissions')
    permissions_json = str(payload.get('permissions_json') or '').strip()
    is_owner = payload.get('is_owner')
    if not tenant_slug or not username:
        return jsonify({'error': 'tenant_slug y username requeridos'}), 400
    if session.get('tenant_slug') and session.get('tenant_slug') != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if not new_username and not new_password and not role and permissions is None and not permissions_json and is_owner is None:
        return jsonify({'error': 'nada para actualizar'}), 400
    if is_owner is not None and not session.get('admin_owner'):
        return jsonify({'error': 'solo el owner puede cambiar owner'}), 403

    db = get_db()
    cur = db.cursor()
    try:
        ensure_admin_users_rbac_columns(db, cur)
    except Exception:
        pass
    try:
        cur.execute("SELECT COALESCE(role, 'admin') AS role FROM admin_users WHERE tenant_slug = ? AND lower(username) = lower(?)", (tenant_slug, username))
        r0 = cur.fetchone()
        if not r0:
            return jsonify({'error': 'usuario no encontrado'}), 404
        current_role = str(r0[0] or 'admin').strip().lower() or 'admin'
        effective_role = current_role
        if role:
            effective_role, _ = _role_defaults(role)
        if is_owner is not None and bool(is_owner) and effective_role != 'admin':
            try:
                db.rollback()
            except Exception:
                pass
            return jsonify({'error': 'solo un usuario con rol admin puede ser owner'}), 400
        if is_owner is not None and bool(is_owner):
            owner_limit = _tenant_owner_limit()
            owners = _count_tenant_owners(cur, tenant_slug, exclude_username=username)
            if owners >= owner_limit:
                try:
                    db.rollback()
                except Exception:
                    pass
                return jsonify({'error': f'límite de owners alcanzado (máx {owner_limit})'}), 403
        if new_username:
            cur.execute("SELECT 1 FROM admin_users WHERE tenant_slug = ? AND lower(username) = lower(?)", (tenant_slug, new_username))
            if cur.fetchone():
                return jsonify({'error': 'el nuevo usuario ya existe'}), 409
            cur.execute("UPDATE admin_users SET username = ? WHERE tenant_slug = ? AND lower(username) = lower(?)", (new_username, tenant_slug, username))
            username = new_username
        if new_password:
            ph = generate_password_hash(new_password)
            cur.execute("UPDATE admin_users SET password_hash = ? WHERE tenant_slug = ? AND lower(username) = lower(?)", (ph, tenant_slug, username))
        if role:
            role, _ = _role_defaults(role)
            cur.execute("UPDATE admin_users SET role = ? WHERE tenant_slug = ? AND lower(username) = lower(?)", (role, tenant_slug, username))
            if is_owner is not None and bool(is_owner) and role != 'admin':
                try:
                    db.rollback()
                except Exception:
                    pass
                return jsonify({'error': 'solo un usuario con rol admin puede ser owner'}), 400
            if role != 'admin':
                cur.execute("UPDATE admin_users SET is_owner = 0 WHERE tenant_slug = ? AND lower(username) = lower(?)", (tenant_slug, username))
        if permissions is not None and not permissions_json:
            try:
                import json as _json
                if isinstance(permissions, dict) or isinstance(permissions, list):
                    permissions_json = _json.dumps(permissions, ensure_ascii=False)
            except Exception:
                permissions_json = ''
        if permissions_json:
            cur.execute("UPDATE admin_users SET permissions_json = ? WHERE tenant_slug = ? AND lower(username) = lower(?)", (permissions_json, tenant_slug, username))
        if is_owner is not None:
            cur.execute("UPDATE admin_users SET is_owner = ? WHERE tenant_slug = ? AND lower(username) = lower(?)", (1 if bool(is_owner) else 0, tenant_slug, username))
        db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return jsonify({'error': 'no se pudo actualizar el usuario'}), 500
    return jsonify({'ok': True, 'tenant_slug': tenant_slug, 'username': username})
