from flask import Blueprint, jsonify, current_app
import os
import sys
from datetime import datetime, timezone

bp = Blueprint('system', __name__)

@bp.route('/favicon.ico')
def favicon():
    return ('', 204, {'Content-Type': 'image/x-icon'})

@bp.route('/api/version')
def version():
    return jsonify({
        'version': '1.0.14',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'deploy_check': 'ok',
        'render_commit': os.environ.get('RENDER_GIT_COMMIT') or '',
        'render_service': os.environ.get('RENDER_SERVICE_NAME') or '',
        'python_version': sys.version,
        'cwd': os.getcwd()
    })

@bp.route('/api/ping_system')
def ping():
    return jsonify({'status': 'ok', 'message': 'System blueprint active'})

@bp.route('/api/routes_debug')
def routes_debug():
    routes = []
    for rule in current_app.url_map.iter_rules():
        routes.append(f"{rule.endpoint}: {rule.rule} ({','.join(rule.methods)})")
    return jsonify({'routes': routes})

@bp.route('/api/db_check')
def db_check():
    import os
    from app.database import get_db
    
    status = {
        'database_url_configured': bool(os.environ.get('DATABASE_URL')),
        'is_postgres': False,
        'connection_success': False,
        'tables': [],
        'admin_users_count': 0,
        'error': None
    }
    
    # Check URL masking
    db_url = os.environ.get('DATABASE_URL', '')
    if db_url:
        # Mask password
        try:
            parts = db_url.split('@')
            if len(parts) > 1:
                status['masked_url'] = '...' + parts[1]
            else:
                status['masked_url'] = 'Invalid format'
        except:
            status['masked_url'] = 'Error masking'
            
    try:
        db = get_db()
        status['is_postgres'] = current_app.config.get('IS_POSTGRES', False)
        
        cur = db.cursor()
        cur.execute("SELECT 1")
        status['connection_success'] = True
        
        # List tables
        if status['is_postgres']:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        else:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            
        rows = cur.fetchall()
        status['tables'] = [r[0] for r in rows]
        
        # Check users
        if 'admin_users' in status['tables']:
            cur.execute("SELECT COUNT(*) FROM admin_users")
            res = cur.fetchone()
            status['admin_users_count'] = res[0] if res else 0
            
    except Exception as e:
        status['error'] = str(e)
        
    return jsonify(status)

@bp.route('/api/init_db_force')
def init_db_force():
    """Forza la inicialización de la base de datos y creación de tablas."""
    from app.database import init_db, seed_admin_users_from_env, seed_products_from_config
    import os
    
    log = []
    try:
        log.append("Iniciando init_db()...")
        init_db()
        log.append("init_db() completado.")
        
        config_dir = current_app.config.get('CONFIG_DIR')
        if config_dir and os.path.exists(config_dir):
            log.append(f"Seeding admin users from {config_dir}...")
            seed_admin_users_from_env(config_dir)
            log.append("Seeding products...")
            seed_products_from_config(config_dir)
        else:
            log.append("Config dir not found or empty.")
            
        return jsonify({'status': 'ok', 'log': log})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e), 'log': log}), 500
