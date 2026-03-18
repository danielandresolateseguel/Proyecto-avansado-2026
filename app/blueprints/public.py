from flask import Blueprint, send_from_directory, current_app, jsonify
import os
import re

bp = Blueprint('public', __name__)

@bp.route('/Imagenes/<path:filename>')
def serve_images(filename):
    # Construct absolute path to Imagenes directory in project root
    # app.root_path points to '.../app'
    project_root = os.path.dirname(current_app.root_path)
    images_dir = os.path.join(project_root, 'Imagenes')
    return send_from_directory(images_dir, filename)

@bp.route('/')
def index():
    return send_from_directory(current_app.static_folder, 'index.html')

@bp.route('/api/ping')
def ping():
    return jsonify({'pong': True})

@bp.route('/api/routes')
def routes_list():
    return jsonify({'routes': [{'rule': r.rule, 'methods': list(r.methods)} for r in current_app.url_map.iter_rules()]})

@bp.route('/<path:path>')
def static_proxy(path):
    # Evitar capturar prefijos de API que no hayan sido manejados por otros blueprints
    if path.startswith('api/'):
        return jsonify({'error': 'Ruta de API no válida'}), 404
    try:
        return send_from_directory(current_app.static_folder, path)
    except Exception:
        # Si piden un HTML que no existe, devolvemos la carta base principal.
        # El JS interno calculará el tenant_slug a partir del nombre del archivo solicitado.
        if path.endswith('.html') and re.match(r'^[a-z0-9\-_]+\.html$', path):
            try:
                return send_from_directory(current_app.static_folder, 'gastronomia-local1.html')
            except Exception:
                return jsonify({'error': 'Página no disponible'}), 404
        # Para otros assets inexistentes devolvemos 404 controlado
        return jsonify({'error': 'Recurso no encontrado'}), 404
