import os
import json
from datetime import datetime
from werkzeug.utils import secure_filename
from flask import Blueprint, request, jsonify, session, current_app
from app.database import get_db
from app.utils import is_authed, check_csrf
import cloudinary
import cloudinary.uploader

bp = Blueprint('products', __name__, url_prefix='/api')

@bp.route('/products', methods=['GET'])
def list_products():
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    include_inactive = request.args.get('include_inactive') == 'true'
    conn = get_db()
    cur = conn.cursor()
    
    query = "SELECT product_id, name, price, stock, active, COALESCE(details,'') as details, COALESCE(variants_json,'') as variants_json, COALESCE(last_modified, '') as last_modified, COALESCE(image_url, '') as image_url FROM products WHERE tenant_slug = ?"
    params = [tenant_slug]
    
    if not include_inactive:
        query += " AND active = 1"
        
    query += " ORDER BY name ASC"
    
    cur.execute(query, params)
    rows = cur.fetchall()
    
    # Deduplicate by product_id
    seen_ids = set()
    items = []
    for r in rows:
        pid = r[0]
        if pid not in seen_ids:
            seen_ids.add(pid)
            items.append({
                'id': pid, 
                'name': r[1], 
                'price': int(r[2] or 0), 
                'stock': int(r[3] or 0), 
                'active': bool(r[4]), 
                'details': r[5] or '', 
                'variants': r[6] or '', 
                'last_modified': r[7] or '', 
                'image_url': r[8] or ''
            })
            
    return jsonify({'products': items, 'tenant_slug': tenant_slug})

@bp.route('/products', methods=['POST'])
def create_product():
    if not is_authed(): return jsonify({'error': 'no autorizado'}), 401
    
    data = request.get_json(silent=True) or {}
    tenant_slug = data.get('tenant_slug')
    # En entorno multi-tenant, si la sesión tiene tenant_slug, debe coincidir
    if session.get('tenant_slug') and session.get('tenant_slug') != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    product_id = data.get('id')
    name = data.get('name')
    
    try:
        price = int(data.get('price'))
    except:
        return jsonify({'error': 'Precio inválido'}), 400
        
    stock = int(data.get('stock', 0))
    details = data.get('details', '')
    image_url = data.get('image_url', '')
    section = data.get('section', '')
    interest_tag = data.get('interest_tag', '')
    food_categories = data.get('food_categories') or []
    
    if (not section) and food_categories:
        section = 'main'
    
    if isinstance(product_id, str):
        product_id = product_id.strip()
    if not tenant_slug or not name:
        return jsonify({'error': 'Faltan campos obligatorios (tenant, name)'}), 400

    variants = {}
    if section: variants['section'] = section
    if interest_tag: variants['interest_tag'] = interest_tag
    if isinstance(food_categories, list):
        if food_categories: variants['food_categories'] = food_categories
    elif isinstance(food_categories, str):
        cats = [c.strip() for c in food_categories.split(',') if c.strip()]
        if cats: variants['food_categories'] = cats
    variants_json = json.dumps(variants)
    
    try:
        conn = get_db()
        cur = conn.cursor()

        if not product_id:
            cur.execute("SELECT product_id FROM products WHERE tenant_slug = ?", (tenant_slug,))
            rows = cur.fetchall() or []
            max_n = 0
            width = 0
            for r in rows:
                raw = str(r[0] or '').strip()
                if not raw.isdigit():
                    continue
                width = max(width, len(raw))
                try:
                    n = int(raw)
                except Exception:
                    continue
                if n > max_n:
                    max_n = n
            next_n = max_n + 1
            candidate = str(next_n)
            if width > 1 and len(candidate) < width:
                candidate = candidate.zfill(width)
            while True:
                cur.execute("SELECT 1 FROM products WHERE tenant_slug = ? AND product_id = ? LIMIT 1", (tenant_slug, candidate))
                exists = cur.fetchone()
                if not exists:
                    break
                next_n += 1
                candidate = str(next_n)
                if width > 1 and len(candidate) < width:
                    candidate = candidate.zfill(width)
            product_id = candidate

        cur.execute("SELECT active FROM products WHERE tenant_slug=? AND product_id=?", (tenant_slug, product_id))
        row = cur.fetchone()
        if row:
            cur.execute("""
                UPDATE products 
                SET name=?, price=?, stock=?, active=1, details=?, variants_json=?, image_url=?, last_modified=?
                WHERE tenant_slug=? AND product_id=?
            """, (name, price, stock, details, variants_json, image_url, datetime.utcnow().isoformat(), tenant_slug, product_id))
            conn.commit()
            return jsonify({'ok': True, 'id': product_id, 'updated': True})
        
        cur.execute("""
            INSERT INTO products (tenant_slug, product_id, name, price, stock, active, details, variants_json, image_url, last_modified)
            VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
        """, (tenant_slug, product_id, name, price, stock, details, variants_json, image_url, datetime.utcnow().isoformat()))
        conn.commit()
        return jsonify({'ok': True, 'id': product_id, 'created': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@bp.route('/products/<product_id>', methods=['PATCH'])
def update_product(product_id):
    if not is_authed(): return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf(): return jsonify({'error': 'csrf inválido'}), 403
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    if session.get('tenant_slug') and session.get('tenant_slug') != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    payload = request.get_json(silent=True) or {}
    fields = []
    params = []
    if 'stock' in payload:
        try:
            s = int(payload.get('stock'))
            fields.append('stock = ?')
            params.append(max(0, s))
        except: return jsonify({'error': 'stock inválido'}), 400
    if 'price' in payload:
        try:
            pr = int(payload.get('price'))
            fields.append('price = ?')
            params.append(max(0, pr))
        except: return jsonify({'error': 'price inválido'}), 400
    if 'active' in payload:
        try:
            ac = 1 if bool(payload.get('active')) else 0
            fields.append('active = ?')
            params.append(ac)
        except: return jsonify({'error': 'active inválido'}), 400
    if 'name' in payload:
        nm = str(payload.get('name') or '').strip()
        if not nm: return jsonify({'error': 'name requerido'}), 400
        fields.append('name = ?')
        params.append(nm)
    if 'details' in payload:
        dt = str(payload.get('details') or '').strip()
        fields.append('details = ?')
        params.append(dt)
    if 'image_url' in payload:
        img = str(payload.get('image_url') or '').strip()
        fields.append('image_url = ?')
        params.append(img)
    if 'variants' in payload:
        try:
            v = payload.get('variants')
            if isinstance(v, str):
                json.loads(v)
                fields.append('variants_json = ?')
                params.append(v)
            else:
                fields.append('variants_json = ?')
                params.append(json.dumps(v or {}))
        except: return jsonify({'error': 'variants inválido'}), 400
        
    if not fields: return jsonify({'error': 'sin cambios'}), 400
    fields.append('last_modified = ?')
    params.append(datetime.utcnow().isoformat())
    params.extend([tenant_slug, product_id])
    
    conn = get_db()
    cur = conn.cursor()
    cur.execute(f"UPDATE products SET {', '.join(fields)} WHERE tenant_slug = ? AND product_id = ?", params)
    conn.commit()
    return jsonify({'ok': True, 'product_id': product_id, 'last_modified': params[len(fields)-1]})

@bp.route('/products/<product_id>', methods=['DELETE'])
def delete_product(product_id):
    if not is_authed(): return jsonify({'error': 'no autorizado'}), 401
    tenant_slug = request.args.get('tenant_slug')
    if not tenant_slug: return jsonify({'error': 'Falta tenant_slug'}), 400
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            UPDATE products 
            SET active = 0, last_modified = ? 
            WHERE tenant_slug = ? AND product_id = ?
        """, (datetime.utcnow().isoformat(), tenant_slug, product_id))
        if cur.rowcount == 0: return jsonify({'error': 'Producto no encontrado'}), 404
        conn.commit()
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@bp.route('/upload', methods=['POST'])
def upload_file():
    if not is_authed(): return jsonify({'error': 'no autorizado'}), 401
    if 'file' not in request.files: return jsonify({'error': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({'error': 'No selected file'}), 400
    
    if file:
        # Check for Cloudinary configuration
        cloud_name = os.getenv('CLOUDINARY_CLOUD_NAME')
        api_key = os.getenv('CLOUDINARY_API_KEY')
        api_secret = os.getenv('CLOUDINARY_API_SECRET')
        
        if cloud_name and api_key and api_secret:
            try:
                cloudinary.config(
                    cloud_name=cloud_name,
                    api_key=api_key,
                    api_secret=api_secret,
                    secure=True
                )
                
                # Upload to Cloudinary with optimizations
                upload_result = cloudinary.uploader.upload(
                    file,
                    quality="auto",
                    fetch_format="auto",
                    width=1200,
                    crop="limit"
                )
                return jsonify({'url': upload_result['secure_url']})
            except Exception as e:
                return jsonify({'error': f'Cloudinary upload failed: {str(e)}'}), 500

        # Fallback to local storage if Cloudinary is not configured
        filename = secure_filename(file.filename)
        ts = int(datetime.utcnow().timestamp())
        filename = f"{ts}_{filename}"
        
        # Determine upload dir relative to app root (one level up from app package)
        # Assuming app/ is where this blueprint is, and we want uploads in project_root/Imagenes/uploads
        project_root = os.path.dirname(current_app.root_path) 
        base_upload_dir = os.path.join(project_root, 'Imagenes', 'uploads')

        # Check for tenant_slug to organize images by tenant
        tenant_slug = request.args.get('tenant_slug') or request.form.get('tenant_slug')
        if tenant_slug:
            # Sanitize slug (alphanumeric + hyphens/underscores)
            safe_slug = "".join([c for c in tenant_slug if c.isalnum() or c in ('-','_')])
            upload_dir = os.path.join(base_upload_dir, safe_slug)
            url_prefix = f'Imagenes/uploads/{safe_slug}'
        else:
            upload_dir = base_upload_dir
            url_prefix = 'Imagenes/uploads'

        os.makedirs(upload_dir, exist_ok=True)
        
        filepath = os.path.join(upload_dir, filename)
        file.save(filepath)
        return jsonify({'url': f'{url_prefix}/{filename}'})
    
    return jsonify({'error': 'Upload failed'}), 500

@bp.route('/delete_file', methods=['DELETE'])
def delete_file():
    if not is_authed(): return jsonify({'error': 'no autorizado'}), 401
    path = request.args.get('path')
    if not path:
        payload = request.get_json(silent=True) or {}
        path = payload.get('path')
    if not path: return jsonify({'error': 'path requerido'}), 400
    
    path = str(path).strip()
    if '..' in path or not path.replace('\\', '/').startswith('Imagenes/uploads/'):
        return jsonify({'error': 'ruta inválida o prohibida'}), 400
    
    project_root = os.path.dirname(current_app.root_path)
    full_path = os.path.join(project_root, path)
    
    if os.path.exists(full_path) and os.path.isfile(full_path):
        try:
            os.remove(full_path)
            return jsonify({'ok': True})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'archivo no encontrado'}), 404
