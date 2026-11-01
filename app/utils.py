import secrets
import time
import json
import hashlib
import io
import math
import zipfile
import unicodedata
import re
import os
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape as _xml_escape
from datetime import datetime, timedelta, timezone
from flask import session, request
from app.database import get_db

# Simple in-memory cache: {slug: (config_dict, timestamp)}
_config_cache = {}
CACHE_TTL = 300  # 5 minutes

def get_cached_tenant_config(slug):
    now = time.time()
    safe_slug = re.sub(r'[^a-zA-Z0-9_\-]', '', str(slug or '').strip())

    FILESYSTEM_FIRST_SLUGS = {"miprueba", "qplato-demo"}
    is_demo_slug = bool(safe_slug and safe_slug in FILESYSTEM_FIRST_SLUGS)

    def _read_filesystem_fallback(slug_key):
        try:
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
            candidate_paths = [
                os.path.join(project_root, 'config', f'{slug_key}.json'),
                os.path.join(os.path.dirname(__file__), '..', 'config', f'{slug_key}.json'),
            ]
            for cfg_path in candidate_paths:
                resolved = os.path.abspath(cfg_path)
                if os.path.isfile(resolved):
                    try:
                        with open(resolved, 'r', encoding='utf-8') as f:
                            return json.load(f)
                    except Exception as e:
                        print(f"Error reading filesystem config {resolved}: {e}")
        except Exception as e:
            print(f"Error resolving fallback config for {slug_key}: {e}")
        return None

    # Demo slugs: SIN CACHE, pero PRIMERO DB, si no existe → JSON filesystem
    # Así el JSON es el valor inicial por defecto, y los cambios desde el panel
    # (que guardan en DB) tienen precedencia y persisten correctamente.
    if is_demo_slug:
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("SELECT config_json FROM tenant_config WHERE tenant_slug = ?", (slug,))
            row = cur.fetchone()
            if row and row[0]:
                try:
                    cfg = json.loads(row[0])
                    if cfg and isinstance(cfg, dict):
                        return cfg
                except Exception:
                    pass
        except Exception as e:
            print(f"Error fetching DB config for demo slug {slug}: {e}")
        fallback = _read_filesystem_fallback(safe_slug)
        return fallback if fallback is not None else {}

    if slug in _config_cache:
        data, ts = _config_cache[slug]
        if now - ts < CACHE_TTL:
            return data

    # Fetch from DB
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT config_json FROM tenant_config WHERE tenant_slug = ?", (slug,))
        row = cur.fetchone()
        if row and row[0]:
            try:
                cfg = json.loads(row[0])
                _config_cache[slug] = (cfg, now)
                return cfg
            except:
                pass
    except Exception as e:
        print(f"Error fetching config for {slug}: {e}")

    # Fallback: read from config/<slug>.json in project root (filesystem demo tenants)
    if safe_slug:
        fallback = _read_filesystem_fallback(safe_slug)
        if fallback is not None:
            _config_cache[slug] = (fallback, now)
            return fallback

    return {}

def invalidate_tenant_config(slug):
    if slug in _config_cache:
        del _config_cache[slug]

def sync_demo_filesystem_config(slug, config_dict, app_root_path=None):
    import re as _re
    safe_slug = _re.sub(r'[^a-zA-Z0-9_\-]', '', str(slug or '').strip())
    FILESYSTEM_SYNC_SLUGS = {"miprueba", "qplato-demo"}
    if not (safe_slug and safe_slug in FILESYSTEM_SYNC_SLUGS):
        return
    if not isinstance(config_dict, dict):
        return
    try:
        import os as _os
        import json as _json
        base = app_root_path or _os.path.dirname(__file__)
        project_root = _os.path.abspath(_os.path.join(base, '..'))
        candidate_paths = [
            _os.path.join(project_root, 'config', f'{safe_slug}.json'),
            _os.path.join(base, '..', 'config', f'{safe_slug}.json'),
        ]
        for cfg_path in candidate_paths:
            resolved = _os.path.abspath(cfg_path)
            if _os.path.isfile(resolved):
                try:
                    with open(resolved, 'r', encoding='utf-8') as f:
                        file_cfg = _json.load(f)
                    if isinstance(file_cfg, dict):
                        for k, v in config_dict.items():
                            file_cfg[k] = v
                        with open(resolved, 'w', encoding='utf-8') as f:
                            _json.dump(file_cfg, f, ensure_ascii=False, indent=2)
                    break
                except Exception as e:
                    print(f"Error syncing filesystem config {resolved}: {e}")
    except Exception as e:
        print(f"Error resolving filesystem sync for {safe_slug}: {e}")

def is_authed():
    return bool(session.get('admin_auth'))


def get_csrf_token():
    token = session.get('csrf_token')
    if not token:
        token = secrets.token_urlsafe(32)
        session['csrf_token'] = token
    return token

def check_csrf():
    token = request.headers.get('X-CSRF-Token') or request.headers.get('X-CSRFToken')
    return token and token == session.get('csrf_token')

def hash_tv_device_token(token):
    raw = str(token or '').strip()
    if not raw:
        return ''
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()

def get_tv_device_token_from_request():
    token = str(request.headers.get('X-TV-Token') or '').strip()
    if token:
        return token
    auth = str(request.headers.get('Authorization') or '').strip()
    if auth.lower().startswith('bearer '):
        return auth[7:].strip()
    return ''

def get_tv_device_auth(touch=False, kind='tv_kitchen'):
    token = get_tv_device_token_from_request()
    if not token:
        return None
    token_hash = hash_tv_device_token(token)
    if not token_hash:
        return None
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, tenant_slug, device_kind, device_name, device_label, pairing_status,
                   pending_device_token, linked_by, linked_role, created_at, linked_at,
                   last_seen_at, revoked_at, revoked_by, meta_json
            FROM tv_devices
            WHERE device_token_hash = ?
              AND pairing_status = 'active'
              AND COALESCE(revoked_at, '') = ''
            LIMIT 1
            """,
            (token_hash,)
        )
        row = cur.fetchone()
        if not row:
            return None
        device = dict(row)
        if kind and str(device.get('device_kind') or '').strip().lower() != str(kind).strip().lower():
            return None
        if touch:
            try:
                now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
                cur.execute("UPDATE tv_devices SET last_seen_at = ? WHERE id = ?", (now, device.get('id')))
                conn.commit()
                device['last_seen_at'] = now
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
        return device
    except Exception:
        return None

def slugify_simple(value):
    text = str(value or '').strip().lower()
    if not text:
        return ''
    text = unicodedata.normalize('NFD', text)
    text = ''.join(ch for ch in text if unicodedata.category(ch) != 'Mn')
    out = []
    prev_dash = False
    for ch in text:
        if ch.isalnum():
            out.append(ch)
            prev_dash = False
        else:
            if not prev_dash:
                out.append('-')
                prev_dash = True
    return ''.join(out).strip('-')

_XLSX_CELL_REF_RE = re.compile(r'^([A-Z]+)([0-9]+)$')

def _xlsx_col_to_index(col_letters):
    s = str(col_letters or '').strip().upper()
    idx = 0
    for ch in s:
        if not ('A' <= ch <= 'Z'):
            continue
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return max(0, idx - 1)

def _xlsx_ref_to_rowcol(cell_ref):
    m = _XLSX_CELL_REF_RE.match(str(cell_ref or '').strip().upper())
    if not m:
        return None
    col_letters, row_digits = m.group(1), m.group(2)
    try:
        row_idx = int(row_digits) - 1
    except Exception:
        return None
    return (row_idx, _xlsx_col_to_index(col_letters))

def _xlsx_parse_shared_strings(xml_bytes):
    if not xml_bytes:
        return []
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return []
    out = []
    for si in root.findall('.//{*}si'):
        parts = []
        for t in si.findall('.//{*}t'):
            if t.text is not None:
                parts.append(str(t.text))
        out.append(''.join(parts))
    return out

def _xlsx_parse_sheet(xml_bytes, shared_strings):
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return []
    rows_out = []
    max_col = -1
    row_cells = {}
    for c in root.findall('.//{*}sheetData/{*}row/{*}c'):
        ref = c.attrib.get('r') or ''
        rc = _xlsx_ref_to_rowcol(ref)
        if not rc:
            continue
        r_idx, c_idx = rc
        t = (c.attrib.get('t') or '').strip()
        value = None
        if t == 'inlineStr':
            parts = []
            for el in c.findall('.//{*}t'):
                if el.text is not None:
                    parts.append(str(el.text))
            value = ''.join(parts)
        else:
            v = c.find('{*}v')
            raw = (v.text if v is not None else None)
            if raw is None:
                value = None
            elif t == 's':
                try:
                    value = shared_strings[int(raw)]
                except Exception:
                    value = str(raw)
            elif t == 'b':
                value = True if str(raw).strip() == '1' else False
            else:
                value = str(raw)
        row_cells.setdefault(r_idx, {})[c_idx] = value
        if c_idx > max_col:
            max_col = c_idx
    if max_col < 0:
        return []
    max_col += 1
    max_row = max(row_cells.keys()) if row_cells else -1
    for r in range(0, max_row + 1):
        cols = row_cells.get(r, {})
        row = [None] * max_col
        for c_idx, value in cols.items():
            if 0 <= c_idx < max_col:
                row[c_idx] = value
        rows_out.append(row)
    while rows_out and all((v is None or str(v).strip() == '') for v in rows_out[-1]):
        rows_out.pop()
    return rows_out

def read_xlsx_sheets(file_bytes):
    bio = io.BytesIO(file_bytes or b'')
    try:
        zf = zipfile.ZipFile(bio)
    except Exception as e:
        raise ValueError('archivo .xlsx inválido') from e
    try:
        workbook_xml = zf.read('xl/workbook.xml')
    except Exception as e:
        raise ValueError('archivo .xlsx inválido (workbook)') from e
    try:
        rels_xml = zf.read('xl/_rels/workbook.xml.rels')
    except Exception:
        rels_xml = b''
    shared_strings = []
    try:
        shared_strings = _xlsx_parse_shared_strings(zf.read('xl/sharedStrings.xml'))
    except Exception:
        shared_strings = []
    rel_map = {}
    if rels_xml:
        try:
            rel_root = ET.fromstring(rels_xml)
            for rel in rel_root.findall('.//{*}Relationship'):
                rid = rel.attrib.get('Id') or ''
                target = rel.attrib.get('Target') or ''
                if rid and target:
                    rel_map[rid] = target
        except Exception:
            rel_map = {}
    try:
        wb_root = ET.fromstring(workbook_xml)
    except Exception as e:
        raise ValueError('archivo .xlsx inválido (workbook parse)') from e
    sheets = []
    for sh in wb_root.findall('.//{*}sheets/{*}sheet'):
        name = sh.attrib.get('name') or ''
        rid = sh.attrib.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id') or sh.attrib.get('r:id') or ''
        if not name or not rid:
            continue
        target = rel_map.get(rid) or ''
        if not target:
            continue
        target = target.lstrip('/')
        if not target.startswith('xl/'):
            target = 'xl/' + target
        sheets.append((name, target))
    out = {}
    for name, path in sheets:
        try:
            xml_bytes = zf.read(path)
        except Exception:
            continue
        out[name] = _xlsx_parse_sheet(xml_bytes, shared_strings)
    return out

def _xlsx_index_to_col_letters(idx):
    n = int(idx) + 1
    out = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out.append(chr(ord('A') + rem))
    return ''.join(reversed(out)) or 'A'

def _xlsx_sheet_xml(rows):
    rows = rows or []
    lines = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">',
        '<sheetData>'
    ]
    for r_idx, row in enumerate(rows, start=1):
        row = row or []
        lines.append(f'<row r="{r_idx}">')
        for c_idx, cell in enumerate(row, start=1):
            if cell is None:
                continue
            text = str(cell)
            if text == '':
                continue
            col_letters = _xlsx_index_to_col_letters(c_idx - 1)
            ref = f'{col_letters}{r_idx}'
            safe = _xml_escape(text)
            lines.append(f'<c r="{ref}" t="inlineStr"><is><t>{safe}</t></is></c>')
        lines.append('</row>')
    lines.extend(['</sheetData>', '</worksheet>'])
    return '\n'.join(lines).encode('utf-8')

def create_xlsx_bytes(sheets):
    if not isinstance(sheets, list) or not sheets:
        raise ValueError('sheets inválido')
    safe_sheets = []
    for sh in sheets:
        if not isinstance(sh, dict):
            continue
        name = str(sh.get('name') or '').strip()
        if not name:
            continue
        name = name[:31]
        rows = sh.get('rows') or []
        safe_sheets.append({'name': name, 'rows': rows})
    if not safe_sheets:
        raise ValueError('sheets inválido')
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        overrides = [
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        ]
        for i in range(len(safe_sheets)):
            overrides.append(
                f'<Override PartName="/xl/worksheets/sheet{i+1}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            )
        content_types = '\n'.join([
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">',
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>',
            '<Default Extension="xml" ContentType="application/xml"/>',
            *overrides,
            '</Types>'
        ]).encode('utf-8')
        zf.writestr('[Content_Types].xml', content_types)
        root_rels = '\n'.join([
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">',
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>',
            '</Relationships>'
        ]).encode('utf-8')
        zf.writestr('_rels/.rels', root_rels)
        wb_sheets_xml = []
        wb_rels_xml = []
        for i, sh in enumerate(safe_sheets, start=1):
            sid = f'rId{i}'
            wb_sheets_xml.append(f'<sheet name="{_xml_escape(sh["name"])}" sheetId="{i}" r:id="{sid}"/>')
            wb_rels_xml.append(
                f'<Relationship Id="{sid}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{i}.xml"/>'
            )
        workbook_xml = '\n'.join([
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">',
            '<sheets>',
            *wb_sheets_xml,
            '</sheets>',
            '</workbook>'
        ]).encode('utf-8')
        zf.writestr('xl/workbook.xml', workbook_xml)
        wb_rels = '\n'.join([
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">',
            *wb_rels_xml,
            '</Relationships>'
        ]).encode('utf-8')
        zf.writestr('xl/_rels/workbook.xml.rels', wb_rels)
        for i, sh in enumerate(safe_sheets, start=1):
            zf.writestr(f'xl/worksheets/sheet{i}.xml', _xlsx_sheet_xml(sh.get('rows') or []))
    return bio.getvalue()


def _safe_cost_int(v, default=0):
    if v is None:
        return default
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return max(0, v)
    if isinstance(v, float):
        try:
            if v == v:
                return max(0, int(round(v)))
        except Exception:
            pass
        return default
    s = str(v).strip().replace(',', '').replace('.', '')
    if not s:
        return default
    try:
        return max(0, int(s))
    except Exception:
        return default


def normalize_recipe_breakdown(raw):
    if raw is None:
        return None
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        try:
            raw = json.loads(s)
        except Exception as e:
            raise ValueError(f'escandallo JSON inválido: {e}')
    if not isinstance(raw, dict):
        raise ValueError('escandallo debe ser un objeto (dict)')
    version = int(raw.get('version') or 1)
    if version < 1:
        version = 1
    yield_servings = int(raw.get('yield_servings') or 1)
    if yield_servings < 1:
        yield_servings = 1
    currency = str(raw.get('currency') or 'ARS').strip().upper() or 'ARS'
    prep_minutes_raw = raw.get('prep_minutes')
    try:
        prep_minutes = max(0, int(prep_minutes_raw or 0))
    except Exception:
        prep_minutes = 0
    labor_cost_per_minute = _safe_cost_int(raw.get('labor_cost_per_minute'), 0)
    auto_sync = True
    if 'auto_sync_to_product_cost' in raw:
        auto_sync = bool(raw['auto_sync_to_product_cost'])
    ings_raw = raw.get('ingredients') or []
    ingredients = []
    ingredients_subtotal = 0
    if not isinstance(ings_raw, list):
        raise ValueError('ingredients debe ser una lista')
    for idx, ing in enumerate(ings_raw):
        if not isinstance(ing, dict):
            raise ValueError(f'el ingrediente fila {idx+1} no es un objeto')
        name = str(ing.get('name') or '').strip()
        if not name:
            raise ValueError(f'el ingrediente fila {idx+1} no tiene nombre')
        try:
            qty = float(ing.get('qty') or 0)
        except Exception:
            raise ValueError(f'cantidad inválida en ingrediente: {name}')
        if qty < 0:
            qty = 0.0
        unit = str(ing.get('unit') or 'un').strip() or 'un'
        unit_cost = _safe_cost_int(ing.get('unit_cost'), 0)
        if isinstance(ing.get('total_line'), (int, float)) and ing.get('total_line') is not None:
            try:
                user_total = int(round(float(ing.get('total_line'))))
                if user_total >= 0:
                    line_total = user_total
                else:
                    line_total = int(round(qty * unit_cost))
            except Exception:
                line_total = int(round(qty * unit_cost))
        else:
            line_total = int(round(qty * unit_cost))
        ingredients_subtotal += int(line_total)
        entry = {
            'ingredient_id': str(ing.get('ingredient_id') or '').strip(),
            'name': name,
            'qty': qty,
            'unit': unit,
            'unit_cost': unit_cost,
            'total_line': int(line_total),
            'notes': str(ing.get('notes') or '').strip()
        }
        for extra_key in ('supplier', 'brand', 'waste_percent'):
            if extra_key in ing and ing[extra_key] is not None:
                entry[extra_key] = ing[extra_key]
        ingredients.append(entry)
    other_vars_raw = raw.get('other_variables') or []
    other_variables = []
    other_vars_subtotal = 0
    if isinstance(other_vars_raw, list):
        for idx, ov in enumerate(other_vars_raw):
            if not isinstance(ov, dict):
                continue
            name_ov = str(ov.get('name') or '').strip()
            if not name_ov:
                continue
            total_ov = _safe_cost_int(ov.get('total'), 0)
            other_vars_subtotal += total_ov
            other_variables.append({'name': name_ov, 'total': total_ov})
    labor_subtotal = prep_minutes * labor_cost_per_minute
    calculated_total_cost = int(ingredients_subtotal) + int(other_vars_subtotal) + int(labor_subtotal)
    unit_serving_cost = 0
    if yield_servings > 0 and calculated_total_cost > 0:
        try:
            unit_serving_cost = int(round(calculated_total_cost / float(yield_servings)))
        except Exception:
            unit_serving_cost = int(calculated_total_cost)
    else:
        unit_serving_cost = int(calculated_total_cost)
    recipe = {
        'version': version,
        'currency': currency,
        'yield_servings': yield_servings,
        'ingredients': ingredients,
        'other_variables': other_variables,
        'ingredients_subtotal': int(ingredients_subtotal),
        'other_vars_subtotal': int(other_vars_subtotal),
        'prep_minutes': int(prep_minutes),
        'labor_cost_per_minute': int(labor_cost_per_minute),
        'labor_subtotal': int(labor_subtotal),
        'calculated_total_cost': int(calculated_total_cost),
        'unit_serving_cost': int(unit_serving_cost),
        'auto_sync_to_product_cost': bool(auto_sync)
    }
    return recipe


# ---------------------------------------------------------------------------
# Disponibilidad de pedidos / programación de pedidos (carta online)
# ---------------------------------------------------------------------------

DAY_KEYS = ('mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun')
_WEEKDAY_LABELS = ('Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom')

_HOURS_DAYS_MAP = {
    'mon': 'mon', 'monday': 'mon', 'lunes': 'mon', 'lun': 'mon',
    'tue': 'tue', 'tuesday': 'tue', 'martes': 'tue', 'mar': 'tue',
    'wed': 'wed', 'wednesday': 'wed', 'miercoles': 'wed', 'miércoles': 'wed', 'mie': 'wed', 'mié': 'wed',
    'thu': 'thu', 'thursday': 'thu', 'jueves': 'thu', 'jue': 'thu',
    'fri': 'fri', 'friday': 'fri', 'viernes': 'fri', 'vie': 'fri',
    'sat': 'sat', 'saturday': 'sat', 'sabado': 'sat', 'sábado': 'sat', 'sab': 'sat', 'sáb': 'sat',
    'sun': 'sun', 'sunday': 'sun', 'domingo': 'sun', 'dom': 'sun',
}


def _parse_hours_intervals(value):
    out = []
    if isinstance(value, list):
        for it in value:
            if isinstance(it, list) and len(it) >= 2 and it[0] and it[1]:
                out.append([str(it[0]), str(it[1])])
            elif isinstance(it, str) and '-' in it:
                parts = [p.strip() for p in it.split('-') if p.strip()]
                if len(parts) >= 2:
                    out.append([parts[0], parts[1]])
    elif isinstance(value, str):
        for seg in [s.strip() for s in value.split(',') if s.strip()]:
            if '-' in seg:
                a, b = [p.strip() for p in seg.split('-', 1)]
                if a and b:
                    out.append([a, b])
    return out


def normalize_opening_hours(raw):
    """Normaliza horarios de apertura a {mon..sun: [[inicio, fin], ...]}."""
    if isinstance(raw, dict):
        res = {}
        for k, v in raw.items():
            key = _HOURS_DAYS_MAP.get(str(k).strip().lower())
            if not key:
                continue
            parsed = _parse_hours_intervals(v)
            if parsed:
                res[key] = parsed
        return res
    if isinstance(raw, str):
        parsed = _parse_hours_intervals(raw)
        if parsed:
            return {key: parsed for key in DAY_KEYS}
    return {}


def _hhmm_to_minutes(value):
    parts = str(value or '').strip().split(':')
    if len(parts) < 2:
        return None
    try:
        hours = int(parts[0])
        minutes = int(parts[1])
    except Exception:
        return None
    if hours < 0 or hours > 24 or minutes < 0 or minutes > 59:
        return None
    return hours * 60 + minutes


def resolve_timezone(name):
    tz_name = str(name or '').strip() or 'America/Argentina/Mendoza'
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(tz_name)
    except Exception:
        # Fallback (Windows sin tzdata): Argentina no aplica horario de verano.
        return timezone(timedelta(hours=-3))


def get_order_settings(cfg):
    """Configuración de pedidos del comercio con valores por defecto seguros."""
    cfg = cfg if isinstance(cfg, dict) else {}
    raw = cfg.get('order_settings') if isinstance(cfg.get('order_settings'), dict) else {}

    def _int(key, default, lo, hi):
        try:
            val = int(raw.get(key))
        except Exception:
            val = default
        return max(lo, min(hi, val))

    types = []
    types_raw = raw.get('schedule_order_types')
    if isinstance(types_raw, list):
        for item in types_raw:
            value = str(item or '').strip().lower()
            if value and value not in types:
                types.append(value)
    if not types:
        types = ['direccion', 'espera']
    return {
        'prep_minutes': _int('prep_minutes', 30, 0, 240),
        'slot_step_minutes': _int('slot_step_minutes', 30, 5, 240),
        'max_days_ahead': _int('max_days_ahead', 2, 1, 2),
        'block_when_closed': bool(raw.get('block_when_closed', True)),
        'allow_schedule_when_open': bool(raw.get('allow_schedule_when_open', True)),
        'schedule_order_types': types,
    }


def _tenant_opening_hours(cfg):
    cfg = cfg if isinstance(cfg, dict) else {}
    meta = cfg.get('meta') if isinstance(cfg.get('meta'), dict) else {}
    branding = meta.get('branding') if isinstance(meta.get('branding'), dict) else {}
    contact = branding.get('contact') if isinstance(branding.get('contact'), dict) else {}
    return normalize_opening_hours(cfg.get('opening_hours') or contact.get('opening_hours') or {})


def get_tenant_timezone_name(cfg):
    cfg = cfg if isinstance(cfg, dict) else {}
    meta = cfg.get('meta') if isinstance(cfg.get('meta'), dict) else {}
    branding = meta.get('branding') if isinstance(meta.get('branding'), dict) else {}
    contact = branding.get('contact') if isinstance(branding.get('contact'), dict) else {}
    return str(cfg.get('timezone') or contact.get('timezone') or 'America/Argentina/Mendoza').strip()


def _build_opening_windows(hours, tz, reference_local, days_back, days_ahead):
    windows = []
    for offset in range(-abs(days_back), days_ahead):
        day_date = (reference_local + timedelta(days=offset)).date()
        key = DAY_KEYS[day_date.weekday()]
        for interval in hours.get(key) or []:
            start_min = _hhmm_to_minutes(interval[0])
            end_min = _hhmm_to_minutes(interval[1])
            if start_min is None or end_min is None:
                continue
            start_min = start_min % (24 * 60)
            start = datetime.combine(day_date, datetime.min.time()).replace(tzinfo=tz) + timedelta(minutes=start_min)
            duration = (end_min - start_min) if end_min > start_min else (24 * 60 - start_min + end_min)
            if duration <= 0:
                duration = 24 * 60
            windows.append((start, start + timedelta(minutes=duration)))
    windows.sort(key=lambda w: w[0])
    return windows


def _slot_label(moment, reference_local):
    day_delta = (moment.date() - reference_local.date()).days
    if day_delta == 0:
        prefix = 'Hoy'
    elif day_delta == 1:
        prefix = 'Mañana'
    else:
        prefix = f'{_WEEKDAY_LABELS[moment.weekday()]} {moment.strftime("%d/%m")}'
    return f'{prefix} {moment.strftime("%H:%M")}'


def _availability_payload(slug, tz_name, settings, has_hours, is_open, can_order_now, next_opening, closing_at, slots):
    return {
        'tenant_slug': slug,
        'timezone': tz_name,
        'has_hours': has_hours,
        'is_open': bool(is_open),
        'can_order_now': bool(can_order_now),
        'block_when_closed': bool(settings.get('block_when_closed')),
        'allow_schedule_when_open': bool(settings.get('allow_schedule_when_open')),
        'schedule_order_types': list(settings.get('schedule_order_types') or []),
        'prep_minutes': int(settings.get('prep_minutes') or 0),
        'slot_step_minutes': int(settings.get('slot_step_minutes') or 0),
        'max_days_ahead': int(settings.get('max_days_ahead') or 0),
        'next_opening': next_opening.isoformat() if next_opening else None,
        'closing_at': closing_at.isoformat() if closing_at else None,
        'slots': slots,
    }


def _build_slots(windows, now_local, step, prep):
    slots = []
    for window_start, window_end in windows:
        if window_end <= now_local:
            continue
        base = window_start + timedelta(minutes=prep)
        last = window_end - timedelta(minutes=step)
        if last < base:
            continue
        cursor = max(base, now_local)
        if cursor > base:
            delta_minutes = (cursor - base).total_seconds() / 60.0
            cursor = base + timedelta(minutes=math.ceil(delta_minutes / step) * step)
        while cursor <= last and len(slots) < 300:
            slots.append({
                'iso': cursor.astimezone(timezone.utc).isoformat(),
                'local': cursor.strftime('%H:%M'),
                'date': cursor.date().isoformat(),
                'label': _slot_label(cursor, now_local),
            })
            cursor += timedelta(minutes=step)
    return slots


def compute_order_availability(slug, cfg=None, now=None):
    """Calcula si el local está abierto, hasta cuándo y qué franjas se pueden programar."""
    if cfg is None:
        try:
            cfg = get_cached_tenant_config(slug) or {}
        except Exception:
            cfg = {}
    if not isinstance(cfg, dict):
        cfg = {}
    settings = get_order_settings(cfg)
    hours = _tenant_opening_hours(cfg)
    tz_name = get_tenant_timezone_name(cfg)
    tz = resolve_timezone(tz_name)

    now_utc = now or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    now_local = now_utc.astimezone(tz)

    # Sin horarios configurados no bloqueamos nada: el comercio se asume siempre abierto.
    if not hours:
        return _availability_payload(slug, tz_name, settings, False, True, True, None, None, [])

    step = settings['slot_step_minutes']
    prep = settings['prep_minutes']
    horizon = max(int(settings['max_days_ahead'] or 2), 1)
    schedule_cutoff_local = now_local + timedelta(hours=(24 * horizon))
    scan_days_ahead = horizon + 2

    windows = _build_opening_windows(hours, tz, now_local, 1, scan_days_ahead)
    slots = _build_slots(windows, now_local, step, prep)
    filtered_slots = []
    for slot in slots:
        try:
            slot_dt = datetime.fromisoformat(str(slot.get('iso') or '').replace('Z', '+00:00')).astimezone(tz)
        except Exception:
            continue
        if slot_dt <= schedule_cutoff_local:
            filtered_slots.append(slot)
    slots = filtered_slots

    is_open = any(start <= now_local < end for start, end in windows)
    closing_at = None
    current = [(start, end) for start, end in windows if start <= now_local < end]
    if current:
        closing_at = min(end for _, end in current)
    future = [(start, end) for start, end in windows if start > now_local]
    next_opening = future[0][0] if future else (min((start for start, _ in current), default=None))

    return _availability_payload(
        slug, tz_name, settings, True,
        is_open, is_open, next_opening if not is_open else None, closing_at, slots
    )


def validate_scheduled_slot(availability, requested_iso):
    """Devuelve (datetime UTC, '') si la franja pedida sigue siendo válida; si no (None, error)."""
    raw = str(requested_iso or '').strip()
    if not raw:
        return None, 'Elegí un horario para tu pedido.'
    try:
        requested = datetime.fromisoformat(raw.replace('Z', '+00:00'))
    except Exception:
        return None, 'El horario elegido no es válido.'
    if requested.tzinfo is None:
        requested = requested.replace(tzinfo=timezone.utc)
    requested_utc = requested.astimezone(timezone.utc).replace(second=0, microsecond=0)
    for slot in availability.get('slots') or []:
        try:
            slot_dt = datetime.fromisoformat(str(slot.get('iso') or '').replace('Z', '+00:00'))
        except Exception:
            continue
        if slot_dt.tzinfo is None:
            slot_dt = slot_dt.replace(tzinfo=timezone.utc)
        if abs((slot_dt - requested_utc).total_seconds()) < 60:
            return slot_dt.astimezone(timezone.utc), ''
    return None, 'Ese horario ya no está disponible. Elegí otra franja.'


def apply_recipe_to_product_fields(recipe, current_fields=None):
    current_fields = current_fields or {}
    updates = {}
    if not recipe:
        return updates
    auto_sync = bool(recipe.get('auto_sync_to_product_cost'))
    unit_cost = _safe_cost_int(recipe.get('unit_serving_cost'), 0)
    if auto_sync and unit_cost >= 0:
        updates['cost_price'] = int(unit_cost)
        updates['cost_type'] = 'recipe'
    price = current_fields.get('price')
    if price and int(price) > 0 and updates.get('cost_price'):
        cp = int(updates['cost_price'])
        pr = int(price)
        if pr > 0 and cp <= pr:
            margin = int(round(((pr - cp) / float(pr)) * 100)) if pr else 0
            updates['margin_percent'] = max(0, min(1000, margin))
    return updates
