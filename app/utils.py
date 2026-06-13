import secrets
import time
import json
import io
import zipfile
import unicodedata
import re
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape as _xml_escape
from flask import session, request
from app.database import get_db

# Simple in-memory cache: {slug: (config_dict, timestamp)}
_config_cache = {}
CACHE_TTL = 300  # 5 minutes

def get_cached_tenant_config(slug):
    now = time.time()
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
        
    return {}

def invalidate_tenant_config(slug):
    if slug in _config_cache:
        del _config_cache[slug]

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
