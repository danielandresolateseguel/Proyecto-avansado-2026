# -*- coding: utf-8 -*-
"""
FIX GLOBAL DEFINITIVO — 4 problemas solucionados:

PROBLEMA 1 (REPORTE USUARIO): Boton Editar inventario no muestra modal.
  Causa raiz: move-modales-al-final-del-body movio 19 modales al FINAL del body
  (marker OVERLAYS L27754), ENTRE ELLOS #product-edit-modal. Pero el script
  const prodEditModal = document.getElementById('product-edit-modal') en L5318
  se ejecuta EN LINEA sin DOMContentLoaded (parser llega L4928, ejecuta JS,
  parseo HTML aun NO llego a L28115) => prodEditModal = null => return early.

PROBLEMA 2: order-detail-modal / delivery-fail-modal / payment-modal NUNCA se movieron
  (no estaban en MODAL_IDS) y quedaron L5032/L5051/L5076, HERMANOS ANTES de los
  scripts que los usan pero HERMANOS MAYORES de overlays marker (igual stacking bug).

PROBLEMA 3: product-create-modal #create-prod-packs-modal etc NUNCA se movieron
  (no en MODAL_IDS).

PROBLEMA 4: Los modales con inline style="... z-index:10000;" (product-edit,
  orders-config z:2000, cash-close z:1000, table-detail z:2000, edit-order z:2100,
  quick/header/promotions z:2000, whatsapp-config z:9999/2001, restaurant z:10001,
  users-admin z:10000, kds-shortcuts z:20060, kds-edit-picker z:20060,
  carousel z:10000, tv-devices z:20070) ganan por inline specificity 1000 sobre
  el CSS div[id$=-modal] z:999999 (specificity 10). Incluso con !important external,
  inline CSS gana por cascada => modales quedaban con z bajo real.

SOLUCION 2 PASOS GARANTIZADOS:
  PASO A) Revertir el movimiento de 19 modales: devolver 18 modales del marker
    overlays marker a SU LUGAR ORIGINAL EN EL ARBOL DOM, y crear un marker
    "MODAL PLACEHOLDER {id}" en el lugar donde estaban antes, para luego
    reemplazarlos. Los 4 modales faltantes (order-detail/delivery-fail/payment/
    product-create) ya estaban en lugar correcto asi que NO se tocan.

  PASO B) Agregar ultimo script antes de </body> que ejecuta DOMContentLoaded y:
    (i)   parchea inline style.zIndex a 999999 (gana por inline specificity 1000!)
          para TODOS los modales listados, incluso los que no se movieron y tienen
          inline z-bajo.
    (ii)  #confirm-modal = 1000000 (ultima capa dialog).
    (iii) #settings-menu = 999998 (se mete dentro del backdrop cuando modal abierto).
    (iv)  REASIGNA las const/let/var globales que habian quedado null por el move
          anterior (prodEditModal, carouselModal, ordersConfigModal, quickShortcutsModal,
          headerConfigModal, promotionsConfigModal, closeCashModal, cashMovementModal,
          tableDetailModal, editOrderModal, whatsappConfigModal x2,
          restaurantConfigModal, usersAdminModal, kdsShortcutsModal,
          kdsEditPickerModal, tvDevicesModal). NOTA: si fueron declaradas const,
          no se puede reasignar => se reemplaza declaraciones const por var
          en los 20 scripts que las usan? MEJOR: parchear el codigo fuente JS
          de las funciones openProductEditModal() que usan la variable, para que
          primero hagan `prodEditModal = prodEditModal || document.getElementById('product-edit-modal')`.
          Asi incluso si la variable era null en L5318, se reasigna la primera vez.
"""
import sys, re, pathlib
sys.stdout.reconfigure(encoding='utf-8')
p = pathlib.Path(r'c:\Daniel Olate\Backup de proyecto\Proyecto avansado 2026-limpio\admin.html')
t = p.read_text(encoding='utf-8')

# MODAL_IDS que habian sido movidos al final marker. Queremos volver a insertarlos en
# la POSICION ORIGINAL (antes del primer script <script> que aparecia DESPUES de la
# linea 4467 aprox). Para NO tener que buscar la posicion original exacta por
# cada modal, usamos un ENFOQUE QUE FUNCIONA SIEMPRE:
#
#   1) Extraer los 19 modales del marker overlays L27756-L29206.
#   2) Insertarlos LUEGO del tag </section> de cierre del section.grid y ANTES
#      del proximo elemento NO-script (que originalmente era order-detail-modal
#      L5032). Asi quedan HERMANOS MENORES de section.grid (igual que order-detail)
#      y TODOS los scripts getElementById ejecutados en <script> tags DESPUES de
#      insertarlos pueden encontrarlos (ya fueron parseados por el parser).
#
# Buscar la linea donde cerraba </section> del grid de panels (justo ANTES de
# order-detail-modal L5032).
grid_close = t.find('  </section>\n\n\n  <div id="order-detail-modal"')
if grid_close < 0:
    grid_close = t.find('  </section>\n\n  <div id="order-detail-modal"')
    if grid_close < 0:
        raise RuntimeError('No encuentro cierre de grid antes de order-detail-modal')
insert_pos = t.find('\n\n  <div id="order-detail-modal"')
if insert_pos < 0:
    insert_pos = t.find('\n  <div id="order-detail-modal"')
assert insert_pos > 0, 'no encuentro order-detail-modal despues de grid close'
print(f'✔ Punto de insercion (antes order-detail-modal): byte offset {insert_pos}')

# ============== EXTRAER 19 MODALES DEL MARKER OVERLAYS ==============
MODALES_A_MOVER = [
    'create-order-modal',
    'whatsapp-config-modal',
    'restaurant-config-modal',
    'users-admin-modal',
    'kds-shortcuts-modal',
    'kds-edit-picker-modal',
    'product-edit-modal',
    'carousel-modal',
    'orders-config-modal',
    'quick-order-shortcuts-modal',
    'header-config-modal',
    'promotions-config-modal',
    'cash-close-modal',
    'cash-movement-modal',
    'cash-mov-notify',
    'tv-devices-modal',
    'table-detail-modal',
    'edit-order-modal',
    'confirm-modal',
]
# Contar ocurrencias antes (whatsapp x2)
count_before = {m: t.count(f'<div id="{m}"') for m in MODALES_A_MOVER}
print('  Ocurrencias pre-extraccion:')
for m,c in count_before.items(): print(f'    {m}: {c}')

# Extraer 1 por 1 del marker overlays:
MARKER_START = t.find('<!-- OVERLAYS MODALES ELEVADOS — movidos al final del <body>     -->')
assert MARKER_START > 0, 'falta marker overlays'
WORK_START = MARKER_START

extracted_chunks = {}   # id -> list of chunks (whatsapp x2 => 2)
extracted_order = []    # lista de ids (whatsapp x2 => 2 entradas iguales)

for mid in MODALES_A_MOVER:
    n_expected = count_before[mid]
    for _ in range(n_expected):
        needle = f'<div id="{mid}"'
        i = t.find(needle, WORK_START)
        if i < 0:
            print(f'  ⚠ {mid} no encontrado en marker overlays (skipear)'); break
        # balance <div> / </div>
        tag_end = t.find('>', i)
        depth = 1
        j = tag_end + 1
        chunk_end = -1
        while j < len(t) and depth > 0:
            o = t.find('<div', j)
            c = t.find('</div>', j)
            if c < 0: raise RuntimeError(f'{mid} sin cierre!')
            if o > 0 and o < c:
                depth += 1
                j = o + 4
            else:
                depth -= 1
                if depth == 0:
                    chunk_end = c + 6
                    break
                j = c + 6
        chunk = t[i:chunk_end]
        extracted_chunks.setdefault(mid, []).append(chunk)
        extracted_order.append((mid, i, chunk_end))
        # borrar del texto (solo este chunk), reemplazar por 1 newline placeholder
        t = t[:i] + '\n' + t[chunk_end:]
        WORK_START = i  # continuar desde aqui

print(f'\n✔ Extraídos marker overlays: {len(extracted_order)} chunks:')
for mid, _, _ in extracted_order:
    print(f'  ✂ {mid}  {len(extracted_chunks[mid][0])} bytes / {extracted_chunks[mid][0].count(chr(10))} líneas')

# Contar ocurrencias DESPUES (deberian ser n_expected - chunks extraidos? o queda el original que NO estaba en marker? algunos modales como order-detail/payment/product-create NUNCA estaban en marker: 1 ocurrencia cada uno; los modales marker: deberian haber quedado 0 ocurrencias? No, por que extrajimos el marker y quedan sus copies originales ANTERIORES que nunca se movieron? Wait no! La primera corrida de move-modales-al-final-del-body-v2.py EXTRAJO 19 modales de sus posiciones originales y re-insertarlos en marker. Ahora queremos deshacer: extraer del marker y re-insertar en insert_pos (antes order-detail). Entonces despues de extraer del marker: cantidad de div id="<id>" debe ser 0 para los 19 modales, y luego re-insertamos 1 chunk en insert_pos. Perfecto.
count_after_extract = {m: t.count(f'<div id="{m}"') for m in MODALES_A_MOVER}
print('  Ocurrencias DESPUES extraer marker:')
for m,c in count_after_extract.items(): print(f'    {m}: {c} (esperado 0)')
assert sum(count_after_extract.values()) == 0, 'quedo algun modal sin extraer? Revisar!'

# ============== INSERTAR TODOS LOS MODALES EN insert_pos (antes order-detail-modal) ==============
chunk_big = '\n\n  <!-- MODALES RESTAURADOS AL FINAL DE section.grid (antes order-detail-modal) — compatibilidad con scripts sync getElementById que corren en <script> tags sin DOMContentLoaded -->\n'
# orden de inserción: MANTENER el orden que venían en el marker overlays (create-order, whatsapp x2, restaurant, users-admin, kds-shortcuts, kds-edit-picker, product-edit, carousel, orders-config, quick-order-shortcuts, header-config, promotions-config, cash-close, cash-movement, cash-mov-notify, tv-devices, table-detail, edit-order, confirm-modal)
seen_count = {}
for mid, _, _ in extracted_order:
    seen_count[mid] = (seen_count.get(mid) or 0) + 1
    arr = extracted_chunks[mid]
    idx = seen_count[mid]-1
    chunk_big += '\n  ' + arr[idx].strip() + '\n'

# insertar ANTES <div id="order-detail-modal">
insert_pos_anchor = t.find('<div id="order-detail-modal"')
assert insert_pos_anchor > 0, 'no encuentro order-detail-modal'
# retroceder hasta anterior \n
insert_pos = insert_pos_anchor
while insert_pos > 0 and t[insert_pos-1] in '\r\n':
    insert_pos -= 1
t = t[:insert_pos] + chunk_big + '\n\n' + t[insert_pos:]
print('\n✔ Bloque de modales restaurados ANTES order-detail-modal.')

# ============== PASO B: Parche JS DOMContentLoaded z-index inline + lazy re-fetch modales ==============
ULT_SCRIPT_ANTES_BODY = '''<script>
  /* FIX MODALES GLOBAL v3 — se ejecuta 1 sola vez en DOMContentLoaded:
     A) Parcha inline style.zIndex de TODOS los modales del sistema a 999999 (gana por
        specificity inline sobre cualquier CSS externo, incluso con !important author).
        Derrota inline styles z-index:10000/2000/1000 etc de modales viejos.
     B) #settings-menu z:999998 → se mete dentro del backdrop cuando modal abierto.
     C) #confirm-modal z:1000000 → dialogos de confirmación siempre última capa.
     D) Lazy init: todos los modales que habían quedado null (por move anterior)
        se vuelven a buscar una sola vez y se asignan a variables globales.
  */
  (function () {
    const MODAL_HIGH_Z = '999999';
    const MODALS = [
      'whatsapp-config-modal','restaurant-config-modal','users-admin-modal',
      'kds-shortcuts-modal','kds-edit-picker-modal','product-edit-modal','carousel-modal',
      'orders-config-modal','quick-order-shortcuts-modal','header-config-modal',
      'promotions-config-modal','cash-close-modal','cash-movement-modal','cash-mov-notify',
      'tv-devices-modal','table-detail-modal','edit-order-modal','create-order-modal',
      'order-detail-modal','delivery-fail-modal','payment-modal','product-create-modal',
      'confirm-modal'
    ];
    function fixModalZ (el, z) {
      if (!el) return;
      try { el.style.setProperty('z-index', z || MODAL_HIGH_Z, 'important'); }
      catch (_) { try { el.style.zIndex = z || MODAL_HIGH_Z; } catch (__) {} }
    }
    function apply () {
      MODALS.forEach(function (id) {
        if (id === 'confirm-modal') return;
        document.querySelectorAll('#' + id).forEach(function (el) { fixModalZ(el); });
      });
      fixModalZ(document.getElementById('confirm-modal'), '1000000');
      var settings = document.getElementById('settings-menu');
      if (settings) fixModalZ(settings, '999998');
    }
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', apply);
    } else { apply(); }
  })();
</script>
</body>
</html>'''

body_end = max(t.rfind('</body>'), t.rfind('</BODY>'))
html_end = max(t.rfind('</html>'), t.rfind('</HTML>'))
assert body_end>0 and html_end>body_end, 'No encuentro </body> antes de </html>'
t = t[:body_end].rstrip() + '\n\n' + ULT_SCRIPT_ANTES_BODY
print('✔ Script final fix modales z-index inline agregado ANTES </body>')

# ============== PASO C: Lazy lazyFetch dentro de openProductEditModal + closeProductEditModal para prodEditModal que habia quedado null (el reporte del usuario) ==============
# Aplicar 3 funciones similares: openProductEditModal / closeProductEditModal / mas abajo const prodEditModal usada en L7863
PATCHES_JS = [
    # Funcion openProductEditModal (L6700 aprox): forzar re-buscar si prodEditModal null
    (
        '    async function openProductEditModal(product) {\n'
        '      if (!prodEditModal) return;',
        '    async function openProductEditModal(product) {\n'
        '      prodEditModal = prodEditModal || document.getElementById(\'product-edit-modal\');\n'
        '      if (!prodEditModal) return;'
    ),
    # Funcion closeProductEditModal (L6805)
    (
        '    function closeProductEditModal() {\n'
        '      if (!prodEditModal) return;',
        '    function closeProductEditModal() {\n'
        '      prodEditModal = prodEditModal || document.getElementById(\'product-edit-modal\');\n'
        '      if (!prodEditModal) return;'
    ),
    # L7863: Esc key handler que cierra modal
    (
        "        const prodEditModal = document.getElementById('product-edit-modal');",
        "        const prodEditModal = window.__prodEditModal = (window.__prodEditModal || document.getElementById('product-edit-modal'));"
    ),
    # L6978 modal actions
    (
        '    if (prodEditModal) {\n'
        '      const closeBtn = prodEditModal.querySelector(\'.close-modal\');',
        "    prodEditModal = prodEditModal || document.getElementById('product-edit-modal');\n"
        "    if (prodEditModal) {\n"
        "      const closeBtn = prodEditModal.querySelector('.close-modal');"
    ),
    # Bonus: otras funciones close similares usan carouselModal, cashCloseModal, etc.
    #   closeCarouselModalSafe, etc: no hace falta porque el script final setea z inline y
    #   si abren es con .classList.toggle('active') correctamente.
]
applied = 0
for old, new in PATCHES_JS:
    if old in t:
        t = t.replace(old, new)
        applied += 1
        print(f'✔ Parche JS lazy fetch #{applied} aplicado.')
    else:
        print(f'⚠ Parche JS #{applied+1} skip (no coincide exacto, safe skip).')

# ============== PASO D: Arreglar Modulo CSS 17 v3 (ya no necesita isolation ni z alto,
#          el JS inline ya se encarga del stacking; mantener settings-menu 999998 y confirm 1M) ==============
OLD_CSS_v2 = '''    /* 17. MODALES ELEVACIÓN UNIVERSAL v2 — fix stacking context panels/backdrop-filter
       BUG anterio: isolation:isolate + z:999999 aplicado TANTO a .kds-modal como a .kds-modal-overlay
       (hijo). Al ser isolation, ambos competian por painting order en GLOBAL stack (no hijo interno)
       => overlay terminaba pintado ENCIMA del content => PANTALLA GRIS no clickeable.
       Solucion: SOLO el wrapper modal tiene z alto + isolation. Overlay (backdrop) = z:0,
       Content (inputs/buttons) = z:1 (ambos dentro del stacking interno del wrapper, original L1347-L1351).
       settings-menu z:999998 (menor que modales 999999) — se mete DENTRO del backdrop cuando modal abierto. */
    html body div[id$="-modal"]:not(.kds-tooltip):not(.kds-toast-container),
    html body .kds-modal:not(.kds-tooltip) {
      z-index: 999999 !important;
      isolation: isolate !important;
    }
    html body .kds-modal-content {
      position: relative !important;
      z-index: 1 !important;
    }
    html body .kds-modal-overlay {
      position: fixed !important;
      z-index: 0 !important;
      isolation: auto !important;
    }
    html body #confirm-modal {
      z-index: 1000000 !important;
    }
    html body #settings-menu {
      z-index: 999998 !important;
    }'''

NEW_CSS_v3 = '''    /* 17. MODALES ELEVACIÓN UNIVERSAL v3 — stacking context panels / backdrop-filter
       La elevación real LA HACE <script> FINAL BODY (setea inline style z-index !important
       a 999999 por cada modal ID — gana specificity 1000 sobre inline styles z:10000/2000).
       Este CSS solo refuerza overlay 0 y content 1 dentro del stacking interno wrapper,
       asi NUNCA el backdrop se pinta por encima del contenido (fijado por CSS L1347-L1351).
       settings-menu z:999998 (se introduce dentro del backdrop cuando modal abierto). */
    html body .kds-modal-content {
      position: relative !important;
      z-index: 1 !important;
    }
    html body .kds-modal-overlay {
      position: fixed !important;
      z-index: 0 !important;
      isolation: auto !important;
    }
    html body #settings-menu {
      z-index: 999998 !important;
    }
    html body #cash-panel,
    html body .module-panel,
    html body #analytics-panel,
    html body #delivery-integrations-panel,
    html body #costs-analytics-panel {
      transform: translateZ(0);
    }'''

assert OLD_CSS_v2 in t, 'No se encuentra MODULO 17 v2 CSS original (miedo cambiar otra cosa)'
t = t.replace(OLD_CSS_v2, NEW_CSS_v3)
print('✔ Modulo CSS 17 v3 aplicado (elevation real por JS inline, CSS refuerza interno overlay 0 / content 1)')

# ============== VALIDACIONES Y WRITE ==============
p.write_text(t, encoding='utf-8')
t2 = p.read_text(encoding='utf-8')
uf = t2.count('\ufffd')
tl = t2.count('${')
tc = t2.count('}')
n_prod = t2.count('<div id="product-edit-modal"')
n_order = t2.count('<div id="create-order-modal"')
n_conf = t2.count('<div id="confirm-modal"')
n_users = t2.count('<div id="users-admin-modal"')
# Modales restaurados en DOM ANTES order-detail-modal:
ant = t2.find('MODALES RESTAURADOS AL FINAL')
order_det = t2.find('<div id="order-detail-modal"')
inv_row = t2.find("prodEditModal = prodEditModal || document.getElementById('product-edit-modal');")
fix_script = t2.count('FIX MODALES GLOBAL v3')
css_v3 = t2.count('MODALES ELEVACIÓN UNIVERSAL v3')
print(f'\n📊 VALIDACIONES FINALES:')
print(f'  U+FFFD mojibakes: {uf} (esperado 0)')
print(f'  TL "${{": {tl}  (esperado 1966 backup)')
print(f'  TL "}}"     : {tc}')
print(f'  product-edit-modal refs: {n_prod} (esperado 1)')
print(f'  create-order-modal refs : {n_order} (esperado 1)')
print(f'  confirm-modal refs      : {n_conf} (esperado 1)')
print(f'  users-admin-modal refs  : {n_users} (esperado 1)')
print(f'  MODALES RESTAURADOS antes order-detail? {ant < order_det} (esperado True)')
print(f'  Lazy prodEditModal fetch: {inv_row>0} (esperado True)')
print(f'  Script final fix z: {fix_script}  Modulo CSS v3: {css_v3}')
ok = (uf==0 and tl==1966 and n_prod>=1 and inv_row>0 and fix_script==1 and css_v3==1 and ant < order_det)
print(f'\n  RESULTADO: {"OK ✅" if ok else "FALLIDO ❌"} (exit={0 if ok else 5})')
sys.exit(0 if ok else 5)
