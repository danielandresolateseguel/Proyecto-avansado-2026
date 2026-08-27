# Plantilla de Alta de Comercio

Usa esta plantilla al iniciar el onboarding de un nuevo comercio. La idea es completar esta ficha antes de tocar config, frontend o paneles.

Si necesitas abrir una tarea operativa para seguimiento interno, usa también [plantilla-ticket-alta-comercio.md](file:///c:/Daniel%20Olate/Backup%20de%20proyecto/Proyecto%20avansado%202026-limpio/plantilla-ticket-alta-comercio.md).

---

## 1. Identificación del comercio

- Fecha de alta:
- Responsable:
- Nombre comercial:
- Slug propuesto:
- Rubro:
- Estado:
  - pendiente
  - en configuración
  - en validación
  - activo

## 2. Datos de contacto

- Teléfono principal:
- WhatsApp de pedidos:
- Instagram:
- Facebook:
- Dirección:
- Horario:

## 3. Dominio y publicación

- URL pública deseada:
- ¿Usa dominio propio o dominio compartido?:
- HTML público base a reutilizar:
- ¿Cloudflare configurado?:
  - sí
  - no
- ¿SSL/TLS `Full (strict)` activo?:
  - sí
  - no

## 4. Checkout y operación

- Modo de checkout:
  - mesa
  - direccion
  - espera
  - whatsapp
- ¿Tiene retiro en local?:
- ¿Tiene delivery?:
- Costo base de envío:
- Tiempo estimado mesa:
- Tiempo estimado espera:
- Tiempo estimado delivery:
- SLA advertencia:
- SLA crítico:

## 4b. Integraciones de Delivery (Pedidos Ya / Rappi / etc.)

- ¿Tiene integrado Pedidos Ya hoy?:
  - no
  - sí, manual (panel)
  - sí, API (integración nativa)
- ¿Tiene integrado Rappi hoy?:
  - no
  - sí, manual
  - sí, API
- ¿Otros canales? (Cuotasí, Glovo, etc.):
- Ejecutivo de cuenta de Pedidos Ya (nombre / contacto):
- % Comisión pactado con Pedidos Ya:
- % Comisión pactado con Rappi:
- Checklist habilitación API Pedidos Ya — FASE 1 / FASE 2 (Inbound — PedidosYa → Qplato):
  - [ ] Trámite iniciado (portal de Pedidos Ya / ejecutivo)
  - [ ] API habilitada en portal
  - [ ] Credenciales recibidas (Client ID / Merchant ID / API Key / Secret)
  - [ ] Sandbox de Pedidos Ya habilitado
  - [ ] Webhook URL configurada en el portal de Pedidos Ya
        (`https://<TU-HOST-RENDER>/api/webhooks/pedidosya/<tenant_slug>`)
  - [ ] Headers de firma configurados (X-PedidosYa-Signature + opcional X-PedidosYa-Timestamp)
  - [ ] Commission % cargado en Qplato (/api/delivery/config)
  - [ ] Mapeo de IDs de productos (product_id Qplato ↔ SKU Pedidos Ya)
  - [ ] Pedido de prueba en SANDBOX exitoso
  - [ ] Cancelación de prueba en SANDBOX exitosa
  - [ ] Prueba Idempotencia (mismo external_order_id × 3 = 1 sola orden)
  - [ ] Prueba SKUs desconocidos (SKUs no mapeados = order_items NULL cost=0, sin 500)
  - [ ] Conciliación de prueba de un pedido real con reporte de Pedidos Ya

- Checklist FASE 3 — Sync Bidireccional SALIENTE (Qplato → PedidosYa API):
  - [ ] Campos opcionales configurados en `delivery_integrations.pedidosya` (ver tabla abajo):
        (si no los configuras, Qplato usa defaults por defecto apuntando a prod)
  - [ ] `enabled=true` + `api_key` cargado para el tenant
  - [ ] Prueba manual 1: cambiar estado pedido PedidosYa a "preparacion"
        → en order_events aparece `pedidosya_status_sync_out` result=sent HTTP 2xx
  - [ ] Prueba manual 2: cancelar pedido PedidosYa con motivo
        → se envía HTTP body con campo `reason`
  - [ ] Prueba manual 3: dos veces el MISMO status sobre el mismo pedido
        → el 2do envio es SKIP (idempotency: result=skipped duplicate)
  - [ ] Prueba error: apagar internet / usar api_key invalida
        → Qplato NO crashea, NO retorna 5xx al usuario; event log = result=error
  - [ ] Prueba reconexión: con api_key correcta + red OK, nuevo cambio de estado
        → vuelve a enviar correctamente sin crashear

### Campos config FASE 3 (Qplato → PedidosYa)

Todos **opcionales** — si no se configuran, se usan defaults.
Guardados bajo `tenant_config.config_json.delivery_integrations.pedidosya`:

| Campo | Tipo | Default | ¿Para qué sirve? |
|---|---|---|---|
| `enabled` | bool | `false` | Master switch general (ON/OFF) de TODA la integración (inbound + outbound) |
| `api_key` | str | `''` | Bearer token que entrega PedidosYa. **NUNCA commitear a git.** Si empieza con `dev-` = entorno local/debug (webhook bypass firma). |
| `webhook_secret` | str | `''` | Secreto HMAC SHA256 para validar firma X-PedidosYa-Signature inbound. |
| `merchant_id` | str | `''` | Sucursal PedidosYa (se envía como header `X-Merchant-Id` en sync outbound) |
| `commission_percent` | int | `25` | % que cobra PedidosYa por pedido → se guarda en `orders.external_fee_amount` para Rentabilidad |
| `api_base_url` | str | `https://api.pedidosya.com` | URL base del API de PedidosYa (sandbox / producción según entrega PedidosYa) |
| `status_endpoint` | str | `/v1/orders/{external_order_id}/status` | Path del endpoint para actualizar estado; `{external_order_id}` se reemplaza automáticamente |
| `status_http_method` | str | `PATCH` | Método HTTP: `PATCH` / `PUT` / `POST` (depende versión API PedidosYa) |
| `http_timeout_seconds` | int 1..20 | `6` | Timeout máximo en segundos por request sync outbound; **NUNCA superar 20s** para no bloquear la UI del usuario cocina/caja |
| `_version` | int | `1` | Campo libre para auditoría interna (no lo usa el código) |

### Comportamiento FASE 3 que el comercio debe conocer

1. **NUNCA Qplato se rompe si PedidosYa se cae.** Si la API de PedidosYa contesta 5xx, hay timeout, o hay corte de internet, el cambio de estado de Qplato se guarda IGUAL — el usuario no se entera, solo queda un log `result=error` en order_events.
2. **Mapeo de estados automático:**
   - `pendiente` / `por_aprobar` → `PENDING`
   - `confirmado` (webhook inicial) → `CONFIRMED`
   - `preparacion` → `PREPARING`
   - `listo` → `READY_FOR_PICKUP`
   - `en_camino` → `OUT_FOR_DELIVERY`
   - `entregado` → `DELIVERED`
   - `cancelado` → `CANCELLED` + `reason` (motivo del usuario)
3. **No hay dobles envíos.** Idempotencia: si el último cambio de estado para ese pedido fue el Mismo status Y resultó exitoso, el próximo intento se skippea.
4. **Comisiones visibles en Rentabilidad (FASE 5 — YA IMPLEMENTADA).** El campo `external_fee_amount` (25% o % configurado) se usa para:
   - Filtrar Panel Rentabilidad por Canal: Local / PedidosYa / Ambos
   - Calcular Margen Real = Total - CMV - external_fee_amount
   - Mostrar **dos porcentajes** de comisión:
     - *Contractual:* Comisión ÷ Venta bruta PedidosYa (ej: 25%)
     - *Efectiva:* Comisión ÷ Ventas netas al comercio (ej: 30%)
5. **Auditoría 100% de syncs.** Cualquier cosa que pase (skip/envío/error) se graba en `order_events.event_type = 'pedidosya_status_sync_out'` con:
   - `actor` = usuario Qplato que cambió el estado
   - `payload_json` con URL, método, HTTP status, latency_ms, api_key_masked (4prim+4ult asteriscos), motivo de skip/error, response snippet 800 chars si falla
6. **Tiempo máximo de respuesta adicional por cambio de estado:** `http_timeout_seconds` (default 6s). Recomendado no cambiarlo a menos que lo indique PedidosYa.
7. **Deploy a Render:** La URL pública de webhook es `https://<TU-HOST-RENDER>/api/webhooks/pedidosya/<tenant_slug>`. Recordar informársela al ejecutivo de cuenta PedidosYa.


## 4c. FASE 4 — Onboarding Producción PedidosYa (PASO A PASO)

Esta sección la completas con TU CLIENTE (comercio final) para pasar de dev local a entorno real. El comercio DEBE ejecutar estos pasos ya que PedidosYa le entrega credenciales SOLO al titular de la cuenta.

### 📋 Paso 1 — Confirmar contrato y API habilitada (1 día hábil)

- [ ] El comercio **ya firmó el contrato con PedidosYa** y está activo en la app
- [ ] El comercio contacta a su **ejecutivo de cuenta PedidosYa** y le solicita:
  > "Hola, queremos habilitar la INTEGRACIÓN POR API con nuestro sistema de gestión interno (Qplato). Necesitamos que nos habiliten el módulo de Integraciones / Desarrolladores en el Portal Admin de PedidosYa y nos envíen la documentación de la versión actual del API (inbound webhooks + PATCH /v1/orders endpoint)."
- [ ] PedidosYa contesta con: link al Portal Admin + Usuario de desarrollador + Documentación API versión X

### 🔑 Paso 2 — Generar credenciales API en Portal PedidosYa

El comercio (o Qplato acompañando) se loguea en **Portal Administrador PedidosYa** sección **Integraciones / Desarrolladores** y crea una Aplicación nueva:

- [ ] Tipo de integración: `Webhook + REST API` (ambos)
- [ ] Entorno: PRIMERO `Sandbox` (si existe), DESPUÉS `Producción`
- [ ] Credenciales que debe generar y guardar en un lugar SEGURO (NO subir a GitHub/WhatsApp):
  1. **API Key** (Bearer token) — Campo `api_key` Qplato
  2. **Merchant ID / Sucursal ID** — Campo `merchant_id` Qplato
  3. **Webhook Secret (Signing Secret)** — Campo `webhook_secret` Qplato (HMAC SHA256)
  4. **Commission % pactada** (sacar del contrato PedidosYa, por defecto 25) — Campo `commission_percent` Qplato
  5. (Opcional) URL base de API: `https://api.sandbox.pedidosya.com` (sandbox) o `https://api.pedidosya.com` (prod)
- [ ] **Configurar la Webhook URL PÚBLICA en Portal PedidosYa:**
  > URL: `https://<TU-HOST-RENDER>.onrender.com/api/webhooks/pedidosya/<tenant_slug>?sync=1`
  >
  > Ejemplo Planeta Pancho: `https://planeta-pancho.onrender.com/api/webhooks/pedidosya/planeta-pancho?sync=1`
- [ ] **Eventos que debe suscribir la webhook (al menos estos):**
  - `order_created` (nuevo pedido)
  - `order_cancelled` (cancelación PedidosYa)
  - `order_status_changed` (cambios de estado adicionales si existen)
- [ ] Headers de firma: Activar `X-PedidosYa-Signature` y `X-PedidosYa-Timestamp` (si existen)

### ⚙️ Paso 3 — Cargar credenciales en Qplato

Qplato tiene 2 formas. Elegir la opción A (más fácil) si existe la UI de Delivery Config, sino opción B (directo en DB).

**Opción A — UI Qplato Panel Admin (recomendada):**
  - Panel → Configuración → Integraciones → Delivery
  - Dropdown `Proveedor = PedidosYa` → ON
  - Completar 6 campos (api_key, webhook_secret, merchant_id, commission_percent, api_base_url, http_timeout=6)
  - Guardar → muestra `"Sync Outbound: OK"` o `"SKIP credencial vacía"`

**Opción B — Directo en tabla tenant_config (si NO hay UI):**
```sql
UPDATE tenant_config
SET config_json = JSON_SET(COALESCE(config_json, '{}'),
    '$.delivery_integrations.pedidosya.enabled', true,
    '$.delivery_integrations.pedidosya.api_key', '<API_KEY_AQUI>',
    '$.delivery_integrations.pedidosya.webhook_secret', '<WEBHOOK_SECRET_AQUI>',
    '$.delivery_integrations.pedidosya.merchant_id', '<MERCHANT_ID_AQUI>',
    '$.delivery_integrations.pedidosya.commission_percent', 25,
    '$.delivery_integrations.pedidosya.api_base_url', 'https://api.sandbox.pedidosya.com',
    '$.delivery_integrations.pedidosya.http_timeout_seconds', 6,
    '$.delivery_integrations.pedidosya._version', 1)
WHERE tenant_slug = '<tenant_slug>' AND config_key = 'default';
```

### 🧪 Paso 4 — Prueba de conectividad básica (antes de pedidos reales)

- [ ] Desde Postman o similar, enviar un pedido de prueba a la webhook URL Render (con `?sync=1`) usando la misma estructura JSON que envía PedidosYa
- [ ] Respuesta HTTP 200: `{"status":"processed"}`
- [ ] Panel Qplato → Pedidos cerrados / Kanban: aparece el nuevo pedido (orden N° nuevo, source=PedidosYa)
- [ ] Ir al pedido → pestaña Eventos → hay 5+ eventos (webhook_in + created + confirmed, etc.)

### 🔄 Paso 5 — Mapeo de SKUs (CRÍTICO para costeo en Rentabilidad)

Para que **Margen Real y Comisión se calculen CORRECTAMENTE**: el comercio DEBE mapear al menos sus 10 productos top vendidos por PedidosYa a SKUs de Qplato:

- [ ] Exportar listado de SKUs PedidosYa desde el portal (nombre, SKU code, precio)
- [ ] Exportar listado de productos Qplato desde Panel → Inventario → Exportar CSV
- [ ] Relación 1 a 1: `SKU PedidosYa (inbound webhook items.sku) ↔ product.id Qplato`
- [ ] Cargar mapeo en Qplato: Tabla `sku_mapping` o en products.alternate_ids según corresponda
- [ ] **RECOMENDACIÓN POST-ONBOARDING:** 1 vez al mes correr `migrate_costs_backfill.py` para backfillear order_items.unit_cost en todos los pedidos antiguos con unit_cost=0 + product_id conocido. (Arregla el "Margen Bruto 100% falso" en Rentabilidad.)

---

## 4d. FASE 4 — UAT (Pruebas de Aceptación Usuario) FINALES (7 pruebas obligatorias)

Ejecutar ESTAS 7 pruebas EN SANDBOX primero, y luego con 1 pedido real pequeño en PRODUCCIÓN. Requiere dos personas: 1 del comercio que opera PedidosYa App y 1 que opera Panel Qplato.

| # | Prueba | Resultado esperado | ¿Se cumple? |
|---|---|---|---|
| **F4-P1** | **Alta pedido nuevo** desde App PedidosYa cliente | Panel Qplato → Kanban "Nuevos" aparece pedido N° X, source=PedidosYa, items correctos, total correcto | ☐ |
| **F4-P2** | **Cancelación pedido** desde PedidosYa antes de preparar | En Panel Qplato → Estado = Cancelado, order_notes contiene `[CANCEL POR PEDIDOSYA] code=X motivo="..."` + evento `pedidosya_cancelled` | ☐ |
| **F4-P3** | **Idempotencia** (replay webhook 3 veces mismo external_order_id) | En DB → COUNT(id) orders = 1. Ningún pedido duplicado. Respuestas webhook = status=processed 200 OK | ☐ |
| **F4-P4** | **SKU desconocido** en payload (1 o + items con sku falso) | Order creada OK, order_items.product_id=NULL, unit_cost=0, NO crash HTTP 500, KPI Productos sin costo Sube en Rentabilidad | ☐ |
| **F4-P5** | **Comisión correcta** en order recién creado | orders.external_fee_amount = total × (commission_percent÷100) (ej: total $4800 × 25% = $1200) | ☐ |
| **F4-P6** | **Sync Outbound** (Cocina Qplato cambia estado a "Preparación" → "Listo" → "Entregado") | En order_events por cada cambio: 1 evento `pedidosya_status_sync_out` cada uno; result=sent (HTTP 2xx) para API key válida. NO crashea si falla red. | ☐ |
| **F4-P7** | **Rentabilidad Panel F5** (período = mes actual, Canal=PedidosYa) | KPI Comisión Delivery ≈ sum external_fee_amount, Por Canal fila "PedidosYa · Delivery" con sublínea Comisión $, Margen Real $ ≠ Bruto $ | ☐ |

**Troubleshooting rápido (si alguna prueba falla):**

1. **F4-P1 falla (no aparece pedido):** Ir al pedido webhook → Postman reenviar con `?sync=1` → mostrar `_debug` en HTTP response body. Buscar `force_sync_dev=True/False` y `processing_error`.
2. **F4-P5 Comisión 0:** Validar `delivery_integrations.pedidosya.commission_percent` ≠ NULL y `api_key` no empieza con `dev-` (dev mode no calcula fee algunas veces).
3. **F4-P6 Sync Outbound siempre ERROR:** Copiar order_events.payload_json → `curl -X PATCH` el mismo request desde máquina local del cliente. Si falla → firewall/proxy del cliente está bloqueando api.pedidosya.com por TLS 1.2+ o IP. Reportar al ejecutivo.

---

## 4e. FASE 5 — Rentabilidad (Operación diaria post-onboarding)

Puntos que el comercio debe conocer sobre el panel Rentabilidad con PedidosYa activo:

1. **Filtros Canal:**
   - **Ambos** (default): Todo (Local + PedidosYa + Rappi futuro) → sirve para resultado mensual global
   - **Local**: Pedidos mesa/delivery/retiro SIN delivery app → sirve para analizar operación propia
   - **PedidosYa**: Solo PedidosYa → sirve para conciliar contra reporte de PedidosYa (revisar N° pedidos, total Comisión $, % efectivo)
2. **2 KPIs NUEVOS:**
   - **Comisión Delivery:** $ Comisión total cobrada en período, subtítulo con 2 % (contractual vs efectivo) y venta bruta PYA.
   - **Margen Real:** Ganancia Bruta - Comisión Delivery. **Este es el KPI más importante para el comercio**, NO la Ganancia Bruta.
3. **Si el KPI CMV o Margen Real dice `⚠ CMV ciego`:** NO te fíes del Margen Bruto 100% → cargar costos en inventario y backfillear unit_cost.
4. **Conciliación contra reporte PedidosYa (mensual):**
   - Exportar reporte de PedidosYa (1 archivo Excel).
   - Panel Rentabilidad → Filtro período: todo el mes → Canal: PedidosYa → Exportar CSV / Copiar KPIs.
   - Comparar 3 columnas: N° pedidos entregados, Comisión Delivery $, Ventas Netas $. Si difieren > 2% → usar IDs de pedidos del reporte contra tabla orders `where source='pedidos_ya' and delivered_at BETWEEN ...` para audit diferencias.


## 5. Branding

- Nombre visible en carta:
- Color principal:
- Color secundario:
- Color botones:
- Color texto botones:
- Logo:
- Imagen principal/carrusel:
- Observaciones visuales:

## 6. Catálogo inicial

- ¿Catálogo listo?:
- Fuente del catálogo:
  - manual
  - planilla
  - JSON previo
  - migración desde otro comercio
- Categorías principales:
- Cantidad estimada de productos:
- ¿Tiene packs o variantes?:
- ¿Tiene destacados/promos?:
- ¿Tiene imágenes listas?:

## 7. Configuración técnica

- Archivo config objetivo:
- `meta.slug` validado:
- `checkout.mode` validado:
- `checkout.whatsappNumber` validado:
- `filters` revisados:
- `catalog` cargado:
- Página pública creada/ajustada:
- Versionado de assets actualizado:

## 8. Seguridad

- `public_order_token` generado:
- Token visible en panel master:
- Frontend recibiendo token desde `/api/config`:
- Regla Cloudflare para `POST /api/orders` activa:
- Eventos de seguridad revisados:

## 9. Usuarios del comercio

- Usuario principal:
- Password inicial entregada:
- ¿Owner asignado?:
- Usuarios adicionales:
  - admin:
  - cocina:
  - caja:
  - mozo:
  - repartidor:

## 10. Validación funcional

- Carta carga correctamente:
- Productos visibles:
- Búsqueda funciona:
- Carrito funciona:
- Pedido de prueba exitoso:
- Pedido visible en panel:
- WhatsApp correcto:
- Mesas correctas:
- Delivery correcto:
- Integración Pedidos Ya (webhook de prueba):
- Snapshot de unit_cost en pedidos delivery:
- Comisión Pedidos Ya descontada en rentabilidad:
- **FASE 3 Sync Bidireccional:**
  - [ ] order_events.pedidosya_status_sync_out se escribe en cada cambio de estado a PedidosYa
  - [ ] Result = "sent" en sandbox con API key válida
  - [ ] Result = "skipped" si API key no configurada (sin crashear)
  - [ ] Prueba idempotencia OK (2do mismo status = skip)
  - [ ] Prueba error network / credencial inválida = panel Qplato sigue funcionando (sin 5xx)
- **FASE 4 Onboarding + UAT:**
  - [ ] Contrato PedidosYa firmado y API habilitada por ejecutivo
  - [ ] Credenciales (api_key/merchant_id/webhook_secret) guardadas de forma segura
  - [ ] Webhook URL Render configurada en portal PedidosYa con ?sync=1
  - [ ] Mapeo SKUs top 10 PedidosYa ↔ Qplato cargado
  - [ ] **F4-P1 Alta pedido nuevo PedidosYa OK** (ver §4d)
  - [ ] **F4-P2 Cancelación PedidosYa OK**
  - [ ] **F4-P3 Idempotencia webhook OK**
  - [ ] **F4-P4 SKU desconocido OK (sin crash)**
  - [ ] **F4-P5 Comisión external_fee_amount calculada OK**
  - [ ] **F4-P6 Sync Outbound 3 status cambios OK (sent/skipped/error)**
  - [ ] **F4-P7 Panel Rentabilidad F5 KPIs OK (Comisión 2%, Margen Real, filtros Canal)**
- **FASE 5 Rentabilidad:**
  - [ ] Cargar cost_price en todos los productos
  - [ ] Backfill unit_cost order_items históricos (run migrate_costs_backfill.py 1 vez)
  - [ ] Conciliación 1er mes: reporte PedidosYa vs Panel Rentabilidad F5 Canal=PedidosYa OK

## 11. Pendientes

- [ ]
- [ ]
- [ ]

## 12. Cierre

- Fecha de cierre:
- Validado por:
- Estado final:
  - listo para producción
  - listo para beta
  - falta información
  - bloqueado
- Notas finales:

---

## Comandos y referencias útiles

- Checklist general: [checklist-onboarding-multicomercio.md](file:///c:/Daniel%20Olate/Backup%20de%20proyecto/Proyecto%20avansado%202026-limpio/checklist-onboarding-multicomercio.md)
- Ticket operativo: [plantilla-ticket-alta-comercio.md](file:///c:/Daniel%20Olate/Backup%20de%20proyecto/Proyecto%20avansado%202026-limpio/plantilla-ticket-alta-comercio.md)
- Panel master: [adminmaster.html](file:///c:/Daniel%20Olate/Backup%20de%20proyecto/Proyecto%20avansado%202026-limpio/adminmaster.html)
- Plantilla pública: [public-menu-base.html](file:///c:/Daniel%20Olate/Backup%20de%20proyecto/Proyecto%20avansado%202026-limpio/public-menu-base.html)
- Config base: [config/comercio-base.json](file:///c:/Daniel%20Olate/Backup%20de%20proyecto/Proyecto%20avansado%202026-limpio/config/comercio-base.json)

### Integración Pedidos Ya (Delivery)

- Blueprint principal: [app/blueprints/delivery.py](file:///c:/Daniel%20Olate/Backup%20de%20proyecto/Proyecto%20avansado%202026-limpio/app/blueprints/delivery.py)
- Migración columnas delivery (ejecutar en Render): [migrate_delivery_columns.py](file:///c:/Daniel%20Olate/Backup%20de%20proyecto/Proyecto%20avansado%202026-limpio/migrate_delivery_columns.py)
- Script prueba webhook Pedidos Ya: [test_pedidosya_webhook.py](file:///c:/Daniel%20Olate/Backup%20de%20proyecto/Proyecto%20avansado%202026-limpio/test_pedidosya_webhook.py)
- Endpoint webhook público: `POST /api/webhooks/pedidosya/<tenant_slug>`
- Endpoints config delivery: `GET/POST /api/delivery/config/<tenant_slug>`
