[OPEN] Debug Session: tables-not-loading

## Síntoma
- En `+Pedidos` (modal Nuevo Pedido) y en `Gestión de Mesas`, el selector "Nº Mesa" queda vacío / no lista mesas.
- Se reinició servidor y se forzó recarga (F5) sin cambios.

## Hipótesis (falsables)
1) `/api/tenant_tables` responde 401/403 (sesión/permiso `tables_manage`) y por eso el frontend no recibe mesas.
2) Se está consultando un `tenant_slug` distinto al esperado (ej. `tenant-input` vs tenant del modal), y ese slug no tiene configuración de mesas.
3) La respuesta de `/api/tenant_tables` es 200 pero trae `zones=[]` o `zones.tables=[]` por configuración vacía en DB/cache.
4) La carga falla por un error JS (excepción) durante `populateTableSelect()` y sale antes de renderizar.
5) El fetch a `/api/tenant_tables` se hace contra un origin/base incorrecto en local y no llega al backend esperado.

## Plan de evidencia
- Instrumentar:
  - Backend: endpoint de logging de depuración que escriba NDJSON en `.dbg/trae-debug-log-tables-not-loading.ndjson`.
  - Frontend: reportar eventos desde `populateTableSelect()` (slug resuelto, cache, status HTTP, tamaño zones/tables y errores).
- Reproducir: abrir `+Pedidos` (tipo Mesa) y/o `Gestión de Mesas` → crear pedido.
- Analizar logs y confirmar/descartar hipótesis.

## Estado
- Pendiente de instrumentación y recolección de evidencia.
