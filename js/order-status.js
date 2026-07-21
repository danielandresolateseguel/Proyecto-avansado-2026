
import { getBusinessSlug, getOrderStatusWhatsappConfig } from './config.js?v=8';

// Estado interno del módulo
let pollingInterval = null;
let statusActionInterval = null;
let isPaymentMode = false;
let celebrationShown = false;
let currentConfig = {};

/**
 * Inicializa el sistema de estado de pedidos
 */
export function initOrderStatus() {
    const statusBtn = document.getElementById('order-status-float');
    const statusModal = document.getElementById('order-status-modal');
    const closeBtn = document.querySelector('.close-status-modal');
    
    // Si no existen los elementos, no hacemos nada
    if (!statusBtn || !statusModal) return;

    // Eventos de apertura/cierre
    statusBtn.addEventListener('click', function() {
        statusModal.style.display = 'flex';
        celebrationShown = false; // Reset celebration flag
        // Forzar reflow para animación
        statusModal.offsetHeight; 
        statusModal.classList.add('active');
        statusModal.setAttribute('aria-hidden', 'false');
        document.body.style.overflow = 'hidden';
        fetchOrderStatus();
        startPolling();
    });

    if (closeBtn) {
        closeBtn.addEventListener('click', closeStatusModal);
    }

    statusModal.addEventListener('click', function(e) {
        if (e.target === statusModal) closeStatusModal();
    });

    // Cerrar con ESC
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape' && statusModal.classList.contains('active')) {
            closeStatusModal();
        }
    });

    // Iniciar polling de fondo (cada 30 segundos)
    setInterval(checkBackgroundStatus, 30000);
    // Chequeo inicial rápido (1 seg después de carga)
    setTimeout(checkBackgroundStatus, 1000);
}

function startPolling() {
    stopPolling();
    // Actualizar datos cada 5 segundos
    pollingInterval = setInterval(updateStatusSilently, 5000);
    
    // Alternar botón cada 5 segundos
    statusActionInterval = setInterval(() => {
        isPaymentMode = !isPaymentMode;
        const btn = document.querySelector('.btn-whatsapp-status');
        if (btn) {
            if (isPaymentMode) {
                btn.innerHTML = '<i class="fas fa-credit-card"></i> Realizar pago';
                if (btn.dataset.payUrl) btn.href = btn.dataset.payUrl;
                btn.setAttribute('aria-label', 'Realizar pago por WhatsApp');
            } else {
                btn.innerHTML = '<i class="fab fa-whatsapp"></i> Consultar por este pedido';
                if (btn.dataset.chatUrl) btn.href = btn.dataset.chatUrl;
                btn.setAttribute('aria-label', 'Consultar pedido por WhatsApp');
            }
        }
    }, 5000);
}

function stopPolling() {
    if (pollingInterval) clearInterval(pollingInterval);
    pollingInterval = null;
    if (statusActionInterval) clearInterval(statusActionInterval);
    statusActionInterval = null;
    isPaymentMode = false; // Reset state
}

function closeStatusModal() {
    const statusModal = document.getElementById('order-status-modal');
    if (!statusModal) return;

    stopPolling();
    if (statusActionInterval) { clearInterval(statusActionInterval); statusActionInterval = null; }
    statusModal.classList.remove('active');
    statusModal.style.display = 'none';
    statusModal.setAttribute('aria-hidden', 'true');
    document.body.style.overflow = '';
}

function getTenantSlug() {
    let slug = getBusinessSlug();
    const alias = {
        'gatrolocal1': 'gastronomia-local1',
        'gastro-local1': 'gastronomia-local1',
        'gastro1': 'gastronomia-local1'
    };
    slug = alias[slug] || slug || 'gastronomia-local1';
    return slug;
}

function _paymentHintKey() {
    const slug = getTenantSlug();
    return 'payment_hint_shown_' + slug;
}

function shouldShowPaymentHintOnce(order) {
    try {
        if (!order || order.status === 'entregado' || order.status === 'cancelado') return false;
        return localStorage.getItem(_paymentHintKey()) !== '1';
    } catch (_) {
        return false;
    }
}

function markPaymentHintShown() {
    try { localStorage.setItem(_paymentHintKey(), '1'); } catch (_) {}
}

async function updateStatusSilently() {
    const slug = getTenantSlug();
    const orderId = localStorage.getItem('last_order_id_' + slug);
    const statusBody = document.getElementById('order-status-body');
    if (!orderId || !statusBody) return;

    try {
        // Reutilizamos getOrderData existente
        const data = await getOrderData(orderId);
        
        if (data && data.order) {
            // Si el modal está abierto, asumimos que el usuario vio el nuevo estado
            localStorage.setItem('last_viewed_status_' + slug, data.order.status);
            hideNotificationBadge();

            const fullOrder = {
                ...data.order,
                items: data.items || []
            };
            
            // Guardar posición de scroll para evitar saltos
            const scrollTop = statusBody.scrollTop;
            renderStatus(fullOrder, currentConfig);
            statusBody.scrollTop = scrollTop;
        }
    } catch (e) {
        // Fallo silencioso en actualización background
        console.debug('Silent update skipped:', e);
    }
}

async function fetchOrderStatus() {
    const slug = getTenantSlug();
    const orderId = localStorage.getItem('last_order_id_' + slug);
    
    if (!orderId) {
        renderStatusError('No tienes pedidos recientes registrados en este dispositivo.');
        return;
    }

    renderLoading();

    try {
        // Determine base URL for config fetch
        const origin = window.location.origin || '';
        const base = /^file:/i.test(origin) ? 'http://127.0.0.1:8000' : origin;

        // Fetch order data and config in parallel
        // const slug = window.BUSINESS_SLUG || 'gastronomia-local1'; // Already got slug above
        const [orderData, configData] = await Promise.all([
            getOrderData(orderId),
            fetch(`${base}/api/config?slug=${slug}`).then(r => r.json()).catch(() => ({}))
        ]);
        
        currentConfig = configData || {};

        if (orderData && orderData.order) {
            // Actualizar estado visto al abrir modal
            localStorage.setItem('last_viewed_status_' + slug, orderData.order.status);
            hideNotificationBadge(); // Limpiar notificación

            const fullOrder = {
                ...orderData.order,
                items: orderData.items || []
            };
            renderStatus(fullOrder, configData);
        } else {
            renderStatusError('Error en el formato de respuesta del pedido.');
        }

    } catch (error) {
        console.error('Error fetching order status:', error);
        // Si es 404, ya se manejó en getOrderData o aquí
        if (error.message.includes('404')) {
            renderStatusError('No se encontró el pedido #' + orderId);
        } else {
            renderStatusError('No se pudo conectar con el sistema de pedidos. Intenta nuevamente.');
        }
    }
}

async function getOrderData(orderId) {
    const origin = window.location.origin || '';
    const base = /^file:/i.test(origin) ? 'http://127.0.0.1:8000' : origin;
    
    const resp = await fetch(`${base}/api/orders/${orderId}`);
    
    if (!resp.ok) {
        if (resp.status === 404) throw new Error('404 Not Found');
        throw new Error('Error al consultar el servidor');
    }
    return await resp.json();
}

function showNotificationBadge() {
    const statusBtn = document.getElementById('order-status-float');
    if (!statusBtn) return;

    // Evitar duplicados
    if (statusBtn.querySelector('.notification-badge')) return;
    
    const badge = document.createElement('div');
    badge.className = 'notification-badge';
    badge.innerHTML = '<i class="fas fa-bell"></i>';
    statusBtn.appendChild(badge);
    
    // Efecto visual (opcional, ya tiene animación CSS)
    statusBtn.classList.add('has-notification');
}

function hideNotificationBadge() {
    const statusBtn = document.getElementById('order-status-float');
    if (!statusBtn) return;

    const badge = statusBtn.querySelector('.notification-badge');
    if (badge) badge.remove();
    statusBtn.classList.remove('has-notification');
}

async function checkBackgroundStatus() {
    const slug = getTenantSlug();
    const orderId = localStorage.getItem('last_order_id_' + slug);
    if (!orderId) return;

    try {
        const data = await getOrderData(orderId);
        if (data && data.order) {
            const currentStatus = data.order.status;
            const lastViewed = localStorage.getItem('last_viewed_status_' + slug);

            // Si el estado es diferente al último visto, mostrar notificación
            // Ignorar si nunca se ha visto (primera carga) y el estado es 'pendiente' (opcional)
            if (currentStatus !== lastViewed) {
                showNotificationBadge();
            }
        }
    } catch (e) {
        // Silencioso en background
    }
}

function renderLoading() {
    const statusBody = document.getElementById('order-status-body');
    if (!statusBody) return;
    statusBody.innerHTML = `
        <div class="status-loading">
            <i class="fas fa-spinner fa-spin"></i> Verificando estado...
        </div>
    `;
}

function renderStatusError(msg) {
    const statusBody = document.getElementById('order-status-body');
    if (!statusBody) return;
    statusBody.innerHTML = `
        <div class="status-loading" style="color: var(--gastro-danger, #ef4444);">
            <i class="fas fa-exclamation-circle"></i>
            <p>${msg}</p>
        </div>
    `;
}

function triggerConfetti() {
    if (document.getElementById('confetti-canvas')) return;

    const canvas = document.createElement('canvas');
    canvas.id = 'confetti-canvas';
    canvas.style.position = 'fixed';
    canvas.style.top = '0';
    canvas.style.left = '0';
    canvas.style.width = '100%';
    canvas.style.height = '100%';
    canvas.style.pointerEvents = 'none';
    canvas.style.zIndex = '9999';
    document.body.appendChild(canvas);

    const ctx = canvas.getContext('2d');
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;

    const particles = [];
    const colors = ['#f44336', '#e91e63', '#9c27b0', '#673ab7', '#3f51b5', '#2196f3', '#03a9f4', '#00bcd4', '#009688', '#4caf50', '#8bc34a', '#cddc39', '#ffeb3b', '#ffc107', '#ff9800', '#ff5722'];

    for (let i = 0; i < 150; i++) {
        particles.push({
            x: Math.random() * canvas.width,
            y: Math.random() * canvas.height - canvas.height,
            w: Math.random() * 10 + 5,
            h: Math.random() * 10 + 5,
            color: colors[Math.floor(Math.random() * colors.length)],
            speed: Math.random() * 5 + 2,
            angle: Math.random() * 360,
            spin: Math.random() * 15 - 7.5
        });
    }

    let animationId;
    function draw() {
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        let active = false;

        particles.forEach(p => {
            p.y += p.speed;
            p.angle += p.spin;
            
            if (p.y < canvas.height) active = true;

            ctx.save();
            ctx.translate(p.x, p.y);
            ctx.rotate(p.angle * Math.PI / 180);
            ctx.fillStyle = p.color;
            ctx.fillRect(-p.w / 2, -p.h / 2, p.w, p.h);
            ctx.restore();
        });

        if (active) {
            animationId = requestAnimationFrame(draw);
        } else {
            canvas.remove();
        }
    }

    draw();

    // Safety cleanup
    setTimeout(() => {
        if (document.body.contains(canvas)) {
            cancelAnimationFrame(animationId);
            canvas.remove();
        }
    }, 8000);
}

function renderStatus(order, config = {}) {
    const statusBody = document.getElementById('order-status-body');
    if (!statusBody) return;
    
    if (order.status === 'entregado' && !celebrationShown) {
        triggerConfetti();
        celebrationShown = true;
    }

    let paymentHintHtml = '';
    if (shouldShowPaymentHintOnce(order)) {
        paymentHintHtml = `
            <div class="status-alert" style="margin: 10px 0 0; padding: 12px 14px; background: #fff7ed; color: #7c2d12; border: 1px solid #fed7aa; border-radius: 10px; text-align: left; font-weight: 600; line-height: 1.25;">
                <i class="fas fa-info-circle" style="margin-right:6px;"></i>
                Por favor, mantente atento a tus mensajes: es posible que te contactemos para coordinar el pago de tu pedido.
            </div>
        `;
        markPaymentHintShown();
    }
    
    const statusMap = {
        'por_aprobar': { label: 'Por aprobar', class: 'pendiente', icon: 'fa-user-check' },
        'pendiente': { label: 'Pendiente', class: 'pendiente', icon: 'fa-clock' },
        'preparacion': { label: 'En preparación', class: 'preparacion', icon: 'fa-fire' },
        'listo': { label: 'Listo para retirar', class: 'listo', icon: 'fa-shopping-bag' },
        'en_camino': { label: 'En camino', class: 'en_camino', icon: 'fa-motorcycle' },
        'entregado': { label: 'Entregado', class: 'entregado', icon: 'fa-smile-beam' },
        'cancelado': { label: 'Cancelado', class: 'cancelado', icon: 'fa-times-circle' }
    };

    const s = statusMap[order.status] || { label: order.status, class: 'default', icon: 'fa-info-circle' };
    
    // Ensure date is treated as UTC if it doesn't have timezone info
    let dateStr = order.created_at || '';
    if (dateStr && !dateStr.endsWith('Z') && !dateStr.includes('+')) {
        dateStr += 'Z';
    }
    
    const date = new Date(dateStr).toLocaleString('es-AR', { 
        hour: '2-digit', minute: '2-digit', day: '2-digit', month: '2-digit',
        timeZone: 'America/Argentina/Buenos_Aires'
    });

    // Estimated Time Logic
    let estimatedTimeHtml = '';
    if (order.status !== 'entregado' && order.status !== 'cancelado') {
        let minutes = 0;
        const type = order.order_type || 'mesa';
        
        // Prioridad: manual config por ahora (hasta tener lógica auto backend)
        if (type === 'mesa') minutes = config.time_mesa;
        else if (type === 'espera') minutes = config.time_espera;
        else if (type === 'direccion') minutes = config.time_delivery;

        // Si está activado auto, podríamos mostrar un rango o texto diferente
        // Por ahora mostramos el valor manual como base
        
        if (minutes > 0) {
             estimatedTimeHtml = `
                <div class="estimated-time" style="text-align: center; margin-top: -1rem; margin-bottom: 1.5rem; color: #6b7280; font-size: 0.9rem;">
                    <i class="fas fa-hourglass-half"></i> Tiempo estimado: <strong>${minutes} min</strong>
                </div>
            `;
        }
    }

    // --- LÓGICA DE STEPPER (Línea de Tiempo) ---
    // Detectar si es delivery (por tipo o por estado explícito)
    // Intentamos usar order_type si viene del backend, o inferir si el estado actual es 'en_camino'
    const isDelivery = (order.order_type === 'direccion') || (order.status === 'en_camino');

    let steps = [];
    if (isDelivery) {
        steps = [
            { key: 'por_aprobar', label: 'Aprobación', icon: 'fa-user-check' },
            { key: 'pendiente', label: 'Recibido', icon: 'fa-clipboard-check' },
            { key: 'preparacion', label: 'Cocina', icon: 'fa-fire' },
            { key: 'listo', label: 'Listo', icon: 'fa-check' },
            { key: 'en_camino', label: 'En camino', icon: 'fa-motorcycle' },
            { key: 'entregado', label: 'Entregado', icon: 'fa-smile' }
        ];
    } else {
        steps = [
            { key: 'por_aprobar', label: 'Aprobación', icon: 'fa-user-check' },
            { key: 'pendiente', label: 'Recibido', icon: 'fa-clipboard-check' },
            { key: 'preparacion', label: 'Cocina', icon: 'fa-fire' },
            { key: 'listo', label: 'Listo', icon: 'fa-check' },
            { key: 'entregado', label: 'Entregado', icon: 'fa-smile' }
        ];
    }

    // Mapear estado actual a índice de paso
    let currentStepIndex = 0;
    
    if (isDelivery) {
        // Lógica para 5 pasos (con En camino)
        if (order.status === 'por_aprobar') currentStepIndex = 0;
        else if (order.status === 'pendiente') currentStepIndex = 1;
        else if (order.status === 'preparacion') currentStepIndex = 2;
        else if (order.status === 'listo') currentStepIndex = 3;
        else if (order.status === 'en_camino') currentStepIndex = 4;
        else if (order.status === 'entregado') currentStepIndex = 5;
        else if (order.status === 'cancelado') currentStepIndex = -1;
    } else {
        // Lógica estándar de 4 pasos
        if (order.status === 'por_aprobar') currentStepIndex = 0;
        else if (order.status === 'pendiente') currentStepIndex = 1;
        else if (order.status === 'preparacion') currentStepIndex = 2;
        else if (order.status === 'listo') currentStepIndex = 3;
        // Si llega en_camino pero no es delivery (caso raro/fallback), lo mostramos como paso 2 (Listo) o 3 si fuera posible
        // Para mantener consistencia, si no es delivery, en_camino se visualiza igual que listo (ya salió)
        else if (order.status === 'en_camino') currentStepIndex = 3; 
        else if (order.status === 'entregado') currentStepIndex = 4;
        else if (order.status === 'cancelado') currentStepIndex = -1;
    }

    let stepperHtml = '';
    if (currentStepIndex !== -1) {
        stepperHtml = '<div class="status-stepper">';
        steps.forEach((step, index) => {
            let stepClass = 'step';
            if (index < currentStepIndex) stepClass += ' completed';
            else if (index === currentStepIndex) stepClass += ' active';
            
            // Icono: si está completo, usar check, si no, el del paso
            const icon = (index < currentStepIndex) ? 'fa-check' : step.icon;

            stepperHtml += `
                <div class="${stepClass}">
                    <div class="step-circle">
                        <i class="fas ${icon}"></i>
                    </div>
                    <div class="step-label">${step.label}</div>
                </div>
            `;
        });
        stepperHtml += '</div>';
    } else {
        // Mensaje visual para cancelado
        stepperHtml = `
            <div class="status-stepper cancelled" style="justify-content: center;">
               <div class="step active">
                    <div class="step-circle" style="border-color: var(--gastro-danger); color: var(--gastro-danger);">
                        <i class="fas fa-times"></i>
                    </div>
                    <div class="step-label" style="color: var(--gastro-danger);">Pedido Cancelado</div>
               </div>
            </div>
        `;
    }

    // --- DETALLE DE ITEMS ---
    let itemsHtml = '';
    if (order.items && Array.isArray(order.items)) {
        itemsHtml = '<div class="order-items-container">';
        order.items.forEach(item => {
            const note = (item.notes && item.notes !== 'null' && item.notes !== 'undefined') ? item.notes : '';
            itemsHtml += `
                <div class="order-summary-item">
                    <div style="display:flex; flex-direction:column; width:100%;">
                        <div style="display:flex; justify-content:space-between; align-items:center;">
                            <span class="item-name"><strong>${item.qty}x</strong> ${item.name}</span>
                            <span class="item-price">${(function(){try{return (new Intl.NumberFormat((window.CURRENCY_LOCALE||'es-AR'),{style:'currency',currency:(window.CURRENCY_CODE||'ARS'),maximumFractionDigits:0})).format(item.unit_price)+' '+(window.CURRENCY_CODE||'ARS')}catch(_){return '$'+item.unit_price}})()}</span>
                        </div>
                        ${note ? `<div class="item-note" style="font-size:0.85em; color:#e65100; margin-left:1.5rem; margin-top:0.2rem; font-style:italic;">Nota: ${note}</div>` : ''}
                    </div>
                </div>
            `;
        });
        itemsHtml += '</div>';
    }

    // --- BOTÓN WHATSAPP ---
    const orderStatusWhatsapp = getOrderStatusWhatsappConfig(config);
    const whatsappNumber = String(orderStatusWhatsapp.number || '').replace(/\D+/g, '');
    const tenantOrderNumberRaw = (order && (order.tenant_order_number ?? order.tenant_order_number)) ?? null;
    let displayOrderNumber = String(order.id || '');
    const formatTenantOrderSeries = (index) => {
        let n = Math.max(0, Math.trunc(Number(index) || 0));
        let out = '';
        do {
            out = String.fromCharCode(65 + (n % 26)) + out;
            n = Math.floor(n / 26) - 1;
        } while (n >= 0);
        return out;
    };
    const formatTenantOrderNumber = (value) => {
        const raw = Math.trunc(Number(value));
        if (!Number.isFinite(raw) || raw <= 0) return String(value || '').trim();
        if (raw <= 9999) {
            const s = String(raw);
            return s.length >= 3 ? s : s.padStart(3, '0');
        }
        const offset = raw - 10000;
        const series = formatTenantOrderSeries(Math.floor(offset / 9999));
        const withinSeries = (offset % 9999) + 1;
        const s = String(withinSeries);
        return `${series}${s.length >= 3 ? s : s.padStart(3, '0')}`;
    };
    if (tenantOrderNumberRaw !== null && tenantOrderNumberRaw !== undefined && String(tenantOrderNumberRaw).trim() !== '') {
        const n = parseInt(tenantOrderNumberRaw, 10);
        displayOrderNumber = (isFinite(n) && n > 0) ? formatTenantOrderNumber(n) : String(tenantOrderNumberRaw);
    }

    const hasWhatsappContact = !!orderStatusWhatsapp.enabled && !!whatsappNumber;
    const whatsappMsg = encodeURIComponent(`Hola, tengo una consulta sobre mi pedido #${displayOrderNumber}.`);
    const whatsappUrl = hasWhatsappContact ? `https://wa.me/${whatsappNumber}?text=${whatsappMsg}` : '#';

    const whatsappPaymentMsg = encodeURIComponent(`Hola, quiero realizar el pago del pedido #${displayOrderNumber}.`);
    const whatsappPaymentUrl = hasWhatsappContact ? `https://wa.me/${whatsappNumber}?text=${whatsappPaymentMsg}` : '#';

    // Lógica de totales (Propina)
    let totalSectionHtml = '';
    if (order.order_type === 'mesa') {
        const baseTotal = parseInt(order.total) || 0;
        const tip = Math.round(baseTotal * 0.10);
        const totalWithTip = baseTotal + tip;
        
        totalSectionHtml = `
            <div class="order-summary-item" style="border-top: 1px dashed #eee; margin-top: 10px; padding-top: 10px;">
                <span class="item-name" style="font-weight: bold;">Total (sin propina)</span>
                <span class="item-price">${(function(){try{return (new Intl.NumberFormat((window.CURRENCY_LOCALE||'es-AR'),{style:'currency',currency:(window.CURRENCY_CODE||'ARS'),maximumFractionDigits:0})).format(baseTotal)+' '+(window.CURRENCY_CODE||'ARS')}catch(_){return '$'+baseTotal}})()}</span>
            </div>
            <div class="order-summary-item" style="color: #2e7d32;">
                <span class="item-name">Propina sugerida (10%)</span>
                <span class="item-price">${(function(){try{return (new Intl.NumberFormat((window.CURRENCY_LOCALE||'es-AR'),{style:'currency',currency:(window.CURRENCY_CODE||'ARS'),maximumFractionDigits:0})).format(tip)+' '+(window.CURRENCY_CODE||'ARS')}catch(_){return '$'+tip}})()}</span>
            </div>
            <div class="order-summary-total">
                <span>Total con propina</span>
                <span>${(function(){try{return (new Intl.NumberFormat((window.CURRENCY_LOCALE||'es-AR'),{style:'currency',currency:(window.CURRENCY_CODE||'ARS'),maximumFractionDigits:0})).format(totalWithTip)+' '+(window.CURRENCY_CODE||'ARS')}catch(_){return '$'+totalWithTip}})()}</span>
            </div>
        `;
    } else {
        totalSectionHtml = `
            <div class="order-summary-total">
                <span>Total</span>
                <span>${(function(){try{return (new Intl.NumberFormat((window.CURRENCY_LOCALE||'es-AR'),{style:'currency',currency:(window.CURRENCY_CODE||'ARS'),maximumFractionDigits:0})).format(order.total)+' '+(window.CURRENCY_CODE||'ARS')}catch(_){return '$'+order.total}})()}</span>
            </div>
        `;
    }

    statusBody.innerHTML = `
        <div class="order-status-card">
            <!-- Header con Badge -->
            <div style="text-align: center; margin-bottom: 1rem;">
                <div class="order-status-badge ${s.class}">
                    <i class="fas ${s.icon}"></i> ${s.label}
                </div>
            </div>

            ${paymentHintHtml}

            <!-- Stepper Visual -->
            ${stepperHtml}

            <div class="order-header" style="text-align: center; margin-bottom: 1.5rem; border-bottom: none; padding-bottom: 0;">
                <span class="order-id" style="display: block; font-size: 0.875rem; color: #6b7280;">Pedido #${displayOrderNumber}</span>
                <span class="order-time" style="font-size: 0.75rem; color: #9ca3af;">${date}</span>
            </div>

            ${estimatedTimeHtml}

            <div class="order-details">
                <h4 style="font-size: 1rem; margin-bottom: 1rem; color: var(--gastro-text-dark, #111827);">Detalle del pedido</h4>
                ${itemsHtml}
                ${totalSectionHtml}
            </div>

            ${order.status === 'listo' ? `
                <div class="status-alert ready" style="margin-top: 1.5rem; padding: 1rem; background-color: #d1fae5; color: #065f46; border-radius: 0.5rem; text-align: center; font-weight: 600;">
                    <i class="fas fa-bell"></i> ¡Tu pedido está listo! Por favor acércate al mostrador.
                </div>
            ` : ''}

            ${hasWhatsappContact ? `
            <a href="${isPaymentMode ? whatsappPaymentUrl : whatsappUrl}" 
               target="_blank" 
               class="btn-whatsapp-status"
               data-chat-url="${whatsappUrl}"
               data-pay-url="${whatsappPaymentUrl}"
               aria-label="${isPaymentMode ? 'Realizar pago por WhatsApp' : 'Consultar pedido por WhatsApp'}">
                ${isPaymentMode ? '<i class="fas fa-credit-card"></i> Realizar pago' : '<i class="fab fa-whatsapp"></i> Consultar por este pedido'}
            </a>
            ` : `
            <div class="status-alert" style="margin-top: 1rem; padding: 0.85rem 1rem; background: #fef3c7; color: #92400e; border-radius: 0.75rem; text-align: center; font-weight: 600;">
                WhatsApp no disponible para este local.
            </div>
            `}
        </div>
    `;
}
