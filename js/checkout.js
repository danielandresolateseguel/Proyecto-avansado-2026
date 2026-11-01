/**
 * Checkout and WhatsApp Logic
 */
import { cart, clearCart, updateCartDisplay } from './cart.js?v=8';
import { closeCartUI } from './ui.js?v=8';
import { getWhatsappNumber, CATEGORY, getCheckoutMode, getWhatsappEnabled, getWhatsappTemplate, getBusinessSlug, formatMoneyWithCode, calculateShippingQuote } from './config.js?v=8';
import { openOrderStatusModal } from './order-status.js?v=11';

function getCheckoutMixSummary(item) {
    if (item && typeof item.mix_summary === 'string' && item.mix_summary.trim()) {
        return item.mix_summary.trim();
    }
    const modifiers = item && item.modifiers && typeof item.modifiers === 'object' ? item.modifiers : {};
    const mix = Array.isArray(modifiers.mix) ? modifiers.mix : [];
    if (!mix.length) return '';
    return mix.map(part => `1/2 ${String(part && part.name || 'Pizza').trim()}`).join(' + ');
}

function getCheckoutAddonsSummary(item) {
    if (item && typeof item.addons_summary === 'string' && item.addons_summary.trim()) {
        return item.addons_summary.trim();
    }
    const modifiers = item && item.modifiers && typeof item.modifiers === 'object' ? item.modifiers : {};
    if (typeof modifiers.addons_summary === 'string' && modifiers.addons_summary.trim()) {
        return modifiers.addons_summary.trim();
    }
    const addons = Array.isArray(modifiers.addons) ? modifiers.addons : [];
    if (!addons.length) return '';
    return addons.map(addon => {
        const qty = parseInt(addon && addon.qty, 10) || 1;
        const label = String(addon && addon.label || addon && addon.name || 'Adicional').trim();
        return qty > 1 ? `${label} x${qty}` : label;
    }).join(' + ');
}

function getGeoApiBase() {
    const origin = window.location.origin || '';
    return /^file:/i.test(origin) ? 'http://127.0.0.1:8000' : origin;
}

function getLiveDeliveryAddressInputs() {
    return {
        addressInput: document.getElementById('delivery-address'),
        localityInput: document.getElementById('delivery-locality')
    };
}

function setInputValueAndNotify(input, value) {
    if (!input) return false;
    const nextValue = String(value || '').trim();
    if (!nextValue) return false;
    if (String(input.value || '').trim() === nextValue) return false;
    input.value = nextValue;
    input.dispatchEvent(new Event('input', { bubbles: true }));
    input.dispatchEvent(new Event('change', { bubbles: true }));
    return true;
}

function deriveGeoAddress(payload) {
    if (!payload || typeof payload !== 'object') {
        return { address: '', locality: '' };
    }
    const addr = (payload.address && typeof payload.address === 'object') ? payload.address : null;
    let address = (typeof payload.address === 'string') ? String(payload.address || '').trim() : '';
    let locality = (typeof payload.locality === 'string') ? String(payload.locality || '').trim() : '';
    const displayName = String(payload.display_name || '').trim();
    const parts = displayName
        .split(',')
        .map(part => String(part || '').trim())
        .filter(Boolean);

    if (!address && addr) {
        const road = String(
            addr.road
            || addr.pedestrian
            || addr.footway
            || addr.path
            || addr.residential
            || addr.quarter
            || addr.neighbourhood
            || addr.suburb
            || addr.hamlet
            || addr.city_district
            || addr.municipality
            || ''
        ).trim();
        const house = String(addr.house_number || '').trim();
        address = road && house ? `${road} ${house}`.trim() : road;
    }

    if (!locality && addr) {
        const district = String(
            addr.suburb
            || addr.neighbourhood
            || addr.quarter
            || addr.city_district
            || ''
        ).trim();
        const city = String(
            addr.city
            || addr.town
            || addr.village
            || addr.hamlet
            || addr.municipality
            || addr.county
            || ''
        ).trim();
        const state = String(addr.state || '').trim();
        locality = [district, city, state].filter(Boolean).join(', ');
    }

    if (!address && parts.length) {
        address = parts[0];
    }
    if (!locality && parts.length > 1) {
        locality = parts
            .slice(1, 4)
            .filter(part => part && part !== address)
            .join(', ');
    }
    return { address, locality };
}

async function reverseGeocodeFromCoords(lat, lng) {
    const parseResponse = async (response) => {
        if (!response || !response.ok) return null;
        const payload = await response.json().catch(() => null);
        if (!payload) return null;
        const derived = deriveGeoAddress(payload);
        if (!derived.address && !derived.locality) return null;
        return {
            address: derived.address,
            locality: derived.locality,
            payload
        };
    };

    try {
        const url = new URL('/api/geocode/reverse', getGeoApiBase());
        url.searchParams.set('lat', String(lat));
        url.searchParams.set('lng', String(lng));
        const slug = String(getBusinessSlug() || window.BUSINESS_SLUG || '').trim();
        if (slug) url.searchParams.set('slug', slug);
        const response = await fetch(url.toString(), {
            cache: 'no-store',
            credentials: 'same-origin'
        });
        const primary = await parseResponse(response);
        if (primary) return primary;
    } catch (_) {
        // Intenta el fallback igual más abajo.
    }

    try {
        const fallbackUrl = new URL('https://nominatim.openstreetmap.org/reverse');
        fallbackUrl.searchParams.set('format', 'jsonv2');
        fallbackUrl.searchParams.set('lat', String(lat));
        fallbackUrl.searchParams.set('lon', String(lng));
        fallbackUrl.searchParams.set('addressdetails', '1');
        const fallbackResponse = await fetch(fallbackUrl.toString(), {
            cache: 'no-store',
            headers: { 'Accept-Language': 'es-AR,es;q=0.9,en;q=0.7' }
        });
        return await parseResponse(fallbackResponse);
    } catch (_) {
        return null;
    }
}

function applyGeoAutofill(address, locality, options = {}) {
    const { force = false } = options || {};
    const { addressInput, localityInput } = getLiveDeliveryAddressInputs();
    const currentAddress = String(addressInput?.value || '').trim();
    const currentLocality = String(localityInput?.value || '').trim();

    if (addressInput && address && (force || !currentAddress)) {
        setInputValueAndNotify(addressInput, address);
    }
    if (localityInput && locality && (force || !currentLocality)) {
        setInputValueAndNotify(localityInput, locality);
    }

    return {
        address: String(addressInput?.value || address || '').trim(),
        locality: String(localityInput?.value || locality || '').trim()
    };
}

export async function handleCheckout() {
    if (cart.length === 0) {
        alert('Tu carrito está vacío');
        return;
    }

    const CHECKOUT_MODE = getCheckoutMode();
    const orderTypeEl = document.querySelector('input[name="orderType"]:checked');
    const orderType = orderTypeEl ? orderTypeEl.value : (CHECKOUT_MODE === 'mesa' ? 'mesa' : 'none');
    
    const mesaNumber = (document.getElementById('mesa-number')?.value || '').trim();
    let address = (document.getElementById('delivery-address')?.value || '').trim();
    let locality = (document.getElementById('delivery-locality')?.value || '').trim();
    const contactPhone = (document.getElementById('contact-phone')?.value || '').trim();
    const deliveryName = (document.getElementById('delivery-name')?.value || '').trim();
    const esperaName = (document.getElementById('espera-name')?.value || '').trim();
    const esperaPhone = (document.getElementById('espera-phone')?.value || '').trim();
    const orderNotes = (document.getElementById('order-notes')?.value || '').trim();

    // --- Programación de pedidos / local cerrado -----------------------------
    const scheduleModeEl = document.querySelector('input[name="scheduleMode"]:checked');
    let scheduleMode = scheduleModeEl ? String(scheduleModeEl.value || 'asap').trim().toLowerCase() : 'asap';
    const slotSelectEl = document.getElementById('schedule-slot');
    let scheduledFor = '';
    if (scheduleMode === 'scheduled') {
        scheduledFor = slotSelectEl ? String(slotSelectEl.value || '').trim() : '';
        if (!scheduledFor) {
            alert('Elegí el horario en que querés que esté listo tu pedido.');
            return;
        }
    }
    const availData = (__orderAvailabilityCache && __orderAvailabilityCache.data) ? __orderAvailabilityCache.data : null;
    if (availData && availData.has_hours) {
        if (availData.block_when_closed && !availData.is_open) {
            const allowedTypes = Array.isArray(availData.schedule_order_types) ? availData.schedule_order_types : [];
            if (!allowedTypes.includes(String(orderType || '').toLowerCase())) {
                alert('El local está cerrado ahora. Para dejar un pedido programado elegí Delivery o Retiro en local.');
                return;
            }
            if (scheduleMode !== 'scheduled') {
                alert('El local está cerrado ahora. Programá tu pedido para más tarde.');
                return;
            }
        } else if (availData.is_open && !availData.allow_schedule_when_open) {
            scheduleMode = 'asap';
            scheduledFor = '';
        }
    }
    // -------------------------------------------------------------------------

    const whatsappEnabled = getWhatsappEnabled();
    const whatsappNumber = getWhatsappNumber();
    const latRaw = (document.getElementById('delivery-geo-lat')?.value || '').trim();
    const lngRaw = (document.getElementById('delivery-geo-lng')?.value || '').trim();
    const accRaw = (document.getElementById('delivery-geo-acc')?.value || '').trim();
    const tsRaw = (document.getElementById('delivery-geo-ts')?.value || '').trim();

    let geo = null;
    if (orderType === 'direccion') {
        const lat = parseFloat(latRaw);
        const lng = parseFloat(lngRaw);
        const accuracy = parseFloat(accRaw);
        const ts = tsRaw ? parseInt(tsRaw, 10) : null;
        if (Number.isFinite(lat) && Number.isFinite(lng)) {
            geo = {
                lat,
                lng,
                accuracy: Number.isFinite(accuracy) ? accuracy : null,
                ts: Number.isFinite(ts) ? ts : null
            };
        }

        if (geo && (!address || !locality)) {
            const resolved = await reverseGeocodeFromCoords(geo.lat, geo.lng);
            if (resolved) {
                const applied = applyGeoAutofill(resolved.address, resolved.locality);
                address = applied.address;
                locality = applied.locality;
                try {
                    const saved = sessionStorage.getItem('delivery_geo');
                    const cached = saved ? JSON.parse(saved) : {};
                    const next = (cached && typeof cached === 'object') ? cached : {};
                    next.address = address || next.address || '';
                    next.locality = locality || next.locality || '';
                    sessionStorage.setItem('delivery_geo', JSON.stringify(next));
                } catch (_) {}
            }
        }
    }

    // Validaciones
    if (orderType === 'mesa' && !mesaNumber) { alert('Por favor, ingresa el número de mesa.'); return; }
    if (orderType === 'direccion') {
        if (!address) { alert('Por favor, ingresa la dirección de entrega.'); return; }
        if (!contactPhone) { alert('Por favor, ingresa el teléfono de contacto.'); return; }
        if (!deliveryName) { alert('Por favor, ingresa tu nombre.'); return; }
    }
    if (orderType === 'espera') {
        if (!esperaName) { alert('Por favor, ingresa tu nombre.'); return; }
        if (!esperaPhone) { alert('Por favor, ingresa tu teléfono.'); return; }
    }
    if (whatsappEnabled && !whatsappNumber) {
        alert('Este local no tiene configurado un número de WhatsApp para recibir pedidos. Verificalo desde el panel antes de publicar la carta.');
        return;
    }

    // Construcción del mensaje (Nueva Lógica con Template)
    // 1. Prepare Data Strings
    let pedidoInfo = '';
    if (orderType === 'mesa') pedidoInfo = `\uD83D\uDCCD Modalidad: Mesa\n   \uD83C\uDF7D Mesa N°: ${mesaNumber}`;
    else if (orderType === 'direccion') pedidoInfo = `\uD83D\uDCCD Modalidad: Dirección\n   \uD83C\uDFE0 Dirección: ${address}${locality ? `\n   \uD83D\uDCCD Localidad: ${locality}` : ''}\n   \uD83D\uDC64 Nombre: ${deliveryName}`;
    else if (orderType === 'espera') pedidoInfo = `\uD83D\uDCCD Modalidad: Espera en local\n   \uD83D\uDC64 Nombre: ${esperaName}\n   \uD83D\uDCDE Teléfono: ${esperaPhone}`;

    let itemsList = '';
    cart.forEach((item, index) => {
        const precioFormateado = formatMoneyWithCode(parseInt(item.price));
        itemsList += `${index + 1}. \uD83D\uDCE6 ${item.name}\n`;
        itemsList += `   \uD83D\uDCCA Cantidad: ${item.quantity}\n`;
        itemsList += `   \uD83D\uDCB5 Precio unitario: ${precioFormateado}\n`;
        const mixSummary = getCheckoutMixSummary(item);
        if (mixSummary) itemsList += `   \uD83C\uDF55 Mitades: ${mixSummary}\n`;
        const addonsSummary = getCheckoutAddonsSummary(item);
        if (addonsSummary) itemsList += `   \u2795 Adicionales: ${addonsSummary}\n`;
        const subtotalTxt = formatMoneyWithCode(parseInt(item.price * item.quantity));
        itemsList += `   \uD83D\uDCB0 Subtotal: ${subtotalTxt}\n`;
        if ((item.notes || '').trim()) itemsList += `   \uD83D\uDCDD Detalle: ${(item.notes||'').trim()}\n`;
        itemsList += '\n';
    });

    // Totals logic
    let shippingCost = 0;
    if (orderType === 'direccion') {
        shippingCost = calculateShippingQuote(orderType, geo).cost || 0;
    }
    const totalNumber = cart.reduce((sum, item) => sum + (item.price * item.quantity), 0) + shippingCost;
    const totalText = formatMoneyWithCode(parseInt(totalNumber));
    const currentCategory = (CATEGORY || '').toLowerCase();
    const isCommerce = currentCategory === 'comercio' || currentCategory === 'general';

    let totales = '';
    if (shippingCost > 0) {
        const envioTxt = formatMoneyWithCode(parseInt(shippingCost));
        totales += `\uD83D\uDE9A Costo de envío: ${envioTxt}\n`;
    }
    
    if (isCommerce) {
        totales += `\uD83D\uDCB0 TOTAL: ${totalText}\n`;
    } else {
        totales += `\uD83D\uDCB0 TOTAL (sin propina): ${totalText}\n`;
    }

    if (orderType === 'mesa') {
        const tip = Math.round(totalNumber * 0.10);
        const tipTxt = formatMoneyWithCode(parseInt(tip));
        const totalWithTipTxt = formatMoneyWithCode(parseInt(totalNumber + tip));
        totales += `\uD83D\uDC81 Propina sugerida (10%): ${tipTxt}\n`;
        totales += `\uD83C\uDF7D\uFE0F TOTAL con propina sugerida: ${totalWithTipTxt}\n`;
    }

    let notas = '';
    if (orderNotes) notas = `\uD83D\uDCDD Detalle adicional: ${orderNotes}`;
    if (scheduleMode === 'scheduled' && scheduledFor) {
        const labelTxt = (slotSelectEl && slotSelectEl.selectedOptions && slotSelectEl.selectedOptions[0])
            ? String(slotSelectEl.selectedOptions[0].textContent || '').trim()
            : '';
        const schedTxt = labelTxt || scheduledFor;
        notas = (notas ? notas + '\n' : '') + `\u23F0 Pedido programado para estar listo: ${schedTxt}`;
    }

    // 2. Get Template
    let template = getWhatsappTemplate();
    if (!template) {
        // Fallback default template
        template = `¡Hola! \uD83D\uDC4B Espero que estés muy bien.

\uD83D\uDED2 Me gustaría realizar el siguiente pedido:

{PEDIDO_INFO}

{ITEMS}

{TOTALES}

{NOTAS}

`;
        if (orderType !== 'mesa') template += '¿Podrías confirmarme la disponibilidad y el método de entrega?\n\n';
        if (isCommerce) template += '¿Qué métodos de pago aceptan? (efectivo, débito, crédito, transferencia)\n\n';
        template += '¡Muchas gracias! \uD83D\uDE0A';
    }

    // 3. Construct Final Message
    let mensaje = template
        .replace('{PEDIDO_INFO}', pedidoInfo)
        .replace('{ITEMS}', itemsList)
        .replace('{TOTALES}', totales)
        .replace('{NOTAS}', notas);

    // SANITIZATION: Check for corruption (diamonds) and fallback if necessary
    if (mensaje.indexOf('\ufffd') !== -1) {
        console.warn('Corrupt template detected (diamonds). Using fallback.');
        mensaje = `¡Hola! \uD83D\uDC4B Espero que estés muy bien.

\uD83D\uDED2 Me gustaría realizar el siguiente pedido:

${pedidoInfo}

${itemsList}

${totales}

${notas}`;
    }

    // cleanup multiple newlines
    mensaje = mensaje.replace(/\n{3,}/g, '\n\n').trim();

    try {
        await sendOrderToBackend(orderType, { mesaNumber, address, locality, contactPhone, esperaName, esperaPhone, deliveryName, orderNotes, geo, scheduleMode, scheduledFor }, totalNumber);

        // 4. Send to WhatsApp (Use api.whatsapp.com directly to avoid redirect encoding issues)
        if (whatsappEnabled) {
            const sanitizedNumber = whatsappNumber.replace(/\D+/g, '');
            const urlWhatsApp = `https://api.whatsapp.com/send?phone=${sanitizedNumber}&text=${encodeURIComponent(mensaje)}`;
            const popup = window.open(urlWhatsApp, '_blank');
            if (!popup) {
                window.location.href = urlWhatsApp;
            }
        }

        clearCart();
        closeCartUI();
        try { openOrderStatusModal(); } catch (_) {}
    } catch (error) {
        console.error('Error procesando checkout:', error);
        alert('No se pudo registrar el pedido en el sistema. Tu carrito se conservó para que puedas reintentar. Detalle: ' + error.message);
    }
}

async function sendOrderToBackend(orderType, data, total) {
    const getTenantSlug = () => {
        let slug = getBusinessSlug();
        const alias = {
            'gatrolocal1': 'gastronomia-local1',
            'gastro-local1': 'gastronomia-local1',
            'gastro1': 'gastronomia-local1',
            'planeta-pancho': 'planeta-pancho'
        };
        slug = alias[slug] || slug || 'gastronomia-local1';
        return slug;
    };
    const tenantSlug = getTenantSlug();
    const publicOrderToken = String(
        (window.BusinessConfig && window.BusinessConfig.checkout && window.BusinessConfig.checkout.publicOrderToken)
        || window.PUBLIC_ORDER_TOKEN
        || ''
    ).trim();

    const payload = {
        tenant_slug: tenantSlug,
        public_order_token: publicOrderToken,
        order_type: orderType,
        table_number: orderType === 'mesa' ? data.mesaNumber : '',
        address: orderType === 'direccion' ? { address: data.address, locality: data.locality, geo: data.geo || null } : {},
        customer_phone: orderType === 'direccion' ? data.contactPhone : (orderType === 'espera' ? data.esperaPhone : ''),
        customer_name: orderType === 'espera' ? data.esperaName : (orderType === 'direccion' ? data.deliveryName : ''),
        items: cart.map(it => ({
            id: it.id,
            product_id: it.product_id || it.id,
            pack_id: it.pack_id || '',
            pack_label: it.pack_label || '',
            pack_size: it.pack_size || 1,
            name: it.name,
            price: (() => {
                const explicitBase = (typeof it.base_price === 'number' && isFinite(it.base_price) && it.base_price > 0) ? it.base_price : 0;
                if (explicitBase > 0) return explicitBase;
                const modifiers = it && it.modifiers && typeof it.modifiers === 'object' ? it.modifiers : {};
                const rawAddons = Array.isArray(modifiers.addons) ? modifiers.addons : [];
                const addonsTotal = rawAddons.reduce((sum, addon) => {
                    const tp = parseInt(addon && addon.total_price, 10);
                    if (Number.isFinite(tp) && tp > 0) return sum + tp;
                    const up = parseInt(addon && addon.unit_price, 10);
                    const qty = parseInt(addon && addon.qty, 10) || 1;
                    if (Number.isFinite(up) && up > 0) return sum + (up * Math.max(1, qty));
                    return sum;
                }, 0);
                const display = parseInt(it && it.price, 10) || 0;
                const derived = display - addonsTotal;
                return derived > 0 ? derived : display;
            })(),
            quantity: it.quantity,
            modifiers: it.modifiers || {},
            notes: it.notes || ''
        })),
        order_notes: data.orderNotes,
        schedule_mode: data.scheduleMode === 'scheduled' ? 'scheduled' : 'asap',
        scheduled_for: data.scheduleMode === 'scheduled' ? String(data.scheduledFor || '') : ''
    };

    const origin = window.location.origin || '';
    const API_BASE = /^file:/i.test(origin) ? 'http://127.0.0.1:8000' : origin;

    console.log('Enviando orden al backend:', payload, 'a', API_BASE);

    const response = await fetch(new URL('/api/orders', API_BASE).toString(), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });
    if (!response.ok) {
        const errData = await response.json().catch(() => ({}));
        console.error('Error del servidor:', errData);
        throw new Error(errData.error || `HTTP error! status: ${response.status}`);
    }
    const responseData = await response.json();
    if (responseData.order_id) {
        console.log('Orden creada con ID:', responseData.order_id);
        const slug = getTenantSlug();
        localStorage.setItem('last_order_id_' + slug, responseData.order_id);
        localStorage.setItem('last_viewed_status_' + slug, responseData.status || 'pending');
    } else {
        console.warn('Backend no devolvió order_id', responseData);
    }
    return responseData;
}

function initDeliveryGeoUI() {
    const container = document.getElementById('order-address-fields');
    if (!container) return;

    if (document.getElementById('use-location-btn')) return;

    const btn = document.createElement('button');
    btn.type = 'button';
    btn.id = 'use-location-btn';
    btn.className = 'clear-cart-btn';
    btn.style.marginTop = '8px';
    btn.textContent = 'Usar mi ubicación';

    const editBtn = document.createElement('button');
    editBtn.type = 'button';
    editBtn.id = 'edit-geo-address-btn';
    editBtn.style.marginTop = '8px';
    editBtn.style.marginLeft = '6px';
    editBtn.style.height = '36px';
    editBtn.style.padding = '0 12px';
    editBtn.style.borderRadius = '10px';
    editBtn.style.border = '1px solid #e5e7eb';
    editBtn.style.background = '#f9fafb';
    editBtn.style.color = '#374151';
    editBtn.style.fontWeight = '700';
    editBtn.style.display = 'none';
    editBtn.textContent = 'Editar dirección';

    const preview = document.createElement('div');
    preview.id = 'delivery-location-preview';
    preview.style.marginTop = '6px';
    preview.style.fontSize = '12px';
    preview.style.color = '#555';

    const latEl = document.createElement('input');
    latEl.type = 'hidden';
    latEl.id = 'delivery-geo-lat';
    const lngEl = document.createElement('input');
    lngEl.type = 'hidden';
    lngEl.id = 'delivery-geo-lng';
    const accEl = document.createElement('input');
    accEl.type = 'hidden';
    accEl.id = 'delivery-geo-acc';
    const tsEl = document.createElement('input');
    tsEl.type = 'hidden';
    tsEl.id = 'delivery-geo-ts';

    const first = container.firstChild;
    container.insertBefore(btn, first);
    container.insertBefore(editBtn, first);
    container.insertBefore(preview, first);
    container.insertBefore(latEl, first);
    container.insertBefore(lngEl, first);
    container.insertBefore(accEl, first);
    container.insertBefore(tsEl, first);

    const setPreview = (html) => { preview.innerHTML = html || ''; };
    const notifyCartTotals = () => {
        try { updateCartDisplay(); } catch (_) {}
    };

    if (!navigator.geolocation) {
        btn.disabled = true;
        btn.style.opacity = '0.6';
        setPreview('Tu navegador no permite compartir ubicación.');
        return;
    }

    const addressInput = document.getElementById('delivery-address');
    const localityInput = document.getElementById('delivery-locality');
    const nameInput = document.getElementById('delivery-name');
    const phoneInput = document.getElementById('contact-phone');
    const esperaNameInput = document.getElementById('espera-name');
    const esperaPhoneInput = document.getElementById('espera-phone');

    if (addressInput) {
        addressInput.setAttribute('autocomplete', 'street-address');
        if (!addressInput.getAttribute('name')) addressInput.setAttribute('name', 'delivery_address');
    }
    if (localityInput) {
        localityInput.setAttribute('autocomplete', 'address-level2');
        if (!localityInput.getAttribute('name')) localityInput.setAttribute('name', 'delivery_locality');
    }
    if (nameInput) {
        nameInput.setAttribute('autocomplete', 'name');
        if (!nameInput.getAttribute('name')) nameInput.setAttribute('name', 'delivery_name');
    }
    if (phoneInput) {
        phoneInput.setAttribute('autocomplete', 'tel');
        if (!phoneInput.getAttribute('name')) phoneInput.setAttribute('name', 'delivery_phone');
    }
    if (esperaNameInput) {
        esperaNameInput.setAttribute('autocomplete', 'name');
        if (!esperaNameInput.getAttribute('name')) esperaNameInput.setAttribute('name', 'pickup_name');
    }
    if (esperaPhoneInput) {
        esperaPhoneInput.setAttribute('autocomplete', 'tel');
        if (!esperaPhoneInput.getAttribute('name')) esperaPhoneInput.setAttribute('name', 'pickup_phone');
    }

    const defaultAddressPlaceholder = addressInput ? String(addressInput.getAttribute('placeholder') || '') : '';
    const defaultLocalityPlaceholder = localityInput ? String(localityInput.getAttribute('placeholder') || '') : '';

    const setGeoResolvingState = (resolving) => {
        const { addressInput: liveAddressInput, localityInput: liveLocalityInput } = getLiveDeliveryAddressInputs();
        if (liveAddressInput) {
            const currentValue = String(liveAddressInput.value || '').trim();
            liveAddressInput.placeholder = resolving && !currentValue
                ? 'Completando dirección...'
                : defaultAddressPlaceholder;
        }
        if (liveLocalityInput) {
            const currentValue = String(liveLocalityInput.value || '').trim();
            liveLocalityInput.placeholder = resolving && !currentValue
                ? 'Completando localidad...'
                : defaultLocalityPlaceholder;
        }
    };

    const lockAddressInputs = () => {
        const { addressInput: liveAddressInput, localityInput: liveLocalityInput } = getLiveDeliveryAddressInputs();
        const hasGeo = Number.isFinite(parseFloat((latEl.value || '').trim())) && Number.isFinite(parseFloat((lngEl.value || '').trim()));
        if (!hasGeo) return;
        const hasAnyText = Boolean((liveAddressInput && String(liveAddressInput.value || '').trim()) || (liveLocalityInput && String(liveLocalityInput.value || '').trim()));
        if (!hasAnyText) return;
        if (liveAddressInput) {
            liveAddressInput.readOnly = true;
            liveAddressInput.style.backgroundColor = '#f9fafb';
        }
        if (liveLocalityInput) {
            liveLocalityInput.readOnly = true;
            liveLocalityInput.style.backgroundColor = '#f9fafb';
        }
        editBtn.style.display = '';
        editBtn.textContent = 'Editar dirección';
    };

    const refreshGeoFromStoredCoords = async (options = {}) => {
        const { retry = false } = options || {};
        const lat = parseFloat((latEl.value || '').trim());
        const lng = parseFloat((lngEl.value || '').trim());
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
        const appliedNow = applyGeoAutofill('', '');
        if (appliedNow.address && appliedNow.locality) {
            lockAddressInputs();
            return appliedNow;
        }
        const resolved = await tryAutofillFromGeo(lat, lng);
        if ((!resolved || (!resolved.address && !resolved.locality)) && retry) {
            return new Promise(resolve => {
                setTimeout(async () => {
                    resolve(await tryAutofillFromGeo(lat, lng));
                }, 650);
            });
        }
        return resolved;
    };

    const unlockAddressInputs = () => {
        const { addressInput: liveAddressInput, localityInput: liveLocalityInput } = getLiveDeliveryAddressInputs();
        if (liveAddressInput) {
            liveAddressInput.readOnly = false;
            liveAddressInput.style.backgroundColor = '';
        }
        if (liveLocalityInput) {
            liveLocalityInput.readOnly = false;
            liveLocalityInput.style.backgroundColor = '';
        }
        editBtn.style.display = '';
        editBtn.textContent = 'Bloquear dirección';
    };

    editBtn.addEventListener('click', () => {
        const { addressInput: liveAddressInput, localityInput: liveLocalityInput } = getLiveDeliveryAddressInputs();
        const isLocked = Boolean((liveAddressInput && liveAddressInput.readOnly) || (liveLocalityInput && liveLocalityInput.readOnly));
        if (isLocked) unlockAddressInputs();
        else lockAddressInputs();
    });

    const tryAutofillFromGeo = async (lat, lng) => {
        setGeoResolvingState(true);
        const resolved = await reverseGeocodeFromCoords(lat, lng);
        if (!resolved) {
            setGeoResolvingState(false);
            return null;
        }
        const applied = applyGeoAutofill(resolved.address, resolved.locality);
        if (!applied.address && !applied.locality) {
            console.warn('Geolocalización sin dirección utilizable:', resolved.payload);
        }
        setGeoResolvingState(false);
        lockAddressInputs();
        notifyCartTotals();
        return applied;
    };

    const renderFromValues = (options = {}) => {
        const lat = parseFloat((latEl.value || '').trim());
        const lng = parseFloat((lngEl.value || '').trim());
        const accuracy = parseFloat((accEl.value || '').trim());
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) { setPreview(''); return; }
        const url = `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(`${lat},${lng}`)}`;
        const accTxt = Number.isFinite(accuracy) ? ` (precisión aprox. ${Math.round(accuracy)}m)` : '';
        setPreview(`Ubicación lista${accTxt}: <a href="${url}" target="_blank" rel="noopener">ver en mapa</a>`);
    };

    try {
        const saved = sessionStorage.getItem('delivery_geo');
        if (saved) {
            const j = JSON.parse(saved);
            if (j && Number.isFinite(Number(j.lat)) && Number.isFinite(Number(j.lng))) {
                latEl.value = String(j.lat);
                lngEl.value = String(j.lng);
                accEl.value = (j.accuracy == null) ? '' : String(j.accuracy);
                tsEl.value = (j.ts == null) ? '' : String(j.ts);
                renderFromValues();
                const a = String(j.address || '').trim();
                const l = String(j.locality || '').trim();
                applyGeoAutofill(a, l);
                lockAddressInputs();
                notifyCartTotals();
                const { addressInput: liveAddressInput, localityInput: liveLocalityInput } = getLiveDeliveryAddressInputs();
                if ((!a || !l) && ((liveAddressInput && !(liveAddressInput.value || '').trim()) || (liveLocalityInput && !(liveLocalityInput.value || '').trim()))) {
                    setGeoResolvingState(true);
                    renderFromValues();
                    (async () => {
                        const res = await refreshGeoFromStoredCoords({ retry: true });
                        if (!res) setGeoResolvingState(false);
                        if (!res) return;
                        try {
                            const saved2 = sessionStorage.getItem('delivery_geo');
                            const j2 = saved2 ? JSON.parse(saved2) : {};
                            if (j2 && typeof j2 === 'object') {
                                j2.address = res.address || j2.address || '';
                                j2.locality = res.locality || j2.locality || '';
                                sessionStorage.setItem('delivery_geo', JSON.stringify(j2));
                            }
                        } catch (_) {}
                        notifyCartTotals();
                    })();
                }
            }
        }
    } catch (_) {}

    btn.addEventListener('click', () => {
        btn.disabled = true;
        btn.style.opacity = '0.7';
        setPreview('Obteniendo ubicación...');
        navigator.geolocation.getCurrentPosition(
            (pos) => {
                const lat = pos && pos.coords ? pos.coords.latitude : null;
                const lng = pos && pos.coords ? pos.coords.longitude : null;
                const acc = pos && pos.coords ? pos.coords.accuracy : null;
                if (Number.isFinite(lat) && Number.isFinite(lng)) {
                    latEl.value = String(lat);
                    lngEl.value = String(lng);
                    accEl.value = Number.isFinite(acc) ? String(acc) : '';
                    tsEl.value = pos && pos.timestamp ? String(pos.timestamp) : '';
                    try {
                        const { addressInput: liveAddressInput, localityInput: liveLocalityInput } = getLiveDeliveryAddressInputs();
                        sessionStorage.setItem('delivery_geo', JSON.stringify({
                            lat,
                            lng,
                            accuracy: Number.isFinite(acc) ? acc : null,
                            ts: pos && pos.timestamp ? pos.timestamp : null,
                            address: liveAddressInput ? String(liveAddressInput.value || '').trim() : '',
                            locality: liveLocalityInput ? String(liveLocalityInput.value || '').trim() : ''
                        }));
                    } catch (_) {}
                    setGeoResolvingState(true);
                    renderFromValues();
                    notifyCartTotals();
                    (async () => {
                        const res = await refreshGeoFromStoredCoords({ retry: true });
                        if (!res) setGeoResolvingState(false);
                        if (res && (res.address || res.locality)) {
                            try {
                                const saved2 = sessionStorage.getItem('delivery_geo');
                                const j2 = saved2 ? JSON.parse(saved2) : {};
                                if (j2 && typeof j2 === 'object') {
                                    j2.address = res.address || j2.address || '';
                                    j2.locality = res.locality || j2.locality || '';
                                    sessionStorage.setItem('delivery_geo', JSON.stringify(j2));
                                }
                            } catch (_) {}
                            notifyCartTotals();
                            const lat2 = parseFloat((latEl.value || '').trim());
                            const lng2 = parseFloat((lngEl.value || '').trim());
                            const accuracy2 = parseFloat((accEl.value || '').trim());
                            if (Number.isFinite(lat2) && Number.isFinite(lng2)) {
                                const url2 = `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(`${lat2},${lng2}`)}`;
                                const accTxt2 = Number.isFinite(accuracy2) ? ` (precisión aprox. ${Math.round(accuracy2)}m)` : '';
                                setPreview(`Ubicación lista${accTxt2}: <a href="${url2}" target="_blank" rel="noopener">ver en mapa</a>`);
                            }
                        }
                    })();
                } else {
                    latEl.value = '';
                    lngEl.value = '';
                    accEl.value = '';
                    tsEl.value = '';
                    setPreview('No se pudo leer tu ubicación.');
                }
                btn.disabled = false;
                btn.style.opacity = '';
            },
            (err) => {
                btn.disabled = false;
                btn.style.opacity = '';
                const code = err && err.code ? Number(err.code) : 0;
                if (code === 1) setPreview('Permiso denegado para acceder a tu ubicación.');
                else if (code === 2) setPreview('No se pudo determinar tu ubicación (señal débil).');
                else if (code === 3) setPreview('Tiempo de espera agotado al obtener tu ubicación.');
                else setPreview('No se pudo obtener tu ubicación.');
            },
            { enableHighAccuracy: true, timeout: 12000, maximumAge: 0 }
        );
    });

    document.querySelectorAll('input[name="orderType"]').forEach(radio => {
        radio.addEventListener('change', () => {
            if (!radio.checked || radio.value !== 'direccion') return;
            const { addressInput: liveAddressInput, localityInput: liveLocalityInput } = getLiveDeliveryAddressInputs();
            const hasText = Boolean(String(liveAddressInput?.value || '').trim() || String(liveLocalityInput?.value || '').trim());
            if (hasText) return;
            (async () => {
                await refreshGeoFromStoredCoords({ retry: true });
            })();
        });
    });
}

// ---------------------------------------------------------------------------
// Disponibilidad del local y programación de pedidos (carta online)
// ---------------------------------------------------------------------------
const ORDER_AVAILABILITY_TTL_MS = 60 * 1000;
let __orderAvailabilityCache = { at: 0, slug: '', data: null };

function resolveAvailabilitySlug() {
    let slug = getBusinessSlug();
    const alias = {
        'gatrolocal1': 'gastronomia-local1',
        'gastro-local1': 'gastronomia-local1',
        'gastro1': 'gastronomia-local1'
    };
    return alias[slug] || slug || 'gastronomia-local1';
}

async function fetchOrderAvailability(force) {
    const slug = resolveAvailabilitySlug();
    const now = Date.now();
    if (
        !force
        && __orderAvailabilityCache.data
        && __orderAvailabilityCache.slug === slug
        && (now - __orderAvailabilityCache.at) < ORDER_AVAILABILITY_TTL_MS
    ) {
        return __orderAvailabilityCache.data;
    }
    const origin = window.location.origin || '';
    const API_BASE = /^file:/i.test(origin) ? 'http://127.0.0.1:8000' : origin;
    try {
        const url = new URL('/api/public/order_availability', API_BASE);
        url.searchParams.set('tenant_slug', slug);
        url.searchParams.set('_', String(now));
        const res = await fetch(url.toString(), { cache: 'no-store', credentials: 'same-origin' });
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const data = await res.json();
        __orderAvailabilityCache = { at: now, slug, data };
        return data;
    } catch (e) {
        console.warn('No se pudo obtener la disponibilidad del local:', e);
        return __orderAvailabilityCache.data || null;
    }
}

function buildOrderScheduleBox() {
    if (document.getElementById('order-schedule-box')) return;
    const anchor = document.getElementById('order-notes-box') || document.querySelector('.cart-order-options');
    if (!anchor || !anchor.parentNode) return;

    const wrap = document.createElement('div');
    wrap.className = 'order-extra order-schedule-extra';
    wrap.id = 'order-schedule-box';
    wrap.style.marginTop = '8px';
    wrap.style.display = 'none';
    wrap.innerHTML = '' +
        '<p id="store-status-banner" style="display:none;margin:0 0 8px;padding:8px 10px;border-radius:8px;background:#fff4e5;color:#8a4b00;font-weight:600;font-size:13px;"></p>' +
        '<label class="order-type-label">Horario del pedido</label>' +
        '<div class="order-type-switch order-schedule-switch" id="schedule-mode-switch">' +
        '  <label id="schedule-asap-label" class="schedule-mode-option"><input type="radio" name="scheduleMode" value="asap" checked> Lo antes posible</label>' +
        '  <label id="schedule-later-label" class="schedule-mode-option"><input type="radio" name="scheduleMode" value="scheduled"> <i class="fas fa-calendar-check" aria-hidden="true"></i> Programar</label>' +
        '</div>' +
        '<div id="schedule-slot-wrap" class="schedule-slot-wrap" style="display:none;margin-top:8px;">' +
        '  <label for="schedule-slot">Quiero que esté listo a las...</label>' +
        '  <div class="schedule-select-wrap"><select id="schedule-slot" class="schedule-slot-select"></select></div>' +
        '  <div id="schedule-selected-summary" class="cart-order-mode-summary schedule-selected-summary" style="display:none;"></div>' +
        '</div>';

    anchor.parentNode.insertBefore(wrap, anchor.nextSibling);

    wrap.querySelectorAll('input[name="scheduleMode"]').forEach(radio => {
        radio.addEventListener('change', () => {
            const slotWrap = document.getElementById('schedule-slot-wrap');
            if (slotWrap) slotWrap.style.display = (radio.checked && radio.value === 'scheduled') ? '' : 'none';
            syncScheduleModeUI();
            syncScheduleSelectedSummary();
        });
    });
    const slotSel = wrap.querySelector('#schedule-slot');
    if (slotSel) {
        slotSel.addEventListener('change', () => {
            syncScheduleSelectedSummary();
        });
    }
}

function syncScheduleModeUI() {
    const switchEl = document.getElementById('schedule-mode-switch');
    if (!switchEl) return;
    const labels = Array.from(switchEl.querySelectorAll('label'));
    const visible = labels.filter(label => label.style.display !== 'none');
    switchEl.classList.toggle('single-choice', visible.length <= 1);
}

function syncScheduleSelectedSummary() {
    const summary = document.getElementById('schedule-selected-summary');
    const slotSel = document.getElementById('schedule-slot');
    const schedRadio = document.querySelector('input[name="scheduleMode"][value="scheduled"]');
    if (!summary || !slotSel || !schedRadio || !schedRadio.checked || !slotSel.value) {
        if (summary) {
            summary.style.display = 'none';
            summary.textContent = '';
        }
        return;
    }
    const label = slotSel.selectedOptions && slotSel.selectedOptions[0]
        ? String(slotSel.selectedOptions[0].textContent || '').trim()
        : String(slotSel.value || '').trim();
    summary.textContent = label ? `Horario elegido: ${label}` : '';
    summary.style.display = label ? '' : 'none';
}

function syncOrderTypeRadiosWithAvailability(avail) {
    const radios = Array.from(document.querySelectorAll('input[name="orderType"]'));
    if (!radios.length) return;
    const mustSchedule = Boolean(avail && avail.has_hours && avail.block_when_closed && !avail.is_open);
    const mesaRadio = radios.find(r => r.value === 'mesa');
    const mesaLabel = mesaRadio ? mesaRadio.closest('label') : null;
    if (mesaLabel) mesaLabel.style.display = mustSchedule ? 'none' : '';
    if (mustSchedule && mesaRadio && mesaRadio.checked) {
        const alt = radios.find(r => r.value === 'direccion') || radios.find(r => r.value === 'espera');
        if (alt) {
            alt.checked = true;
            alt.dispatchEvent(new Event('change', { bubbles: true }));
        }
    }
}

function applyAvailabilityToUI(avail) {
    const box = document.getElementById('order-schedule-box');
    if (!box) return;
    syncOrderTypeRadiosWithAvailability(avail);
    if (!avail || !avail.has_hours) {
        box.style.display = 'none';
        return;
    }

    const slotWrap = document.getElementById('schedule-slot-wrap');
    const slotSel = document.getElementById('schedule-slot');
    const banner = document.getElementById('store-status-banner');
    const asapRadio = document.querySelector('input[name="scheduleMode"][value="asap"]');
    const schedRadio = document.querySelector('input[name="scheduleMode"][value="scheduled"]');
    const asapLabel = document.getElementById('schedule-asap-label');
    const laterLabel = document.getElementById('schedule-later-label');

    box.style.display = '';
    const isOpen = Boolean(avail.is_open);
    const allowScheduleOpen = Boolean(avail.allow_schedule_when_open);
    const slots = Array.isArray(avail.slots) ? avail.slots : [];

    if (slotSel) {
        slotSel.innerHTML = '';
        slots.forEach(slot => {
            const opt = document.createElement('option');
            opt.value = String(slot.iso || '');
            opt.textContent = String(slot.label || slot.local || opt.value);
            slotSel.appendChild(opt);
        });
    }

    if (isOpen && !allowScheduleOpen) {
        if (asapLabel) asapLabel.style.display = '';
        if (laterLabel) laterLabel.style.display = 'none';
        if (asapRadio) asapRadio.checked = true;
        if (slotWrap) slotWrap.style.display = 'none';
        if (banner) { banner.style.display = 'none'; banner.textContent = ''; }
        syncScheduleModeUI();
        syncScheduleSelectedSummary();
        return;
    }

    const mustSchedule = !isOpen;
    const hasSlots = slots.length > 0;
    if (asapLabel) asapLabel.style.display = mustSchedule ? 'none' : '';
    if (laterLabel) laterLabel.style.display = (mustSchedule && !hasSlots) ? 'none' : '';
    if (mustSchedule && hasSlots && schedRadio) schedRadio.checked = true;
    if (slotWrap) slotWrap.style.display = (schedRadio && schedRadio.checked && hasSlots) ? '' : 'none';
    syncScheduleModeUI();
    syncScheduleSelectedSummary();

    if (banner) {
        if (mustSchedule) {
            banner.style.display = '';
            const firstLabel = String((slots[0] && slots[0].label) || '').trim();
            banner.textContent = hasSlots
                ? (firstLabel
                    ? `El local está cerrado ahora. Programá tu pedido (primera franja: ${firstLabel}).`
                    : 'El local está cerrado ahora. Programá tu pedido para más tarde.')
                : 'El local está cerrado ahora y no hay franjas disponibles para programar.';
        } else {
            banner.style.display = 'none';
            banner.textContent = '';
        }
    }
}

async function refreshOrderAvailability(force) {
    buildOrderScheduleBox();
    const avail = await fetchOrderAvailability(force);
    applyAvailabilityToUI(avail);
    return avail;
}

document.addEventListener('DOMContentLoaded', () => {
    try { initDeliveryGeoUI(); } catch (e) { console.error(e); }
    try { refreshOrderAvailability(false); } catch (e) { console.error(e); }
});
