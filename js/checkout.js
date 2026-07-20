/**
 * Checkout and WhatsApp Logic
 */
import { cart, clearCart, updateCartDisplay } from './cart.js?v=8';
import { closeCartUI } from './ui.js?v=8';
import { getWhatsappNumber, CATEGORY, getCheckoutMode, getWhatsappEnabled, getWhatsappTemplate, getBusinessSlug, formatMoneyWithCode, calculateShippingQuote } from './config.js?v=8';

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
        await sendOrderToBackend(orderType, { mesaNumber, address, locality, contactPhone, esperaName, esperaPhone, deliveryName, orderNotes, geo }, totalNumber);

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

    const payload = {
        tenant_slug: getTenantSlug(),
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
        order_notes: data.orderNotes
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

document.addEventListener('DOMContentLoaded', () => {
    try { initDeliveryGeoUI(); } catch (e) { console.error(e); }
});
