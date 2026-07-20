/**
 * Configuration and Constants
 */

// Página actual
export const PAGE = (document.body && document.body.dataset && document.body.dataset.page) || '';

// Configuración por rubro/negocio
export const CATEGORY = window.CATEGORY || (document.body ? document.body.getAttribute('data-category') : null) || 'general';
export const VENDOR_ID = window.VENDOR_ID || (document.body ? document.body.getAttribute('data-vendor') : null) || 'default';
export const VENDOR_SLUG = window.VENDOR_SLUG || window.BUSINESS_SLUG || (document.body ? document.body.getAttribute('data-slug') : null) || '';
export const THEME = window.THEME || (document.body ? document.body.getAttribute('data-theme') : null) || '';
export const CART_KEY_PREFIX = window.CART_KEY_PREFIX || 'cart';

// Claves de almacenamiento
export const LEGACY_CART_STORAGE_KEY = `${CART_KEY_PREFIX}_${CATEGORY}_${VENDOR_ID}`;
export const KEY_NAMESPACE = [CATEGORY, (VENDOR_SLUG || VENDOR_ID || 'default'), (THEME || PAGE || '')].filter(Boolean).join('_');
export const CART_STORAGE_KEY = window.CART_STORAGE_KEY || (`${CART_KEY_PREFIX}_${KEY_NAMESPACE}`);

// Helpers de configuración
function normalizeBusinessSlugAlias(value) {
    const slug = String(value || '').trim();
    if (!slug) return '';
    if (slug === 'public-menu-base') return 'gastronomia-local1';
    return slug;
}

export function getBusinessSlug() {
    // 1) Permitir override por querystring para pruebas multi-tenant (?tenant_slug=xxx)
    try {
        const url = new URL(window.location.href);
        const qsSlug = url.searchParams.get('tenant_slug') || url.searchParams.get('slug') || url.searchParams.get('tenant');
        if (qsSlug && qsSlug.trim()) {
            const slug = normalizeBusinessSlugAlias(qsSlug);
            // Persistir en sessionStorage para mantener el contexto en recargas
            try { sessionStorage.setItem('current_tenant_slug', slug); } catch(e){}
            return slug;
        }
    } catch (e) {}
    
    // 2) Configuración explícita en la página
    let slug = window.BUSINESS_SLUG 
            || VENDOR_SLUG 
            || (document.body && document.body.dataset && (document.body.dataset.slug || document.body.dataset.tenant)) 
            || '';
    slug = normalizeBusinessSlugAlias(slug);
    
    // 3) Fallback: Recuperar de sessionStorage (útil si se recarga la página sin querystring)
    if (!slug) {
        try {
            const stored = sessionStorage.getItem('current_tenant_slug');
            if (stored) slug = normalizeBusinessSlugAlias(stored);
        } catch(e) {}
    }

    // 4) Fallback al nombre del archivo (elchef.html -> elchef) si nada definido
    if (!slug) {
        try {
            const name = (window.location.pathname.split('/').pop() || '').replace(/\.html$/,'').trim();
            if (name && name !== 'index') slug = normalizeBusinessSlugAlias(name);
        } catch (e) {}
    }
    
    return slug;
}

export function getWhatsappNumber() {
    const configured = (window.BusinessConfig && window.BusinessConfig.checkout && window.BusinessConfig.checkout.whatsappNumber)
        || window.WHATSAPP_NUMBER
        || '';
    return String(configured || '').trim();
}

export function getWhatsappEnabled() {
    if (window.BusinessConfig && window.BusinessConfig.checkout && typeof window.BusinessConfig.checkout.whatsappEnabled !== 'undefined') {
        return window.BusinessConfig.checkout.whatsappEnabled;
    }
    return true; // Default to enabled
}

export function getWhatsappTemplate() {
    if (window.BusinessConfig && window.BusinessConfig.checkout && window.BusinessConfig.checkout.whatsappTemplate) {
        return window.BusinessConfig.checkout.whatsappTemplate;
    }
    return null;
}


export function getCheckoutMode() {
    const modeFromConfig = (window.BusinessConfig && window.BusinessConfig.checkout && window.BusinessConfig.checkout.mode) || undefined;
    const fallbackByCategory = (CATEGORY === 'servicios' ? 'whatsapp' : CATEGORY === 'comercio' ? 'whatsapp' : CATEGORY === 'gastronomia' ? 'mesa' : 'general');
    return modeFromConfig || window.CHECKOUT_MODE || fallbackByCategory;
}

export const CHECKOUT_MODE = getCheckoutMode();

// Carga de configuración remota
export function loadBusinessConfig(callback) {
    const slug = getBusinessSlug();
    if (!slug || (window.BusinessConfig && window.BusinessConfig.__loaded)) {
        if (callback) callback();
        return;
    }
    
    const url = `/api/config?slug=${slug}`;
    fetch(url, { cache: 'no-store', credentials: 'same-origin' }).then(res => {
        if (!res.ok) throw new Error('No config JSON found');
        return res.json();
    }).then(json => {
        window.BusinessConfig = Object.assign({}, window.BusinessConfig || {}, json, { __loaded: true });
        document.dispatchEvent(new CustomEvent('businessconfig:ready'));
        console.info('BusinessConfig loaded from', url);
        if (callback) callback();
    }).catch(() => {
        // Silencio si no hay config
        if (callback) callback();
    });
}

export function getCurrencySettings() {
    const code = (window.CURRENCY_CODE || (window.BusinessConfig && window.BusinessConfig.currency_code) || 'ARS').toString().trim().toUpperCase();
    const locale = (window.CURRENCY_LOCALE || (window.BusinessConfig && window.BusinessConfig.currency_locale) || 'es-AR').toString().trim() || 'es-AR';
    return { code, locale };
}

export function formatMoney(amount) {
    const { code, locale } = getCurrencySettings();
    try {
        const n = Number(amount || 0);
        const fmt = new Intl.NumberFormat(locale, { style: 'currency', currency: code, maximumFractionDigits: 0 });
        return fmt.format(n);
    } catch (_) {
        try {
            return '$' + parseInt(amount || 0, 10).toLocaleString(locale);
        } catch (_) {
            return '$' + String(parseInt(amount || 0, 10));
        }
    }
}

export function formatMoneyWithCode(amount) {
    const txt = formatMoney(amount);
    const { code } = getCurrencySettings();
    return txt + ' ' + code;
}

function _isFiniteNumber(n) {
    return typeof n === 'number' && Number.isFinite(n);
}

function _extractLatLng(obj) {
    if (!obj || typeof obj !== 'object') return null;
    const lat = Number(obj.lat ?? obj.latitude);
    const lng = Number(obj.lng ?? obj.lon ?? obj.longitude);
    if (!_isFiniteNumber(lat) || !_isFiniteNumber(lng)) return null;
    if (lat < -90 || lat > 90 || lng < -180 || lng > 180) return null;
    return { lat, lng };
}

function _haversineKm(a, b) {
    const r = 6371;
    const toRad = (deg) => (deg * Math.PI) / 180;
    const dLat = toRad(b.lat - a.lat);
    const dLng = toRad(b.lng - a.lng);
    const lat1 = toRad(a.lat);
    const lat2 = toRad(b.lat);
    const sinDLat = Math.sin(dLat / 2);
    const sinDLng = Math.sin(dLng / 2);
    const h = (sinDLat * sinDLat) + Math.cos(lat1) * Math.cos(lat2) * (sinDLng * sinDLng);
    return 2 * r * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
}

export function calculateShippingQuote(orderType, geo = null) {
    const type = String(orderType || '').trim().toLowerCase();
    if (type !== 'direccion') {
        return { cost: 0, distanceKm: null, baseCost: 0, extraCost: 0 };
    }

    const baseCost = parseInt(window.BusinessConfig && window.BusinessConfig.shipping_cost, 10) || 0;
    const sd = window.BusinessConfig && window.BusinessConfig.shipping_distance;
    if (!sd || typeof sd !== 'object' || !sd.enabled) {
        return { cost: Math.max(0, baseCost), distanceKm: null, baseCost: Math.max(0, baseCost), extraCost: 0 };
    }

    const origin = _extractLatLng(sd.origin || {});
    const dest = _extractLatLng(geo || {});
    if (!origin || !dest) {
        return { cost: Math.max(0, baseCost), distanceKm: null, baseCost: Math.max(0, baseCost), extraCost: 0 };
    }

    const includedKm = Number(sd.included_km || 0);
    const extraPerKm = parseInt(sd.extra_per_km, 10) || 0;
    if (!_isFiniteNumber(includedKm) || includedKm < 0 || extraPerKm <= 0) {
        return { cost: Math.max(0, baseCost), distanceKm: null, baseCost: Math.max(0, baseCost), extraCost: 0 };
    }

    const distanceKm = _haversineKm(origin, dest);
    if (!_isFiniteNumber(distanceKm)) {
        return { cost: Math.max(0, baseCost), distanceKm: null, baseCost: Math.max(0, baseCost), extraCost: 0 };
    }

    const extraKm = Math.max(0, distanceKm - includedKm);
    const extraCost = Math.max(0, Math.ceil(extraKm * extraPerKm));
    let cost = Math.max(0, baseCost + extraCost);
    const maxCost = parseInt(sd.max_cost, 10) || 0;
    if (maxCost > 0) cost = Math.min(cost, maxCost);
    return { cost, distanceKm, baseCost: Math.max(0, baseCost), extraCost };
}
