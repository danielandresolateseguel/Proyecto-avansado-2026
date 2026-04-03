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
export function getBusinessSlug() {
    // 1) Permitir override por querystring para pruebas multi-tenant (?tenant_slug=xxx)
    try {
        const url = new URL(window.location.href);
        const qsSlug = url.searchParams.get('tenant_slug') || url.searchParams.get('slug') || url.searchParams.get('tenant');
        if (qsSlug && qsSlug.trim()) {
            const slug = qsSlug.trim();
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
    
    // 3) Fallback: Recuperar de sessionStorage (útil si se recarga la página sin querystring)
    if (!slug) {
        try {
            const stored = sessionStorage.getItem('current_tenant_slug');
            if (stored) slug = stored;
        } catch(e) {}
    }

    // 4) Fallback al nombre del archivo (elchef.html -> elchef) si nada definido
    if (!slug) {
        try {
            const name = (window.location.pathname.split('/').pop() || '').replace(/\.html$/,'').trim();
            if (name && name !== 'index') slug = name;
        } catch (e) {}
    }
    
    return slug;
}

export function getWhatsappNumber() {
    return (window.BusinessConfig && window.BusinessConfig.checkout && window.BusinessConfig.checkout.whatsappNumber)
        || window.WHATSAPP_NUMBER
        || '+5492615893590';
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
    fetch(url).then(res => {
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
