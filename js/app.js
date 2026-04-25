/**
 * Main Application Entry Point
 */
// FORCE CONFIG REFRESH: Clear cached config to ensure fresh load from backend
try {
    let slug = window.BUSINESS_SLUG;
    if (!slug) {
        // Fallback to URL parsing if not set by inline script
        const urlParams = new URLSearchParams(window.location.search);
        slug = urlParams.get('slug') || urlParams.get('tenant') || urlParams.get('tenant_slug');
    }
    if (!slug) slug = 'gastronomia-local1';

    localStorage.removeItem('ordersConfig_' + slug); // Remove slug-specific config
    localStorage.removeItem('ordersConfig'); // Remove legacy config
    console.log('Config cache cleared for update.');
} catch (e) { console.error('Error clearing cache', e); }

import { loadBusinessConfig, PAGE, CHECKOUT_MODE, getBusinessSlug } from './config.js?v=8';
import { 
    initCartElements, 
    loadCart, 
    addToCart, 
    clearCart, 
    updateCartDisplay,
    updateCartCount
} from './cart.js?v=8';
import { 
    bindAddToCartEvents, 
    initDiscountSwipe, 
    openDialog, 
    closeDialog,
    closeCartUI
} from './ui.js?v=10';
import { 
    initSearch
} from './search.js?v=8';
import { handleCheckout } from './checkout.js?v=11';
import { 
    initializeCarousel, 
    loadAndInitCarousel,
    nextSlide, 
    previousSlide, 
    goToSlide, 
    showSlide, 
    toggleAutoPlay,
    initInterestNav, 
    initInterestFocusState 
} from './carousel.js?v=8';
import { 
    scrollDiscounts,
    initProductModals,
    initInterestFiltering,
    initDynamicProducts
} from './ui.js?v=10';
 
 
import { initOrderStatus } from './order-status.js?v=11';

// Exponer funciones globales necesarias para HTML inline (onclick="...")
window.addToCart = function(id, name, price, imageSrc, event) {
    // Wrapper para adaptar la firma de la función antigua
    addToCart(id, name, price, imageSrc, event, null);
};

// Auto-fill table number from URL
document.addEventListener('DOMContentLoaded', () => {
    const urlParams = new URLSearchParams(window.location.search);
    const tableParam = urlParams.get('table') || urlParams.get('mesa');
    if (tableParam) {
        sessionStorage.setItem('preselected_table', tableParam);
    }

    const savedTable = sessionStorage.getItem('preselected_table');
    const mesaInput = document.getElementById('mesa-number');
    if (mesaInput && savedTable) {
        mesaInput.value = savedTable;
        mesaInput.style.borderColor = '#4caf50';
        mesaInput.style.backgroundColor = '#f1f8e9';
    }
});

window.clearCart = clearCart;

// Funciones de carrusel y navegación expuestas globalmente
window.nextSlide = nextSlide;
window.previousSlide = previousSlide;
window.goToSlide = goToSlide;
window.toggleAutoPlay = toggleAutoPlay;
window.scrollDiscounts = scrollDiscounts;

window.closeCartUI = closeCartUI;

function getApiBase() {
    const origin = window.location.origin || '';
    return /^file:/i.test(origin) ? 'http://127.0.0.1:8000' : origin;
}

async function fetchAuthMe() {
    try {
        const base = getApiBase();
        const r = await fetch(new URL('/api/auth/me', base).toString(), { cache: 'no-store', credentials: 'include' });
        if (!r.ok) return null;
        return await r.json();
    } catch (_) {
        return null;
    }
}

async function fetchCsrfToken() {
    try {
        const base = getApiBase();
        const r = await fetch(new URL('/api/auth/csrf', base).toString(), { cache: 'no-store', credentials: 'include' });
        if (!r.ok) return '';
        const j = await r.json();
        return String(j && j.token || '');
    } catch (_) {
        return '';
    }
}

function formatIntervalsForEditor(intervals) {
    if (!Array.isArray(intervals) || !intervals.length) return '';
    const parts = [];
    intervals.forEach(it => {
        if (!Array.isArray(it) || it.length < 2) return;
        const a = String(it[0] || '').trim();
        const b = String(it[1] || '').trim();
        if (!a || !b) return;
        parts.push(`${a}-${b}`);
    });
    return parts.join(', ');
}

function parseIntervalsForEditor(text) {
    const out = [];
    const t = String(text || '').trim();
    if (!t) return out;
    const segs = t.split(',').map(s => s.trim()).filter(Boolean);
    segs.forEach(seg => {
        const m = seg.split('-').map(s => s.trim()).filter(Boolean);
        if (m.length < 2) return;
        const a = m[0];
        const b = m[1];
        if (!/^\d{1,2}:\d{2}$/.test(a) || !/^\d{1,2}:\d{2}$/.test(b)) return;
        const hhA = parseInt(a.split(':')[0], 10);
        const hhB = parseInt(b.split(':')[0], 10);
        const mmA = parseInt(a.split(':')[1], 10);
        const mmB = parseInt(b.split(':')[1], 10);
        if (hhA < 0 || hhA > 23 || hhB < 0 || hhB > 23 || mmA < 0 || mmA > 59 || mmB < 0 || mmB > 59) return;
        out.push([a, b]);
    });
    return out;
}

function parseOpeningHoursText(text) {
    const dayMap = {
        'lun': 'mon', 'lunes': 'mon', 'mon': 'mon', 'monday': 'mon',
        'mar': 'tue', 'martes': 'tue', 'tue': 'tue', 'tuesday': 'tue',
        'mie': 'wed', 'mié': 'wed', 'miercoles': 'wed', 'miércoles': 'wed', 'wed': 'wed', 'wednesday': 'wed',
        'jue': 'thu', 'jueves': 'thu', 'thu': 'thu', 'thursday': 'thu',
        'vie': 'fri', 'viernes': 'fri', 'fri': 'fri', 'friday': 'fri',
        'sab': 'sat', 'sáb': 'sat', 'sabado': 'sat', 'sábado': 'sat', 'sat': 'sat', 'saturday': 'sat',
        'dom': 'sun', 'domingo': 'sun', 'sun': 'sun', 'sunday': 'sun'
    };
    const out = {};
    const lines = String(text || '').split('\n').map(s => s.trim()).filter(Boolean);
    lines.forEach(line => {
        const idx = line.indexOf(':');
        if (idx < 0) return;
        const dayRaw = line.slice(0, idx).trim().toLowerCase();
        const intervalsRaw = line.slice(idx + 1).trim();
        const key = dayMap[dayRaw];
        if (!key) return;
        const intervals = parseIntervalsForEditor(intervalsRaw);
        if (!intervals.length) return;
        out[key] = intervals;
    });
    return out;
}

function buildOpeningHoursText(openingHours) {
    const oh = openingHours && typeof openingHours === 'object' ? openingHours : {};
    const dayLabels = [
        ['mon', 'Lun'],
        ['tue', 'Mar'],
        ['wed', 'Mié'],
        ['thu', 'Jue'],
        ['fri', 'Vie'],
        ['sat', 'Sáb'],
        ['sun', 'Dom']
    ];
    const lines = [];
    dayLabels.forEach(([k, label]) => {
        const line = formatIntervalsForEditor(oh[k]);
        if (!line) return;
        lines.push(`${label}: ${line}`);
    });
    return lines.join('\n');
}

function initHeaderContact() {
    const headerContact = document.querySelector('.header-contact');
    const slug = getBusinessSlug() || 'gastronomia-local1';
    const base = getApiBase();
    const url = `${base}/api/tenant_header?tenant_slug=${encodeURIComponent(slug)}`;
    fetch(url).then(res => {
        if (!res.ok) return null;
        return res.json();
    }).then(data => {
        if (!data) return;
        const tenantName = (data.name || '').trim();
        if (tenantName) {
            document.title = tenantName;
            try {
                localStorage.setItem('cached_tenant_name_' + slug, tenantName);
            } catch (e) {}
        }
        const whatsappValue = (data.whatsapp || '').trim();
        const instagramValue = (data.instagram || '').trim();
        const instagramLabel = (data.instagram_label || '').trim();
        const locationLabel = (data.location_label || data.location || '').trim();
        const locationUrl = (data.location_url || '').trim();
        const openingHours = data.opening_hours || null;
        let openingHoursLabel = (data.opening_hours_label || '').trim();
        const footerTitle = (data.footer_title || '').trim();
        const footerTagline = (data.footer_tagline || '').trim();
        const footerBottom = (data.footer_bottom || '').trim();
        const contactEmail = (data.contact_email || '').trim();
        const timeZone = (data.timezone || '').trim();
        try {
            if (data.currency_code) window.CURRENCY_CODE = String(data.currency_code || '').toUpperCase();
            if (data.currency_locale) window.CURRENCY_LOCALE = String(data.currency_locale || '');
        } catch (_) {}
        const logoUrl = (data.logo_url || '').trim();
        
        // Helper functions for color manipulation
        const darken = (hex, amount) => {
            let col = hex.replace(/^#/, '');
            if (col.length === 3) col = col[0] + col[0] + col[1] + col[1] + col[2] + col[2];
            let num = parseInt(col, 16);
            let r = (num >> 16) + amount;
            let g = ((num >> 8) & 0x00FF) + amount;
            let b = (num & 0x0000FF) + amount;
            return '#' + (
                0x1000000 +
                (r < 255 ? (r < 1 ? 0 : r) : 255) * 0x10000 +
                (g < 255 ? (g < 1 ? 0 : g) : 255) * 0x100 +
                (b < 255 ? (b < 1 ? 0 : b) : 255)
            ).toString(16).slice(1);
        };

        const hexToRgba = (hex, alpha) => {
            let c = hex.replace(/^#/, '');
            if(c.length === 3) c = c[0] + c[0] + c[1] + c[1] + c[2] + c[2];
            const num = parseInt(c, 16);
            const r = (num >> 16) & 255;
            const g = (num >> 8) & 255;
            const b = num & 255;
            return `rgba(${r},${g},${b},${alpha})`;
        };

        const getContrastColor = (hex) => {
            let c = hex.replace(/^#/, '');
            if(c.length === 3) c = c[0] + c[0] + c[1] + c[1] + c[2] + c[2];
            const num = parseInt(c, 16);
            const r = (num >> 16) & 255;
            const g = (num >> 8) & 255;
            const b = num & 255;
            const hsp = Math.sqrt(0.299 * (r * r) + 0.587 * (g * g) + 0.114 * (b * b));
            return hsp > 127.5 ? '#000000' : '#ffffff';
        };

        // Apply Header Background Color
        // Si no hay configuración, usar fallback #333 (gris oscuro) en lugar de violeta
        const headerBgColor = data.header_bg_color || '#333333';
        if (headerBgColor) {
            // Smart Gradient: Detect if color is dark or light to apply contrast
            const isDark = getContrastColor(headerBgColor) === '#ffffff';
            // If dark, lighten the end. If light, darken the end.
            const endColor = isDark ? darken(headerBgColor, 50) : darken(headerBgColor, -50);
            
            const gradient = `linear-gradient(135deg, ${headerBgColor} 0%, ${endColor} 100%)`;
            document.body.style.setProperty('--header-bg', gradient);
            // Cache para evitar flash en la próxima carga
            try {
                localStorage.setItem('cached_header_bg_' + slug, gradient);
            } catch(e) {}
        }

        // Apply Theme Color
        const themeColor = data.theme_color || '#ff6a00';
        if (themeColor) {
            // Use document.body to override CSS definitions on body.sector-gastronomia
            document.body.style.setProperty('--gastro-accent', themeColor);
            
            document.body.style.setProperty('--gastro-accent-dark', darken(themeColor, -40));
            
            // Set all alpha variants used in CSS
            const alphas = [0.08, 0.18, 0.22, 0.25, 0.28, 0.35, 0.42, 0.55, 0.85];
            alphas.forEach(a => {
                let key = a.toString().split('.')[1]; 
                // key is "08", "18", etc.
                document.body.style.setProperty(`--gastro-accent-${key}`, hexToRgba(themeColor, a));
            });
            
            // Set light background tint
            document.body.style.setProperty('--gastro-bg', hexToRgba(themeColor, 0.04));
        }

        // Apply Section Background Colors
        const applySectionGradient = (variableName, textVarName, color) => {
            if (!color) return;
            const isDark = getContrastColor(color) === '#ffffff';
            // 3-stop gradient for more depth
            // Dark BG: Color -> Lighter -> Even Lighter
            // Light BG: Color -> Darker -> Even Darker
            const midColor = isDark ? darken(color, 30) : darken(color, -30);
            const endColor = isDark ? darken(color, 60) : darken(color, -60);
            
            const grad = `linear-gradient(135deg, ${color} 0%, ${midColor} 60%, ${endColor} 100%)`;
            
            document.body.style.setProperty(variableName, grad);
            document.body.style.setProperty(textVarName, getContrastColor(color));
        };

        applySectionGradient('--gastro-special-discounts-bg', '--gastro-special-discounts-text', data.featured_bg_color);
        applySectionGradient('--gastro-products-bg', '--gastro-products-text', data.menu_bg_color);
        applySectionGradient('--gastro-interest-bg', '--gastro-interest-text', data.interest_bg_color);

        const logoImg = document.querySelector('.site-logo img');
        if (logoImg) {
            logoImg.src = logoUrl || 'Imagenes/Epalogo.png';
        }

        // Dynamic Favicon Update
        const faviconUrl = logoUrl || 'Imagenes/Epalogo.png';
        let favicon = document.querySelector('link[rel="icon"]') || document.querySelector('link[rel="shortcut icon"]');
        if (!favicon) {
            favicon = document.createElement('link');
            favicon.rel = 'icon';
            document.head.appendChild(favicon);
        }
        
        // Attempt to create a circular favicon
        const img = new Image();
        img.crossOrigin = "Anonymous"; 
        img.onload = function() {
            try {
                const canvas = document.createElement('canvas');
                const ctx = canvas.getContext('2d');
                const size = 64; 
                canvas.width = size;
                canvas.height = size;

                // Draw circle clip
                ctx.beginPath();
                ctx.arc(size/2, size/2, size/2, 0, 2 * Math.PI);
                ctx.closePath();
                ctx.clip();

                // Draw image
                ctx.drawImage(img, 0, 0, size, size);
                
                // Update favicon
                favicon.href = canvas.toDataURL();
            } catch (e) {
                // Fallback to square if CORS or other issues prevent canvas export
                console.warn('Could not create circular favicon:', e);
                favicon.href = faviconUrl;
            }
        };
        img.onerror = function() {
             // Fallback if image fails to load
             favicon.href = faviconUrl;
        };
        img.src = faviconUrl;

        if (headerContact && whatsappValue) {
            const whatsappIcon = headerContact.querySelector('.fa-whatsapp');
            const whatsappLink = whatsappIcon ? whatsappIcon.closest('a') : null;
            if (whatsappLink) {
                const numberDigits = whatsappValue.replace(/\D+/g, '');
                if (numberDigits) {
                    whatsappLink.href = `https://wa.me/${numberDigits}`;
                }
                const span = whatsappLink.querySelector('span');
                if (span) span.textContent = whatsappValue;
                whatsappLink.setAttribute('data-tooltip', whatsappValue);
            }
        }
        if (headerContact && instagramValue) {
            const instagramIcon = headerContact.querySelector('.fa-instagram');
            const instagramLink = instagramIcon ? instagramIcon.closest('a') : null;
            if (instagramLink) {
                let handle = instagramValue;
                if (handle.startsWith('@')) handle = handle.slice(1);
                let urlValue = instagramValue;
                if (!/^https?:\/\//i.test(instagramValue)) {
                    urlValue = `https://www.instagram.com/${handle}`;
                }
                instagramLink.href = urlValue;
                const span = instagramLink.querySelector('span');
                if (span) {
                    if (instagramLabel) {
                        span.textContent = instagramLabel;
                    } else {
                        span.textContent = handle ? `@${handle}` : instagramValue;
                    }
                }
                instagramLink.setAttribute('data-tooltip', instagramLabel || instagramValue);
            }
        }
        if (headerContact && (locationLabel || locationUrl)) {
            const locationIcon = headerContact.querySelector('.fa-map-marker-alt');
            const locationLink = locationIcon ? locationIcon.closest('a') : null;
            if (locationLink) {
                let urlValue = locationUrl;
                if (!urlValue) {
                    urlValue = `https://maps.google.com/?q=${encodeURIComponent(locationLabel)}`;
                }
                locationLink.href = urlValue;
                const span = locationLink.querySelector('span');
                if (span) {
                    const labelText = locationLabel || 'Ver mapa';
                    span.textContent = labelText;
                }
                locationLink.setAttribute('data-tooltip', locationLabel || locationUrl || '');
            }
        }
        if (headerContact && openingHours && typeof openingHours === 'object') {
            let isOpenNow = false;
            let nextOpeningMinutes = null;
            let nextOpening = null;
            try {
                const days = ['sun','mon','tue','wed','thu','fri','sat'];
                const getZonedNow = () => {
                    if (!timeZone) return null;
                    if (!window.Intl || !Intl.DateTimeFormat) return null;
                    try {
                        const dtf = new Intl.DateTimeFormat('en-US', {
                            timeZone,
                            weekday: 'short',
                            hour: '2-digit',
                            minute: '2-digit',
                            hour12: false
                        });
                        const parts = dtf.formatToParts(new Date());
                        const weekday = (parts.find(p => p.type === 'weekday') || {}).value;
                        const hour = (parts.find(p => p.type === 'hour') || {}).value;
                        const minute = (parts.find(p => p.type === 'minute') || {}).value;
                        const dayKey = String(weekday || '').slice(0, 3).toLowerCase();
                        const idx = days.indexOf(dayKey);
                        const h = parseInt(hour, 10);
                        const m = parseInt(minute, 10);
                        if (idx < 0 || isNaN(h) || isNaN(m)) return null;
                        return { idx, minutes: h * 60 + m };
                    } catch (_) {
                        return null;
                    }
                };
                const zonedNow = getZonedNow();
                const now = new Date();
                const idx = zonedNow ? zonedNow.idx : now.getDay();
                const dayKey = days[idx];
                const prevKey = days[(idx + 6) % 7];
                const minutes = zonedNow ? zonedNow.minutes : (now.getHours() * 60 + now.getMinutes());
                const parseMinutes = (str) => {
                    if (!str || typeof str !== 'string') return null;
                    const parts = str.split(':');
                    if (parts.length < 2) return null;
                    const h = parseInt(parts[0], 10);
                    const m = parseInt(parts[1], 10);
                    if (isNaN(h) || isNaN(m)) return null;
                    return h * 60 + m;
                };
                const checkDay = (key, fromPrev) => {
                    const arr = openingHours[key];
                    if (!Array.isArray(arr) || !arr.length) return false;
                    for (let i = 0; i < arr.length; i++) {
                        const it = arr[i];
                        if (!Array.isArray(it) || it.length < 2) continue;
                        const s = parseMinutes(it[0]);
                        const e = parseMinutes(it[1]);
                        if (s == null || e == null) continue;
                        if (!fromPrev) {
                            if (s <= e) {
                                if (minutes >= s && minutes < e) return true;
                            } else {
                                if (minutes >= s) return true;
                            }
                        } else {
                            if (s > e) {
                                if (minutes < e) return true;
                            }
                        }
                    }
                    return false;
                };
                if (checkDay(dayKey, false) || checkDay(prevKey, true)) {
                    isOpenNow = true;
                }
                const findNextOpening = () => {
                    for (let offset = 0; offset < 7; offset++) {
                        const key = days[(idx + offset) % 7];
                        const arr = openingHours[key];
                        if (!Array.isArray(arr) || !arr.length) continue;
                        for (let i = 0; i < arr.length; i++) {
                            const it = arr[i];
                            if (!Array.isArray(it) || it.length < 2) continue;
                            const s = parseMinutes(it[0]);
                            const e = parseMinutes(it[1]);
                            if (s == null || e == null) continue;
                            if (offset === 0) {
                                if (s <= e) {
                                    if (s > minutes) {
                                        return { minutes: s, offset: offset, day: key };
                                    }
                                } else {
                                    if (minutes < s) {
                                        return { minutes: s, offset: offset, day: key };
                                    }
                                }
                            } else {
                                return { minutes: s, offset: offset, day: key };
                            }
                        }
                    }
                    return null;
                };
                const next = findNextOpening();
                if (next != null) {
                    nextOpeningMinutes = next.minutes;
                    nextOpening = next;
                }
            } catch (e) {}
            const clockItem = headerContact.querySelector('.clock-status') || (() => {
                const clockIcon = headerContact.querySelector('.fa-clock');
                return clockIcon ? (clockIcon.closest('.contact-item-compact') || clockIcon.closest('div')) : null;
            })();
            if (clockItem) {
                const span = clockItem.querySelector('span');
                if (span) {
                    if (isOpenNow) {
                        span.textContent = 'Abierto';
                        span.className = 'fw-bold';
                        span.style.color = '#ffffff';
                    } else if (nextOpeningMinutes != null) {
                        const h = Math.floor(nextOpeningMinutes / 60);
                        const m = nextOpeningMinutes % 60;
                        const hh = h.toString().padStart(2, '0');
                        const mm = m.toString().padStart(2, '0');
                        
                        let dayLabel = '';
                        if (nextOpening && nextOpening.offset > 0) {
                            const dayLabels = { mon: 'Lun', tue: 'Mar', wed: 'Mié', thu: 'Jue', fri: 'Vie', sat: 'Sáb', sun: 'Dom' };
                            if (nextOpening.offset === 1) {
                                dayLabel = 'Mañana ';
                            } else {
                                dayLabel = (dayLabels[nextOpening.day] || '') + ' ';
                            }
                        }
                        
                        span.textContent = 'Abre ' + dayLabel + 'a las ' + hh + ':' + mm + ' hs';
                        span.className = 'fw-bold';
                        span.style.color = '#ffffff';
                    } else {
                        span.textContent = 'Cerrado';
                        span.className = 'fw-bold';
                        span.style.color = '#ffffff';
                    }
                }
                const formatIntervals = (arr) => {
                    if (!Array.isArray(arr) || !arr.length) return '';
                    const parts = [];
                    for (let i = 0; i < arr.length; i++) {
                        const it = arr[i];
                        if (!Array.isArray(it) || it.length < 2) continue;
                        if (!it[0] || !it[1]) continue;
                        parts.push(it[0] + '-' + it[1]);
                    }
                    return parts.join(', ');
                };
                const dayLabels = { mon: 'Lun', tue: 'Mar', wed: 'Mié', thu: 'Jue', fri: 'Vie', sat: 'Sáb', sun: 'Dom' };
                const lines = [];
                Object.keys(dayLabels).forEach(key => {
                    const line = formatIntervals(openingHours[key]);
                    if (line) lines.push(dayLabels[key] + ': ' + line);
                });
                const tooltip = lines.join(' | ');
                if (tooltip) {
                    clockItem.setAttribute('data-tooltip', tooltip);
                }
                if (!openingHoursLabel && tooltip) {
                    openingHoursLabel = tooltip;
                }

                try {
                    const infoItems = Array.from(document.querySelectorAll('.restaurant-info .info-item'));
                    const hoursItem = infoItems.find(el => {
                        const icon = el.querySelector('i');
                        const strong = el.querySelector('strong');
                        const iconOk = icon && (icon.classList.contains('fa-clock') || icon.classList.contains('fas') && icon.classList.contains('fa-clock'));
                        const strongOk = strong && String(strong.textContent || '').trim().toLowerCase() === 'horarios';
                        return iconOk || strongOk;
                    });
                    if (hoursItem) {
                        const valueSpan = hoursItem.querySelector('.info-content span') || hoursItem.querySelector('span');
                        if (valueSpan && openingHoursLabel) valueSpan.textContent = openingHoursLabel;
                    }
                } catch (_) {}
            }
        }

        try {
            const infoItems = Array.from(document.querySelectorAll('.restaurant-info .info-item'));
            infoItems.forEach(el => {
                const strong = el.querySelector('strong');
                const key = String(strong && strong.textContent || '').trim().toLowerCase();
                const valueSpan = el.querySelector('.info-content span') || el.querySelector('span');
                if (!valueSpan) return;
                if (key === 'whatsapp' && whatsappValue) valueSpan.textContent = whatsappValue;
                if (key === 'instagram' && instagramValue) {
                    let handle = instagramValue;
                    if (handle.startsWith('@')) handle = handle.slice(1);
                    valueSpan.textContent = handle ? `@${handle}` : instagramValue;
                }
                if (key === 'ubicación' && locationLabel) valueSpan.textContent = locationLabel;
                if (key === 'horarios' && openingHoursLabel) valueSpan.textContent = openingHoursLabel;
            });
        } catch (_) {}

        try {
            const footer = document.querySelector('footer');
            if (footer) {
                const sections = Array.from(footer.querySelectorAll('.footer-section'));
                const bottomP = footer.querySelector('.footer-bottom p');
                const year = new Date().getFullYear();
                const displayName = tenantName || footerTitle || '';

                if (sections[0]) {
                    const h4 = sections[0].querySelector('h4');
                    const p = sections[0].querySelector('p');
                    if (h4 && (footerTitle || tenantName)) h4.textContent = `🍽️ ${footerTitle || tenantName}`;
                    if (p && footerTagline) p.textContent = footerTagline;
                }
                if (sections[1]) {
                    const ps = Array.from(sections[1].querySelectorAll('p'));
                    ps.forEach(p => {
                        const t = String(p.textContent || '').toLowerCase();
                        if (t.includes('whatsapp') && whatsappValue) {
                            p.textContent = '';
                            const i = document.createElement('i');
                            i.className = 'fab fa-whatsapp';
                            p.appendChild(i);
                            p.appendChild(document.createTextNode(` WhatsApp: ${whatsappValue}`));
                        }
                        if (t.includes('@') || t.includes('mail') || t.includes('correo') || t.includes('info@') || t.includes('gmail') || t.includes('email')) {
                            if (contactEmail) {
                                p.textContent = '';
                                const i = document.createElement('i');
                                i.className = 'fas fa-envelope';
                                p.appendChild(i);
                                p.appendChild(document.createTextNode(` ${contactEmail}`));
                            }
                        }
                    });
                }
                if (sections[2]) {
                    const ps = Array.from(sections[2].querySelectorAll('p'));
                    if (ps[0] && locationLabel) ps[0].textContent = locationLabel;
                    if (ps[1] && openingHoursLabel) ps[1].textContent = openingHoursLabel;
                }
                if (bottomP) {
                    if (footerBottom) {
                        bottomP.textContent = footerBottom;
                    } else if (displayName) {
                        bottomP.textContent = `© ${year} ${displayName}. Todos los derechos reservados.`;
                    }
                }
            }
        } catch (_) {}

        // Announcement Banner
        const announcementActive = data.announcement_active;
        const announcementText = (data.announcement_text || '').trim();
        const banner = document.getElementById('announcement-banner');
        const bannerText = document.getElementById('announcement-text');
        const bannerClose = document.getElementById('announcement-close');

        if (banner && bannerText && announcementActive && announcementText) {
            const closedKey = 'announcement_closed_' + slug;
            if (!sessionStorage.getItem(closedKey)) {
                bannerText.textContent = announcementText;
                banner.style.display = 'block';
                if (bannerClose) {
                    bannerClose.onclick = () => {
                        banner.style.display = 'none';
                        sessionStorage.setItem(closedKey, 'true');
                    };
                }
            }
        }
    }).catch(() => {
        // Fallback or error handling
    });
}

document.addEventListener('DOMContentLoaded', () => {
    if (PAGE === 'gastronomia') {
        document.body.classList.add('products-loading');
        const markProductsReady = () => {
            document.body.classList.remove('products-loading');
            document.body.classList.add('products-ready');
        };
        document.addEventListener('productsLoaded', () => {
            markProductsReady();
        }, { once: true });
        setTimeout(markProductsReady, 2000);
    }

    // Inicializar configuración
    loadBusinessConfig(() => {
        // Callback tras cargar config (opcional)
    });

    // Inicializar Carrusel
    loadAndInitCarousel(window.BUSINESS_SLUG || 'gastronomia-local1');

    // Inicializar elementos del carrito
    initCartElements();
    loadCart();

    // Bindings de eventos generales
    bindAddToCartEvents();
    initDiscountSwipe();
    initDynamicProducts();
    initProductModals();
    initInterestFiltering();

    // Inicializar Carrusel (si existe)
    // loadAndInitCarousel(window.BUSINESS_SLUG); // Removed duplicate call

    // Inicializar navegación de intereses (si existe)
    initInterestNav();
    initInterestFocusState();
    initOrderStatus();
    initHeaderContact();

    // Setup Buscador
    initSearch();

    // Setup Carrito UI (Overlay, toggle)
    const cartIcon = document.querySelector('.cart-icon');
    const shoppingCart = document.getElementById('shopping-cart');
    const closeCartBtn = document.getElementById('close-cart');
    const overlay = document.querySelector('.overlay') || document.createElement('div');
    if (!overlay.parentNode) {
        overlay.className = 'overlay';
        document.body.appendChild(overlay);
    }
    const floatingCart = document.getElementById('floating-cart');

    if (cartIcon && shoppingCart) {
        cartIcon.addEventListener('click', () => {
            shoppingCart.classList.add('active');
            overlay.classList.add('active');
            openDialog(shoppingCart);
            if (floatingCart) floatingCart.classList.remove('show');
        });
    }

    if (floatingCart) {
        floatingCart.addEventListener('click', () => {
            if (!shoppingCart) return;
            shoppingCart.classList.add('active');
            overlay.classList.add('active');
            openDialog(shoppingCart);
            floatingCart.classList.remove('show');
        });
    }

    if (closeCartBtn && shoppingCart) {
        closeCartBtn.addEventListener('click', () => {
            shoppingCart.classList.remove('active');
            overlay.classList.remove('active');
            closeDialog(shoppingCart);
            updateCartDisplay(); 
            updateCartCount(); // Restore floating cart visibility
        });
    }

    overlay.addEventListener('click', () => {
        if (shoppingCart) {
            shoppingCart.classList.remove('active');
            overlay.classList.remove('active');
            closeDialog(shoppingCart);
            updateCartDisplay();
            updateCartCount(); // Restore floating cart visibility
        }
    });

    // Checkout Button
    const checkoutBtn = document.getElementById('checkout-btn');
    if (checkoutBtn) {
        checkoutBtn.addEventListener('click', handleCheckout);
    }
    const clearCartBtn = document.getElementById('clear-cart-btn');
    if (clearCartBtn) {
        clearCartBtn.addEventListener('click', clearCart);
    }

    // Cambio de modalidad de pedido (Mesa/Dirección/Espera)
    const orderTypeRadios = document.querySelectorAll('input[name="orderType"]');
    const mesaFields = document.getElementById('order-mesa-fields');
    const addressFields = document.getElementById('order-address-fields');
    const esperaFields = document.getElementById('order-espera-fields');
    const orderNotesBox = document.getElementById('order-notes-box');
    const shoppingCartEl = document.getElementById('shopping-cart');
    const overlayEl = document.querySelector('.overlay');

    function syncOrderTypeUI(type) {
        if (mesaFields) mesaFields.style.display = type === 'mesa' ? '' : 'none';
        if (addressFields) addressFields.style.display = type === 'direccion' ? '' : 'none';
        if (esperaFields) esperaFields.style.display = type === 'espera' ? '' : 'none';
        if (orderNotesBox) orderNotesBox.style.display = '';
        updateCartDisplay();
    }
    orderTypeRadios.forEach(radio => {
        radio.addEventListener('change', () => {
            const selected = radio.value;
            syncOrderTypeUI(selected);
        });
    });
    const checkedRadio = document.querySelector('input[name="orderType"]:checked');
    syncOrderTypeUI(checkedRadio ? checkedRadio.value : 'mesa');

    // Filtros de Categoría (Gastronomía)
    if (PAGE === 'gastronomia') {
        const categoryFilter = document.getElementById('category-filter');
        if (categoryFilter) {
            const btns = categoryFilter.querySelectorAll('.filter-btn');
            btns.forEach(btn => {
                btn.addEventListener('click', () => {
                    btns.forEach(b => b.classList.remove('active'));
                    btn.classList.add('active');
                    const selected = btn.getAttribute('data-filter');
                    
                    const menuSection = document.getElementById('menu-gastronomia');
                    document.querySelectorAll('.searchable-item').forEach(item => {
                        if (menuSection && !menuSection.contains(item)) return;
                        
                        const catAttr = (item.getAttribute('data-food-category') || '').toLowerCase();
                        const cats = catAttr.split(',').map(c => c.trim());
                        let match = false;
                        if (selected === 'todos') match = true;
                        else if (selected === 'bebidas-cocteles') match = cats.includes('bebidas') || cats.includes('cocteles');
                        else if (selected === 'al-plato') match = cats.includes('al-plato');
                        else match = cats.includes(selected);
                        
                        item.style.display = match ? '' : 'none';
                    });
                });
            });
        }
    }
    
    // Filtros de Categoría (Index/Comercio)
    if (PAGE === 'index' || PAGE === 'comercio') {
        const indexCategoryFilter = document.getElementById('index-category-filter');
        if (indexCategoryFilter) {
            const filterButtons = indexCategoryFilter.querySelectorAll('.filter-btn');
            const toggleBtn = document.getElementById('index-category-toggle');
            const inlineContainer = toggleBtn ? toggleBtn.parentElement : null;

            if (toggleBtn && inlineContainer) {
                toggleBtn.addEventListener('click', () => {
                    const isOpen = inlineContainer.classList.toggle('open');
                    toggleBtn.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
                });
                document.addEventListener('click', (e) => {
                    if (!inlineContainer.contains(e.target)) {
                        inlineContainer.classList.remove('open');
                        toggleBtn.setAttribute('aria-expanded', 'false');
                    }
                });
            }

            filterButtons.forEach(btn => {
                btn.addEventListener('click', () => {
                    filterButtons.forEach(b => b.classList.remove('active'));
                    btn.classList.add('active');
                    const selected = btn.getAttribute('data-filter') || 'todos';

                    if (inlineContainer && toggleBtn) {
                        inlineContainer.classList.remove('open');
                        toggleBtn.setAttribute('aria-expanded', 'false');
                    }

                    const menuSection = document.getElementById('menu-electronica');
                    document.querySelectorAll('.searchable-item').forEach(item => {
                        if (menuSection && !menuSection.contains(item)) return;
                        
                        const catAttr = (item.getAttribute('data-product-category') || '').toLowerCase();
                        const cats = catAttr.split(',').map(c => c.trim());
                        const match = (selected === 'todos') ? true : cats.includes(selected);
                        
                        item.style.display = match ? '' : 'none';
                    });
                });
            });
        }
    }

    // Inicialización segura de visibilidad de productos
    function initProductVisibility() {
        if (PAGE === 'gastronomia') {
            const active = document.querySelector('#category-filter .filter-btn.active') || 
                           document.querySelector('#category-filter .filter-btn[data-filter="todos"]');
            if (active) active.click();
            else document.querySelectorAll('#menu-gastronomia .searchable-item').forEach(el => el.style.display = '');
        }
        else if (PAGE === 'index' || PAGE === 'comercio') {
            const active = document.querySelector('#index-category-filter .filter-btn.active') || 
                           document.querySelector('#index-category-filter .filter-btn[data-filter="todos"]');
            if (active) active.click();
            else document.querySelectorAll('#menu-electronica .searchable-item').forEach(el => el.style.display = '');
        }
    }
    // Ejecutar después de un breve delay para asegurar estabilidad del DOM
    setTimeout(initProductVisibility, 50);
});
