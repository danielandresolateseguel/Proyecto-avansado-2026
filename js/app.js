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
import { handleCheckout } from './checkout.js?v=16';
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
    initDynamicProducts,
    openProductModalByProductId
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

let tenantHeaderData = null;
let entryPromotionResolved = false;
let entryPromotionOpen = false;

function formatPromotionMoney(amount) {
    const value = Number(amount || 0);
    const locale = String(window.CURRENCY_LOCALE || 'es-AR');
    const code = String(window.CURRENCY_CODE || 'ARS').toUpperCase();
    try {
        return new Intl.NumberFormat(locale, {
            style: 'currency',
            currency: code,
            maximumFractionDigits: 0
        }).format(value);
    } catch (_) {
        return `$${Math.round(value || 0)}`;
    }
}

function normalizePromotionsConfig(data) {
    const promotions = data && typeof data.promotions === 'object' ? data.promotions : {};
    const banner = promotions && typeof promotions.banner === 'object' ? promotions.banner : {};
    const entryModal = promotions && typeof promotions.entry_modal === 'object' ? promotions.entry_modal : {};
    const pricing = entryModal && typeof entryModal.pricing === 'object' ? entryModal.pricing : {};
    return {
        banner: {
            active: !!(banner.active || data && data.announcement_active),
            text: String(banner.text || data && data.announcement_text || '').trim()
        },
        entryModal: {
            active: !!entryModal.active,
            productId: String(entryModal.product_id || '').trim(),
            badgeText: String(entryModal.badge_text || '').trim(),
            headline: String(entryModal.headline || '').trim(),
            message: String(entryModal.message || '').trim(),
            ctaText: String(entryModal.cta_text || '').trim() || 'Ver promoción',
            pricing: {
                mode: String(pricing.mode || 'none').trim().toLowerCase() || 'none',
                compareAtPrice: Number.parseInt(pricing.compare_at_price, 10),
                note: String(pricing.note || '').trim(),
                promoPrice: Number.parseInt(pricing.promo_price, 10),
                discountPercent: Number.parseInt(pricing.discount_percent, 10),
                discountAmount: Number.parseInt(pricing.discount_amount, 10)
            },
            frequency: String(entryModal.frequency || 'session').trim().toLowerCase() || 'session',
            startsAt: String(entryModal.starts_at || '').trim(),
            endsAt: String(entryModal.ends_at || '').trim()
        }
    };
}

function getPromotionPriceDisplay(product, entryModal) {
    const basePrice = Number.parseInt(product && product.price, 10);
    if (!Number.isFinite(basePrice)) {
        return {
            currentText: '',
            compareText: '',
            noteText: String(entryModal && entryModal.pricing && entryModal.pricing.note || '').trim(),
            badgeText: String(entryModal && entryModal.badgeText || '').trim()
        };
    }
    const pricing = entryModal && typeof entryModal.pricing === 'object' ? entryModal.pricing : {};
    const mode = String(pricing.mode || 'none').trim().toLowerCase();
    const compareAtPrice = Number.isFinite(pricing.compareAtPrice) && pricing.compareAtPrice >= 0 ? pricing.compareAtPrice : basePrice;
    let promoPrice = null;
    let computedBadge = String(entryModal && entryModal.badgeText || '').trim();

    if (mode === 'promo_price' && Number.isFinite(pricing.promoPrice) && pricing.promoPrice >= 0) {
        promoPrice = pricing.promoPrice;
    } else if (mode === 'percent' && Number.isFinite(pricing.discountPercent) && pricing.discountPercent > 0) {
        promoPrice = Math.max(0, Math.round(compareAtPrice * (1 - (pricing.discountPercent / 100))));
        if (!computedBadge) computedBadge = `${pricing.discountPercent}% OFF`;
    } else if (mode === 'amount' && Number.isFinite(pricing.discountAmount) && pricing.discountAmount > 0) {
        promoPrice = Math.max(0, compareAtPrice - pricing.discountAmount);
        if (!computedBadge) computedBadge = `${formatPromotionMoney(pricing.discountAmount)} OFF`;
    }

    if (!Number.isFinite(promoPrice) || promoPrice === null || promoPrice >= compareAtPrice) {
        return {
            currentText: formatPromotionMoney(basePrice),
            compareText: '',
            noteText: String(pricing.note || '').trim(),
            badgeText: computedBadge
        };
    }
    return {
        currentText: formatPromotionMoney(promoPrice),
        compareText: formatPromotionMoney(compareAtPrice),
        noteText: String(pricing.note || '').trim(),
        badgeText: computedBadge
    };
}

function syncWindowPromotions(data) {
    try {
        window.__tenantPromotions = normalizePromotionsConfig(data || {});
        document.dispatchEvent(new CustomEvent('tenantPromotionsLoaded'));
    } catch (_) {}
}

function isPromotionInWindow(entryModal) {
    const now = Date.now();
    const startsAt = entryModal && entryModal.startsAt ? Date.parse(entryModal.startsAt) : NaN;
    const endsAt = entryModal && entryModal.endsAt ? Date.parse(entryModal.endsAt) : NaN;
    if (Number.isFinite(startsAt) && now < startsAt) return false;
    if (Number.isFinite(endsAt) && now > endsAt) return false;
    return true;
}

function getEntryPromotionKey(slug, entryModal) {
    const parts = [
        String(slug || '').trim(),
        String(entryModal && entryModal.productId || '').trim(),
        String(entryModal && entryModal.badgeText || '').trim(),
        String(entryModal && entryModal.headline || '').trim(),
        String(entryModal && entryModal.startsAt || '').trim(),
        String(entryModal && entryModal.endsAt || '').trim()
    ];
    return `entry_promo_${parts.join('_')}`;
}

function wasEntryPromotionDismissed(slug, entryModal) {
    const key = getEntryPromotionKey(slug, entryModal);
    const frequency = String(entryModal && entryModal.frequency || 'session');
    try {
        if (frequency === 'always') return false;
        if (frequency === 'day') {
            const last = localStorage.getItem(key);
            const today = new Date().toISOString().slice(0, 10);
            return last === today;
        }
        return sessionStorage.getItem(key) === '1';
    } catch (_) {
        return false;
    }
}

function rememberEntryPromotionDismissed(slug, entryModal) {
    const key = getEntryPromotionKey(slug, entryModal);
    const frequency = String(entryModal && entryModal.frequency || 'session');
    try {
        if (frequency === 'always') return;
        if (frequency === 'day') {
            localStorage.setItem(key, new Date().toISOString().slice(0, 10));
            return;
        }
        sessionStorage.setItem(key, '1');
    } catch (_) {}
}

function findCatalogProductById(productId) {
    const id = String(productId || '').trim();
    if (!id) return null;
    const catalog = Array.isArray(window.__tenantCatalogProducts) ? window.__tenantCatalogProducts : [];
    return catalog.find(product => String(product && product.id || '').trim() === id) || null;
}

function ensureEntryPromotionModal() {
    let modal = document.getElementById('entry-promotion-modal');
    if (modal) return modal;
    if (!document.getElementById('entry-promotion-modal-styles')) {
        const style = document.createElement('style');
        style.id = 'entry-promotion-modal-styles';
        style.textContent = `
          #entry-promotion-modal {
            align-items: center;
            justify-content: center;
            padding: clamp(14px, 2.2vw, 28px);
            background: rgba(15, 23, 42, 0.62);
            backdrop-filter: blur(6px);
            z-index: 3300;
          }
          #entry-promotion-modal .entry-promo-shell {
            width: min(880px, 92vw);
            max-width: 880px;
            max-height: min(86vh, 720px);
            overflow: hidden;
            border-radius: 28px;
            background:
              radial-gradient(circle at top left, var(--gastro-accent-18, rgba(255, 106, 0, 0.18)) 0%, rgba(255,255,255,0) 34%),
              linear-gradient(135deg, #ffffff 0%, #fffaf5 100%);
            box-shadow: 0 35px 80px rgba(15, 23, 42, 0.42);
            border: 1px solid rgba(255,255,255,0.7);
            position: relative;
          }
          #entry-promotion-modal .entry-promo-grid {
            display: grid;
            grid-template-columns: minmax(320px, 1.08fr) minmax(340px, 0.92fr);
            min-height: 460px;
          }
          #entry-promotion-modal .entry-promo-media {
            position: relative;
            min-height: 460px;
            overflow: hidden;
            background: linear-gradient(160deg, rgba(15,23,42,0.04) 0%, var(--gastro-accent-18, rgba(255,106,0,0.18)) 100%);
          }
          #entry-promotion-modal .entry-promo-media::after {
            content: "";
            position: absolute;
            inset: 0;
            background: linear-gradient(180deg, rgba(15,23,42,0.08) 0%, rgba(15,23,42,0.28) 100%);
            pointer-events: none;
          }
          #entry-promotion-modal #entry-promo-image {
            width: 100%;
            height: 100%;
            object-fit: cover;
            display: block;
          }
          #entry-promotion-modal .entry-promo-copy {
            display: flex;
            flex-direction: column;
            justify-content: center;
            gap: 14px;
            padding: 34px 32px;
          }
          #entry-promotion-modal .entry-promo-actions {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            margin-top: 4px;
          }
          #entry-promotion-modal .entry-promo-price-stack {
            display: flex;
            flex-direction: column;
            gap: 4px;
          }
          #entry-promotion-modal .entry-promo-compare {
            font-size: 15px;
            font-weight: 700;
            color: #94a3b8;
            text-decoration: line-through;
            min-height: 18px;
          }
          #entry-promotion-modal .entry-promo-current {
            font-size: 32px;
            line-height: 1;
            font-weight: 950;
            color: #111827;
          }
          #entry-promotion-modal .entry-promo-note {
            font-size: 13px;
            font-weight: 700;
            color: #7c3aed;
            min-height: 18px;
          }
          #entry-promotion-modal #entry-promo-cta,
          #entry-promotion-modal #entry-promo-dismiss {
            min-height: 50px;
          }
          @media (max-width: 768px) {
            #entry-promotion-modal {
              padding: 12px;
            }
            #entry-promotion-modal .entry-promo-shell {
              width: min(100%, 96vw);
              max-height: 92vh;
              border-radius: 24px;
            }
            #entry-promotion-modal .entry-promo-grid {
              grid-template-columns: 1fr;
              min-height: auto;
            }
            #entry-promotion-modal .entry-promo-media {
              min-height: 220px;
              max-height: 260px;
            }
            #entry-promotion-modal .entry-promo-copy {
              padding: 22px 18px 20px;
            }
            #entry-promotion-modal #entry-promo-title {
              font-size: 28px !important;
            }
            #entry-promotion-modal .entry-promo-actions {
              flex-direction: column;
            }
            #entry-promotion-modal #entry-promo-cta,
            #entry-promotion-modal #entry-promo-dismiss {
              width: 100%;
            }
          }
        `;
        document.head.appendChild(style);
    }
    modal = document.createElement('div');
    modal.id = 'entry-promotion-modal';
    modal.className = 'product-modal';
    modal.setAttribute('role', 'dialog');
    modal.setAttribute('aria-modal', 'true');
    modal.setAttribute('aria-hidden', 'true');
    modal.style.display = 'none';
    modal.style.alignItems = 'center';
    modal.style.justifyContent = 'center';
    modal.innerHTML = `
      <div class="modal-content entry-promo-shell" style="padding:0;">
        <button type="button" class="close-modal" aria-label="Cerrar promoción" title="Cerrar" style="z-index:2;"><i class="fas fa-times" aria-hidden="true"></i></button>
        <div class="entry-promo-grid">
          <div class="entry-promo-media">
            <img id="entry-promo-image" alt="Promoción destacada">
          </div>
          <div class="entry-promo-copy">
            <div id="entry-promo-badge" style="display:none; width:max-content; padding:7px 12px; border-radius:999px; background:rgba(239,68,68,0.12); color:#b91c1c; font-size:12px; font-weight:900; letter-spacing:0.04em; text-transform:uppercase;"></div>
            <div id="entry-promo-kicker" style="font-size:12px; font-weight:800; color:#64748b; letter-spacing:0.06em; text-transform:uppercase;">Promoción destacada</div>
            <h3 id="entry-promo-title" style="margin:0; font-size:32px; line-height:1.05; color:#0f172a;"></h3>
            <p id="entry-promo-message" style="margin:0; font-size:15px; line-height:1.55; color:#475569;"></p>
            <div class="entry-promo-price-stack">
              <div id="entry-promo-compare" class="entry-promo-compare"></div>
              <div id="entry-promo-price" class="entry-promo-current"></div>
              <div id="entry-promo-note" class="entry-promo-note"></div>
            </div>
            <div class="entry-promo-actions">
              <button type="button" id="entry-promo-cta" style="min-height:48px; padding:0 18px; border:none; border-radius:14px; background:var(--gastro-accent, #ff6a00); color:var(--gastro-accent-contrast, #fff); font-size:16px; font-weight:900; cursor:pointer;"></button>
              <button type="button" id="entry-promo-dismiss" style="min-height:48px; padding:0 18px; border:1px solid #cbd5e1; border-radius:14px; background:#fff; color:#334155; font-size:15px; font-weight:800; cursor:pointer;">Ahora no</button>
            </div>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(modal);
    const closeModal = () => {
        closeDialog(modal);
        entryPromotionOpen = false;
    };
    const closeBtn = modal.querySelector('.close-modal');
    const dismissBtn = modal.querySelector('#entry-promo-dismiss');
    if (closeBtn) closeBtn.addEventListener('click', closeModal);
    if (dismissBtn) dismissBtn.addEventListener('click', closeModal);
    modal.addEventListener('click', (event) => {
        if (event.target === modal) closeModal();
    });
    return modal;
}

function tryRenderEntryPromotion() {
    if (PAGE !== 'gastronomia') {
        entryPromotionResolved = true;
        return;
    }
    if (entryPromotionResolved || !tenantHeaderData) return;
    const slug = getBusinessSlug() || 'gastronomia-local1';
    const promotions = normalizePromotionsConfig(tenantHeaderData);
    const entryModal = promotions.entryModal;
    if (!entryModal.active || !entryModal.productId || !isPromotionInWindow(entryModal)) {
        entryPromotionResolved = true;
        return;
    }
    if (wasEntryPromotionDismissed(slug, entryModal)) {
        entryPromotionResolved = true;
        return;
    }
    const product = findCatalogProductById(entryModal.productId);
    if (!product) return;

    const modal = ensureEntryPromotionModal();
    const imageEl = modal.querySelector('#entry-promo-image');
    const badgeEl = modal.querySelector('#entry-promo-badge');
    const titleEl = modal.querySelector('#entry-promo-title');
    const messageEl = modal.querySelector('#entry-promo-message');
    const compareEl = modal.querySelector('#entry-promo-compare');
    const priceEl = modal.querySelector('#entry-promo-price');
    const noteEl = modal.querySelector('#entry-promo-note');
    const ctaBtn = modal.querySelector('#entry-promo-cta');
    const closeBtn = modal.querySelector('.close-modal');
    const dismissBtn = modal.querySelector('#entry-promo-dismiss');
    const priceDisplay = getPromotionPriceDisplay(product, entryModal);

    if (imageEl) imageEl.src = String(product.image_url || window.__tenantLogoUrl || 'Imagenes/Epalogo.png').trim();
    if (badgeEl) {
        badgeEl.textContent = priceDisplay.badgeText || 'Oferta especial';
        badgeEl.style.display = badgeEl.textContent ? 'inline-flex' : 'none';
    }
    if (titleEl) titleEl.textContent = entryModal.headline || String(product.name || 'Promoción destacada');
    if (messageEl) {
        messageEl.textContent = entryModal.message || String(product.details || 'Descubrí este producto destacado antes de seguir viendo la carta.');
    }
    if (compareEl) {
        compareEl.textContent = priceDisplay.compareText || '';
        compareEl.style.display = priceDisplay.compareText ? 'block' : 'none';
    }
    if (priceEl) priceEl.textContent = priceDisplay.currentText || formatPromotionMoney(product.price);
    if (noteEl) {
        noteEl.textContent = priceDisplay.noteText || '';
        noteEl.style.display = priceDisplay.noteText ? 'block' : 'none';
    }
    if (ctaBtn) {
        ctaBtn.textContent = entryModal.ctaText || 'Ver promoción';
        ctaBtn.onclick = () => {
            rememberEntryPromotionDismissed(slug, entryModal);
            closeDialog(modal);
            entryPromotionOpen = false;
            setTimeout(() => {
                openProductModalByProductId(entryModal.productId);
            }, 40);
        };
    }
    const onDismiss = () => rememberEntryPromotionDismissed(slug, entryModal);
    if (closeBtn) closeBtn.onclick = () => {
        onDismiss();
        closeDialog(modal);
        entryPromotionOpen = false;
    };
    if (dismissBtn) dismissBtn.onclick = () => {
        onDismiss();
        closeDialog(modal);
        entryPromotionOpen = false;
    };
    if (!entryPromotionOpen) {
        entryPromotionResolved = true;
        entryPromotionOpen = true;
        setTimeout(() => {
            if (!entryPromotionOpen) return;
            openDialog(modal);
        }, 220);
    }
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

function applyMainMenuViewMode(enabled) {
    const isCompact = enabled === true || String(enabled || '').trim().toLowerCase() === 'compact';
    if (!document.body) return;
    document.body.classList.toggle('main-menu-view-compact', isCompact);
    document.body.dataset.mainMenuView = isCompact ? 'compact' : 'default';
}

function normalizeConfiguredMainMenuCategories(rawCategories) {
    if (!Array.isArray(rawCategories) || !rawCategories.length) return [];
    const seen = new Set();
    const out = [];
    rawCategories.forEach((raw, index) => {
        const label = String(raw && (raw.label || raw.name || raw.title) || '').trim();
        const id = String(raw && (raw.id || raw.value || raw.slug) || '').trim().toLowerCase();
        if (!label || !id || id === 'todos' || seen.has(id)) return;
        seen.add(id);
        out.push({
            id,
            label,
            position: Math.max(1, parseInt(raw && raw.position || index + 1, 10) || (index + 1))
        });
    });
    out.sort((a, b) => a.position - b.position || a.label.localeCompare(b.label));
    return out;
}

function getFallbackMainMenuCategoriesFromDom() {
    const categoryFilter = document.getElementById('category-filter');
    if (!categoryFilter) return [];
    const items = [];
    const btns = categoryFilter.querySelectorAll('.filter-btn');
    btns.forEach((btn, index) => {
        const id = String(btn.getAttribute('data-filter') || '').trim().toLowerCase();
        const label = String(btn.textContent || '').trim();
        if (!id || id === 'todos' || !label) return;
        items.push({ id, label, position: index + 1 });
    });
    return items;
}

function getMainMenuCategoriesForRendering(configured) {
    const normalized = normalizeConfiguredMainMenuCategories(configured);
    return normalized.length ? normalized : getFallbackMainMenuCategoriesFromDom();
}

function applyGastronomiaCategoryFilter(selected) {
    const chosen = String(selected || 'todos').trim().toLowerCase() || 'todos';
    const menuSection = document.getElementById('menu-gastronomia');
    document.querySelectorAll('.searchable-item').forEach(item => {
        if (menuSection && !menuSection.contains(item)) return;
        const catAttr = (item.getAttribute('data-food-category') || '').toLowerCase();
        const cats = catAttr.split(',').map(c => c.trim()).filter(Boolean);
        let match = false;
        if (chosen === 'todos') match = true;
        else if (chosen === 'bebidas-cocteles') match = cats.includes('bebidas') || cats.includes('cocteles');
        else match = cats.includes(chosen);
        item.style.display = match ? '' : 'none';
    });
}

function scrollToGastronomiaFirstVisibleProduct() {
    const menuSection = document.getElementById('menu-gastronomia');
    if (!menuSection) return;

    const filter = document.getElementById('category-filter');
    const filterHeight = filter && filter.classList.contains('is-fixed') ? (filter.getBoundingClientRect().height || 0) : 0;
    const offset = filterHeight ? (Math.ceil(filterHeight) + 12) : 0;

    const items = Array.from(menuSection.querySelectorAll('.searchable-item'));
    const firstVisible = items.find(el => {
        try {
            if (!el || el.getClientRects().length === 0) return false;
            const st = window.getComputedStyle(el);
            return st.display !== 'none' && st.visibility !== 'hidden';
        } catch (_) {
            return false;
        }
    });

    const target = firstVisible || menuSection.querySelector('.products-grid') || menuSection;
    try {
        const y = target.getBoundingClientRect().top + window.scrollY - offset;
        window.scrollTo({ top: Math.max(0, Math.floor(y)), behavior: 'smooth' });
    } catch (_) {}
}

function bindGastronomiaCategoryFilters() {
    if (PAGE !== 'gastronomia') return;
    const categoryFilter = document.getElementById('category-filter');
    if (!categoryFilter) return;
    const btns = Array.from(categoryFilter.querySelectorAll('.filter-btn'));
    btns.forEach(btn => {
        btn.addEventListener('click', () => {
            btns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            try {
                btn.scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' });
            } catch (_) {}
            applyGastronomiaCategoryFilter(btn.getAttribute('data-filter'));
            requestAnimationFrame(() => {
                requestAnimationFrame(scrollToGastronomiaFirstVisibleProduct);
            });
        });
    });
    const hintSlug = (window.BUSINESS_SLUG || getBusinessSlug() || 'gastronomia').trim();
    initScrollableHint(categoryFilter, 'scrollHint_category_' + hintSlug);
}

function renderMainMenuCategoryFilters(configuredCategories) {
    if (PAGE !== 'gastronomia') return;
    const categoryFilter = document.getElementById('category-filter');
    if (!categoryFilter) return;
    const activeBtn = categoryFilter.querySelector('.filter-btn.active');
    const activeValue = String(activeBtn && activeBtn.getAttribute('data-filter') || 'todos').trim().toLowerCase() || 'todos';
    const categories = getMainMenuCategoriesForRendering(configuredCategories);
    const nextActive = categories.some(cat => cat.id === activeValue) ? activeValue : 'todos';
    let html = '<button class="filter-btn' + (nextActive === 'todos' ? ' active' : '') + '" data-filter="todos">Todos</button>';
    categories.forEach(cat => {
        const isActive = cat.id === nextActive ? ' active' : '';
        html += '<button class="filter-btn' + isActive + '" data-filter="' + cat.id + '">' + cat.label + '</button>';
    });
    categoryFilter.innerHTML = html;
    bindGastronomiaCategoryFilters();
    applyGastronomiaCategoryFilter(nextActive);
}

function initScrollableHint(container, storageKey) {
    if (!container) return;

    const updateState = () => {
        const maxScroll = Math.max(0, container.scrollWidth - container.clientWidth);
        const atStart = container.scrollLeft <= 1;
        const atEnd = container.scrollLeft >= (maxScroll - 1);
        container.classList.toggle('has-left', !atStart);
        container.classList.toggle('has-right', !atEnd);
        return { maxScroll, atStart, atEnd };
    };

    const state = updateState();
    container.addEventListener('scroll', updateState, { passive: true });

    let resizeTimer;
    window.addEventListener('resize', () => {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(updateState, 120);
    });

    if (!window.matchMedia('(max-width: 768px)').matches) return;
    if (!state.maxScroll) return;

    let shown = false;
    try {
        shown = sessionStorage.getItem(storageKey) === '1';
    } catch (_) {}
    if (shown) return;

    setTimeout(() => {
        const latest = updateState();
        if (!latest.maxScroll || !latest.atStart) return;
        container.scrollTo({ left: Math.min(28, latest.maxScroll), behavior: 'smooth' });
        setTimeout(() => {
            container.scrollTo({ left: 0, behavior: 'smooth' });
            updateState();
        }, 480);
        try {
            sessionStorage.setItem(storageKey, '1');
        } catch (_) {}
    }, 650);
}

function initGastronomiaStickyCategoryFilter() {
    const categoryFilter = document.getElementById('category-filter');
    if (!categoryFilter) return;
    if (categoryFilter.dataset.stickyInit === '1') return;
    categoryFilter.dataset.stickyInit = '1';

    const sentinel = document.createElement('div');
    sentinel.style.height = '1px';
    sentinel.style.margin = '0';
    sentinel.style.padding = '0';
    sentinel.style.pointerEvents = 'none';

    const placeholder = document.createElement('div');
    placeholder.style.display = 'none';
    placeholder.style.height = '0px';
    placeholder.style.pointerEvents = 'none';
    if (categoryFilter.parentNode) {
        categoryFilter.parentNode.insertBefore(placeholder, categoryFilter);
        categoryFilter.parentNode.insertBefore(sentinel, placeholder);
    }

    let rafId = 0;
    let isFixed = false;

    const update = () => {
        if (rafId) return;
        rafId = requestAnimationFrame(() => {
            rafId = 0;
            const sentinelTop = sentinel.getBoundingClientRect().top;
            const shouldFix = sentinelTop <= 0.5;
            const shouldUnfix = sentinelTop > 2;

            if (!isFixed && shouldFix) {
                isFixed = true;
                placeholder.style.display = 'block';
                placeholder.style.height = (categoryFilter.offsetHeight || categoryFilter.getBoundingClientRect().height || 0) + 'px';
                categoryFilter.classList.add('is-fixed');
                categoryFilter.classList.add('is-stuck');
            } else if (isFixed && shouldUnfix) {
                isFixed = false;
                placeholder.style.display = 'none';
                placeholder.style.height = '0px';
                categoryFilter.classList.remove('is-fixed');
                categoryFilter.classList.remove('is-stuck');
            }

            if (isFixed) {
                const h = categoryFilter.offsetHeight || 0;
                if (h > 0) placeholder.style.height = h + 'px';
            }
        });
    };

    window.addEventListener('scroll', update, { passive: true });
    window.addEventListener('resize', update);
    update();
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
        if (!data) {
            if (document.body) document.body.setAttribute('data-tenant-theme-loaded', 'true');
            return;
        }
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
        const footerContactTitle = (data.footer_contact_title || '').trim();
        const footerLocationTitle = (data.footer_location_title || '').trim();
        const footerBottom = (data.footer_bottom || '').trim();
        const contactEmail = (data.contact_email || '').trim();
        const timeZone = (data.timezone || '').trim();
        try {
            if (data.currency_code) window.CURRENCY_CODE = String(data.currency_code || '').toUpperCase();
            if (data.currency_locale) window.CURRENCY_LOCALE = String(data.currency_locale || '');
        } catch (_) {}
        const logoUrl = (data.logo_url || '').trim();
        const hasUploadedLogo = !!logoUrl;
        try {
            window.__tenantHasLogoUpload = hasUploadedLogo;
            window.__tenantLogoUrl = logoUrl;
        } catch (_) {}
        try {
            localStorage.setItem('cached_tenant_logo_uploaded_' + slug, hasUploadedLogo ? '1' : '0');
            if (hasUploadedLogo) localStorage.setItem('cached_logo_url_' + slug, logoUrl);
            else localStorage.removeItem('cached_logo_url_' + slug);
        } catch (_) {}
        
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
            const accentContrast = getContrastColor(themeColor);
            // Use document.body to override CSS definitions on body.sector-gastronomia
            document.body.style.setProperty('--gastro-accent', themeColor);
            document.body.style.setProperty('--gastro-accent-contrast', accentContrast);
            try {
                localStorage.setItem('cached_theme_color_' + slug, themeColor);
            } catch (e) {}
            
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
        if (document.body) document.body.setAttribute('data-tenant-theme-loaded', 'true');

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
        applyMainMenuViewMode(data.main_menu_compact_view);
        renderMainMenuCategoryFilters(data.main_menu_categories || []);
        try {
            window.BusinessConfig = Object.assign({}, window.BusinessConfig || {}, {
                main_menu_compact_view: !!data.main_menu_compact_view,
                main_menu_categories: Array.isArray(data.main_menu_categories) ? data.main_menu_categories : []
            });
        } catch (_) {}

        const logoImg = document.querySelector('.site-logo img');
        if (logoImg) {
            logoImg.src = logoUrl || 'Imagenes/Epalogo.png';
        }
        try {
            if (typeof window.applyProductImageFallbacks === 'function') window.applyProductImageFallbacks();
        } catch (_) {}

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
                const setFooterTextLine = (el, text) => {
                    if (!el) return;
                    const value = String(text || '').trim();
                    el.textContent = value;
                    el.style.display = value ? '' : 'none';
                };
                const setFooterIconLine = (el, iconClass, text) => {
                    if (!el) return;
                    const value = String(text || '').trim();
                    el.textContent = '';
                    if (!value) {
                        el.style.display = 'none';
                        return;
                    }
                    const i = document.createElement('i');
                    i.className = iconClass;
                    el.appendChild(i);
                    el.appendChild(document.createTextNode(` ${value}`));
                    el.style.display = '';
                };

                if (sections[0]) {
                    const h4 = sections[0].querySelector('h4');
                    const p = sections[0].querySelector('p');
                    if (h4 && (footerTitle || tenantName)) h4.textContent = `🍽️ ${footerTitle || tenantName}`;
                    setFooterTextLine(p, footerTagline);
                }
                if (sections[1]) {
                    const h4 = sections[1].querySelector('h4');
                    const ps = Array.from(sections[1].querySelectorAll('p'));
                    if (h4 && footerContactTitle) h4.textContent = `📞 ${footerContactTitle}`;
                    setFooterIconLine(ps[0], 'fab fa-whatsapp', whatsappValue ? `WhatsApp: ${whatsappValue}` : '');
                    setFooterIconLine(ps[1], 'fas fa-envelope', contactEmail);
                }
                if (sections[2]) {
                    const h4 = sections[2].querySelector('h4');
                    const ps = Array.from(sections[2].querySelectorAll('p'));
                    if (h4 && footerLocationTitle) h4.textContent = `📍 ${footerLocationTitle}`;
                    setFooterTextLine(ps[0], locationLabel);
                    setFooterTextLine(ps[1], openingHoursLabel);
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

        tenantHeaderData = data;
        const promotionConfig = normalizePromotionsConfig(data);
        syncWindowPromotions(data);
        const announcementActive = promotionConfig.banner.active;
        const announcementText = promotionConfig.banner.text;
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
        tryRenderEntryPromotion();
    }).catch(() => {
        // Fallback or error handling
    }).catch(() => {
        if (document.body) document.body.setAttribute('data-tenant-theme-loaded', 'true');
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
            tryRenderEntryPromotion();
        }, { once: true });
        setTimeout(markProductsReady, 2000);
    }

    // Inicializar configuración
    loadBusinessConfig(() => {
        // Callback tras cargar config (opcional)
    });
    document.addEventListener('businessconfig:ready', () => {
        try { updateCartDisplay(); } catch (_) {}
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
        renderMainMenuCategoryFilters((window.BusinessConfig && window.BusinessConfig.main_menu_categories) || []);
        initGastronomiaStickyCategoryFilter();
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

    initGastronomiaStickyCategoryFilter();

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
