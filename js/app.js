/**
 * Main Application Entry Point
 */
// FORCE CONFIG REFRESH: Clear cached config to ensure fresh load from backend
try {
    const slug = window.BUSINESS_SLUG || 'gastronomia-local1';
    localStorage.removeItem('ordersConfig_' + slug); // Remove slug-specific config
    localStorage.removeItem('ordersConfig'); // Remove legacy config
    console.log('Config cache cleared for update.');
} catch (e) { console.error('Error clearing cache', e); }

import { loadBusinessConfig, PAGE, CHECKOUT_MODE, getBusinessSlug } from './config.js';
import { 
    initCartElements, 
    loadCart, 
    addToCart, 
    clearCart, 
    updateCartDisplay,
    updateCartCount
} from './cart.js';
import { 
    bindAddToCartEvents, 
    initDiscountSwipe, 
    openDialog, 
    closeDialog,
    closeCartUI
} from './ui.js';
import { 
    initSearch
} from './search.js';
import { handleCheckout } from './checkout.js';
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
} from './carousel.js';
import { 
    scrollDiscounts,
    initProductModals,
    initInterestFiltering,
    initDynamicProducts
} from './ui.js';
import { initOrderStatus } from './order-status.js';

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

function initHeaderContact() {
    const headerContact = document.querySelector('.header-contact');
    if (!headerContact) return;
    const slug = getBusinessSlug() || 'gastronomia-local1';
    const origin = window.location.origin || '';
    const base = /^file:/i.test(origin) ? 'http://127.0.0.1:8000' : origin;
    const url = `${base}/api/tenant_header?tenant_slug=${encodeURIComponent(slug)}`;
    fetch(url).then(res => {
        if (!res.ok) return null;
        return res.json();
    }).then(data => {
        if (!data) return;
        const whatsappValue = (data.whatsapp || '').trim();
        const instagramValue = (data.instagram || '').trim();
        const instagramLabel = (data.instagram_label || '').trim();
        const locationLabel = (data.location_label || data.location || '').trim();
        const locationUrl = (data.location_url || '').trim();
        const openingHours = data.opening_hours || null;
        const logoUrl = (data.logo_url || '').trim();
        const logoImg = document.querySelector('.site-logo img');
        if (logoImg) {
            logoImg.src = logoUrl || 'Imagenes/Epalogo.png';
        }
        if (whatsappValue) {
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
        if (instagramValue) {
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
        if (locationLabel || locationUrl) {
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
        if (openingHours && typeof openingHours === 'object') {
            let isOpenNow = false;
            let nextOpeningMinutes = null;
            try {
                const days = ['sun','mon','tue','wed','thu','fri','sat'];
                const now = new Date();
                const idx = now.getDay();
                const dayKey = days[idx];
                const prevKey = days[(idx + 6) % 7];
                const minutes = now.getHours() * 60 + now.getMinutes();
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
                                        return s;
                                    }
                                } else {
                                    if (minutes < s) {
                                        return s;
                                    }
                                }
                            } else {
                                return s;
                            }
                        }
                    }
                    return null;
                };
                const next = findNextOpening();
                if (next != null) {
                    nextOpeningMinutes = next;
                }
            } catch (e) {}
            const clockIcon = headerContact.querySelector('.fa-clock');
            const clockItem = clockIcon ? (clockIcon.closest('.contact-item-compact') || clockIcon.closest('div')) : null;
            if (clockItem) {
                const span = clockItem.querySelector('span');
                if (span) {
                    if (isOpenNow) {
                        span.textContent = 'Abierto';
                    } else if (nextOpeningMinutes != null) {
                        const h = Math.floor(nextOpeningMinutes / 60);
                        const m = nextOpeningMinutes % 60;
                        const hh = h.toString().padStart(2, '0');
                        const mm = m.toString().padStart(2, '0');
                        span.textContent = 'Abre a las ' + hh + ':' + mm;
                    } else {
                        span.textContent = 'Cerrado';
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
            }
        }

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
