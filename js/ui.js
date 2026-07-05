/**
 * UI Animations and Interactions
 */
import { addToCart, updateCartDisplay, updateCartCount } from './cart.js?v=8';
import { getBusinessSlug, formatMoneyWithCode } from './config.js?v=8';
import { refreshSearchableItems } from './search.js?v=8';

function normalizePackList(raw) {
    let arr = raw;
    if (typeof raw === 'string') {
        const s = raw.trim();
        if (!s) return [];
        try {
            arr = JSON.parse(s);
        } catch (_) {
            return [];
        }
    }
    if (!Array.isArray(arr)) return [];
    const out = [];
    const seen = new Set();
    arr.forEach((p, idx) => {
        if (!p || typeof p !== 'object') return;
        const id = String(p.id || p.key || p.value || idx).trim();
        const label = String(p.label || p.name || id).trim();
        const price = parseInt(p.price, 10);
        const size = parseInt(p.size ?? p.qty ?? p.multiplier ?? p.units ?? 1, 10);
        if (!id || !label) return;
        if (!Number.isFinite(price) || price <= 0) return;
        const packSize = Number.isFinite(size) && size > 0 ? size : 1;
        const key = `${id}::${price}::${packSize}`;
        if (seen.has(key)) return;
        seen.add(key);
        out.push({ id, label, price, pack_size: packSize });
    });
    return out;
}

function readPacksFromButton(button) {
    if (!button) return [];
    const raw = String(button.getAttribute('data-packs') || '').trim();
    if (!raw) return [];
    try {
        const direct = JSON.parse(raw);
        return normalizePackList(direct);
    } catch (_) {}
    try {
        const decoded = decodeURIComponent(raw);
        const parsed = JSON.parse(decoded);
        return normalizePackList(parsed);
    } catch (_) {}
    return [];
}

function normalizeMixBuilder(raw) {
    let value = raw;
    if (typeof raw === 'string') {
        const s = raw.trim();
        if (!s) return null;
        try {
            value = JSON.parse(s);
        } catch (_) {
            return null;
        }
    }
    if (!value || typeof value !== 'object') return null;
    const enabled = value.enabled !== false;
    if (!enabled) return null;
    const sourceCategory = String(value.source_category || '').trim();
    const parts = parseInt(value.parts, 10);
    const partFraction = parseFloat(value.part_fraction);
    return {
        enabled: true,
        source_category: sourceCategory,
        parts: Number.isFinite(parts) && parts > 0 ? parts : 2,
        part_fraction: Number.isFinite(partFraction) && partFraction > 0 ? partFraction : 0.5,
        only_mixable: value.only_mixable !== false,
        pricing_mode: String(value.pricing_mode || 'sum_parts').trim() || 'sum_parts'
    };
}

function readMixBuilderFromButton(button) {
    if (!button) return null;
    const raw = String(button.getAttribute('data-mix-builder') || '').trim();
    if (!raw) return null;
    try {
        return normalizeMixBuilder(JSON.parse(raw));
    } catch (_) {}
    try {
        return normalizeMixBuilder(JSON.parse(decodeURIComponent(raw)));
    } catch (_) {}
    return null;
}

function normalizeCategoryToken(value) {
    const text = String(value || '').trim().toLowerCase();
    if (!text) return '';
    return text.normalize ? text.normalize('NFD').replace(/[\u0300-\u036f]/g, '') : text;
}

function getCatalogProducts() {
    return Array.isArray(window.__tenantCatalogProducts) ? window.__tenantCatalogProducts : [];
}

function productMatchesMixSource(product, sourceCategory) {
    if (!sourceCategory) return true;
    const variants = product && product._variants && typeof product._variants === 'object' ? product._variants : {};
    const rawCats = variants.food_categories;
    let cats = [];
    if (Array.isArray(rawCats)) {
        cats = rawCats;
    } else if (typeof rawCats === 'string' && rawCats.trim()) {
        cats = rawCats.split(',').map(cat => cat.trim());
    }
    const wanted = normalizeCategoryToken(sourceCategory);
    return cats.some(cat => normalizeCategoryToken(cat) === wanted);
}

function getMixCandidateProducts(baseProductId, mixBuilder) {
    return getCatalogProducts().filter(product => {
        if (!product || !product.id) return false;
        if (String(product.id) === String(baseProductId)) return false;
        if (product.active === false) return false;
        const variants = product._variants && typeof product._variants === 'object' ? product._variants : {};
        if (variants.mix_builder && variants.mix_builder.enabled !== false) return false;
        if (!productMatchesMixSource(product, mixBuilder && mixBuilder.source_category)) return false;
        if (mixBuilder && mixBuilder.only_mixable && variants.mixable !== true) return false;
        const price = parseInt(product.price, 10);
        return Number.isFinite(price) && price > 0;
    }).sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), 'es'));
}

function buildMixSummary(components) {
    if (!Array.isArray(components) || !components.length) return '';
    return components.map(part => `1/2 ${part.name || 'Pizza'}`).join(' + ');
}

function ensureMixModal() {
    let modal = document.getElementById('mix-modal');
    if (modal) return modal;
    modal = document.createElement('div');
    modal.id = 'mix-modal';
    modal.className = 'product-modal';
    modal.setAttribute('role', 'dialog');
    modal.setAttribute('aria-modal', 'true');
    modal.setAttribute('aria-hidden', 'true');
    modal.style.display = 'none';
    modal.innerHTML = `
      <div class="modal-content mix-modal-shell">
        <button class="close-modal" aria-label="Cerrar modal" title="Cerrar"><i class="fas fa-times" aria-hidden="true"></i></button>
        <div class="modal-body">
          <div class="modal-details mix-modal-details">
            <div class="mix-modal-hero">
              <div class="mix-modal-kicker">Pizza Mixta</div>
              <h3 id="mix-modal-title" class="mix-modal-title"></h3>
              <p class="mix-modal-description">Elegí las dos mitades para calcular el precio final y agregar una sola pizza al carrito.</p>
            </div>
            <div class="mix-modal-body">
              <div class="mix-modal-grid">
                <label class="mix-modal-field">
                  <span class="mix-modal-field-label">Primera mitad</span>
                  <select id="mix-modal-first" class="mix-modal-select"></select>
                </label>
                <label class="mix-modal-field">
                  <span class="mix-modal-field-label">Segunda mitad</span>
                  <select id="mix-modal-second" class="mix-modal-select"></select>
                </label>
              </div>
              <div class="mix-modal-grid mix-modal-summary-grid">
                <div id="mix-modal-summary" class="mix-modal-summary"></div>
                <div id="mix-modal-price" class="mix-modal-price"></div>
              </div>
              <button type="button" id="mix-modal-confirm" class="modal-add-to-cart mix-modal-confirm">Agregar Pizza Mixta</button>
            </div>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(modal);
    const closeBtn = modal.querySelector('.close-modal');
    if (closeBtn) closeBtn.addEventListener('click', () => closeDialog(modal));
    modal.addEventListener('click', (e) => {
        if (e.target === modal) closeDialog(modal);
    });
    return modal;
}

function ensurePackModal() {
    let modal = document.getElementById('pack-modal');
    if (modal) return modal;
    modal = document.createElement('div');
    modal.id = 'pack-modal';
    modal.className = 'product-modal';
    modal.setAttribute('role', 'dialog');
    modal.setAttribute('aria-modal', 'true');
    modal.setAttribute('aria-hidden', 'true');
    modal.style.display = 'none';
    modal.innerHTML = `
      <div class="modal-content">
        <button class="close-modal" aria-label="Cerrar modal" title="Cerrar"><i class="fas fa-times" aria-hidden="true"></i></button>
        <div class="modal-body">
          <div class="modal-details" style="width:100%;">
            <h3 id="pack-modal-title"></h3>
            <p style="margin-top:6px; margin-bottom:10px; opacity:0.9;">Elegí la presentación</p>
            <div id="pack-modal-options" style="display:flex; flex-direction:column; gap:10px;"></div>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(modal);
    const closeBtn = modal.querySelector('.close-modal');
    if (closeBtn) closeBtn.addEventListener('click', () => closeDialog(modal));
    modal.addEventListener('click', (e) => {
        if (e.target === modal) closeDialog(modal);
    });
    return modal;
}

function parseStockValue(value) {
    const n = parseInt(value, 10);
    return Number.isFinite(n) ? n : null;
}

function getStockBadgeMeta(stock) {
    if (!Number.isFinite(stock)) return null;
    if (stock <= 0) return { text: 'Sin stock', className: 'stock-badge out' };
    if (stock <= 5) return { text: 'Ultimas unidades', className: 'stock-badge low' };
    return null;
}

function upsertStockBadge(card, stock) {
    if (!card) return;
    const info = card.querySelector('.product-info');
    if (!info) return;
    let badge = info.querySelector('.stock-badge');
    const meta = getStockBadgeMeta(stock);
    if (!meta) {
        if (badge) badge.remove();
        return;
    }
    if (!badge) {
        badge = document.createElement('span');
        const priceContainer = info.querySelector('.price-container');
        if (priceContainer && priceContainer.parentNode === info) {
            info.insertBefore(badge, priceContainer);
        } else {
            info.appendChild(badge);
        }
    }
    badge.className = meta.className;
    badge.textContent = meta.text;
}

function applyStockStateToButton(button, stock) {
    if (!button) return;
    const isOut = Number.isFinite(stock) && stock <= 0;
    button.toggleAttribute('disabled', isOut);
    if (isOut) {
        button.setAttribute('aria-disabled', 'true');
        button.setAttribute('title', 'Producto sin stock');
    } else {
        button.removeAttribute('aria-disabled');
        button.removeAttribute('title');
    }
}

function applyCardStockState(card, product) {
    if (!card || !product) return;
    const stock = parseStockValue(product.stock);
    card.dataset.stock = Number.isFinite(stock) ? String(stock) : '';
    const button = card.querySelector('.add-to-cart-btn');
    upsertStockBadge(card, stock);
    applyStockStateToButton(button, stock);
}

// Animación de añadir al carrito
export function showAddToCartAnimation(event) {
    const animationElement = document.createElement('div');
    animationElement.className = 'add-to-cart-animation';
    
    let clientX, clientY;
    if (event.touches && event.touches.length > 0) {
        clientX = event.touches[0].clientX;
        clientY = event.touches[0].clientY;
    } else if (event.changedTouches && event.changedTouches.length > 0) {
        clientX = event.changedTouches[0].clientX;
        clientY = event.changedTouches[0].clientY;
    } else {
        clientX = event.clientX || event.target.getBoundingClientRect().left + event.target.offsetWidth / 2;
        clientY = event.clientY || event.target.getBoundingClientRect().top + event.target.offsetHeight / 2;
    }
    
    animationElement.style.left = clientX + 'px';
    animationElement.style.top = clientY + 'px';
    document.body.appendChild(animationElement);
    
    const cartIcon = document.getElementById('floating-cart') || document.querySelector('.cart-icon');
    const cartIconRect = cartIcon ? cartIcon.getBoundingClientRect() : null;
    const cartIconX = cartIconRect ? (cartIconRect.left + cartIconRect.width / 2) : clientX;
    const cartIconY = cartIconRect ? (cartIconRect.top + cartIconRect.height / 2) : clientY;
    
    requestAnimationFrame(() => {
        animationElement.style.transition = 'all 0.6s cubic-bezier(0.25, 0.46, 0.45, 0.94)';
        animationElement.style.left = cartIconX + 'px';
        animationElement.style.top = cartIconY + 'px';
        animationElement.style.opacity = '0';
        animationElement.style.transform = 'scale(0.1)';
    });
    
    setTimeout(() => {
        if (animationElement.parentNode) document.body.removeChild(animationElement);
    }, 600);
}

// Indicador visual en botón
export function showAddedToCartIndicator(button) {
    const originalText = button.textContent;
    button.textContent = '¡Añadido!';
    button.classList.add('added-to-cart');
    
    setTimeout(() => {
        button.textContent = originalText;
        button.classList.remove('added-to-cart');
    }, 1500);
}

// Resaltar elemento
export function highlightElement(element) {
    element.classList.add('highlight-element');
    element.scrollIntoView({ behavior: 'smooth', block: 'start' });
    const categoryFilter = document.getElementById('category-filter');
    if (categoryFilter && categoryFilter.classList.contains('is-stuck')) {
        const h = categoryFilter.getBoundingClientRect().height || 0;
        if (h > 0) {
            setTimeout(() => {
                window.scrollBy({ top: -(Math.ceil(h) + 8), behavior: 'smooth' });
            }, 250);
        }
    }
    setTimeout(() => {
        element.classList.remove('highlight-element');
    }, 2000);
}

// Handler de click "Añadir al carrito"
export function onAddToCartClick(event) {
    const button = event.currentTarget;
    if (button.disabled || button.getAttribute('aria-disabled') === 'true') {
        event.preventDefault();
        return;
    }
    const productCard = button.closest('.product-card');
    const productImage = productCard ? productCard.querySelector('.product-image img') : null;
    const titleEl = productCard ? productCard.querySelector('h3') : null;
    const priceEl = productCard ? productCard.querySelector('.product-price') : null;

    const productId = button.getAttribute('data-id') || (productCard ? productCard.id : '') || `auto-${Date.now()}`;
    let name = button.getAttribute('data-name') || (titleEl ? titleEl.textContent.trim() : '') || (productImage ? (productImage.alt || '').trim() : '') || 'Producto';
    const imageSrc = productImage ? productImage.getAttribute('src') : '';
    const packs = readPacksFromButton(button);
    const mixBuilder = readMixBuilderFromButton(button);

    if (mixBuilder) {
        const candidates = getMixCandidateProducts(productId, mixBuilder);
        if (candidates.length === 0) {
            alert('No hay pizzas disponibles para combinar en este momento.');
            return;
        }
        const modal = ensureMixModal();
        const title = modal.querySelector('#mix-modal-title');
        const firstSelect = modal.querySelector('#mix-modal-first');
        const secondSelect = modal.querySelector('#mix-modal-second');
        const summaryEl = modal.querySelector('#mix-modal-summary');
        const priceEl = modal.querySelector('#mix-modal-price');
        const confirmBtn = modal.querySelector('#mix-modal-confirm');
        if (title) title.textContent = name;
        const buildOption = (product) => {
            const opt = document.createElement('option');
            opt.value = String(product.id || '');
            opt.textContent = `${product.name || 'Pizza'} - ${formatMoneyWithCode(parseInt(product.price, 10) || 0)}`;
            return opt;
        };
        const fillSelect = (selectEl) => {
            if (!selectEl) return;
            selectEl.innerHTML = '';
            candidates.forEach(product => selectEl.appendChild(buildOption(product)));
        };
        fillSelect(firstSelect);
        fillSelect(secondSelect);
        if (firstSelect && candidates[0]) firstSelect.value = String(candidates[0].id);
        if (secondSelect && candidates[1]) {
            secondSelect.value = String(candidates[1].id);
        } else if (secondSelect && candidates[0]) {
            secondSelect.value = String(candidates[0].id);
        }

        const updateMixState = () => {
            const first = candidates.find(product => String(product.id) === String(firstSelect && firstSelect.value || ''));
            const second = candidates.find(product => String(product.id) === String(secondSelect && secondSelect.value || ''));
            if (!first || !second) {
                if (summaryEl) summaryEl.textContent = 'Seleccioná las dos mitades.';
                if (priceEl) priceEl.textContent = '';
                if (confirmBtn) confirmBtn.disabled = true;
                return null;
            }
            const fraction = Number.isFinite(mixBuilder.part_fraction) && mixBuilder.part_fraction > 0 ? mixBuilder.part_fraction : 0.5;
            const components = [first, second].map(product => {
                const basePrice = parseInt(product.price, 10) || 0;
                return {
                    product_id: String(product.id || ''),
                    name: String(product.name || 'Pizza'),
                    fraction,
                    base_price: basePrice,
                    applied_price: Math.round(basePrice * fraction)
                };
            });
            const summary = buildMixSummary(components);
            const totalPrice = components.reduce((sum, part) => sum + (parseInt(part.applied_price, 10) || 0), 0);
            if (summaryEl) {
                summaryEl.innerHTML = `
                  <div class="mix-modal-eyebrow">Combinacion</div>
                  <div class="mix-modal-summary-text">${summary}</div>
                  <div class="mix-modal-chip-list">
                    ${components.map(part => `<span class="mix-modal-chip">1/2 ${part.name} · ${formatMoneyWithCode(part.applied_price)}</span>`).join('')}
                  </div>
                `;
            }
            if (priceEl) priceEl.innerHTML = `<div><div class="mix-modal-eyebrow">Total</div><div class="mix-modal-total">${formatMoneyWithCode(totalPrice)}</div></div>`;
            if (confirmBtn) confirmBtn.disabled = totalPrice <= 0;
            return { components, summary, totalPrice };
        };

        if (firstSelect) firstSelect.onchange = updateMixState;
        if (secondSelect) secondSelect.onchange = updateMixState;
        if (confirmBtn) {
            confirmBtn.onclick = () => {
                const state = updateMixState();
                if (!state || !state.components.length || state.totalPrice <= 0) return;
                const signature = state.components
                    .map(part => String(part.product_id || '').trim())
                    .filter(Boolean)
                    .sort()
                    .join('+');
                const clientId = `${productId}::mix:${signature}`;
                const meta = {
                    product_id: productId,
                    modifiers: {
                        mix: state.components
                    },
                    mix_summary: state.summary
                };
                closeDialog(modal);
                addToCart(clientId, name, state.totalPrice, imageSrc, event, (evt) => {
                    showAddToCartAnimation(evt);
                    showAddedToCartIndicator(button);
                }, '', meta);
            };
        }
        updateMixState();
        openDialog(modal);
        return;
    }

    if (packs.length) {
        const modal = ensurePackModal();
        const title = modal.querySelector('#pack-modal-title');
        const options = modal.querySelector('#pack-modal-options');
        if (title) title.textContent = name;
        if (options) options.innerHTML = '';

        const addFromPack = (pack) => {
            const clientId = `${productId}::pack:${pack.id}`;
            const displayName = `${name} (${pack.label})`;
            const meta = {
                product_id: productId,
                pack_id: pack.id,
                pack_label: pack.label,
                pack_size: pack.pack_size
            };
            addToCart(clientId, displayName, pack.price, imageSrc, event, (evt) => {
                showAddToCartAnimation(evt);
                showAddedToCartIndicator(button);
            }, '', meta);
        };

        packs.forEach(pack => {
            const optBtn = document.createElement('button');
            optBtn.type = 'button';
            optBtn.className = 'modal-add-to-cart';
            optBtn.style.display = 'flex';
            optBtn.style.alignItems = 'center';
            optBtn.style.justifyContent = 'space-between';
            optBtn.style.gap = '12px';
            optBtn.innerHTML = `<span style="font-weight:800;">${pack.label}</span><span>${formatMoneyWithCode(pack.price)}</span>`;
            optBtn.addEventListener('click', () => {
                closeDialog(modal);
                addFromPack(pack);
            });
            if (options) options.appendChild(optBtn);
        });

        openDialog(modal);
        return;
    }

    const attrPrice = button.getAttribute('data-price');
    let price = parseFloat(attrPrice);

    if (!isFinite(price) || price <= 0) {
        const priceText = priceEl ? priceEl.textContent : '';
        const match = priceText && priceText.match(/\d+[\.,]?\d*/);
        price = match ? parseFloat(match[0].replace('.', '').replace(',', '.')) : NaN;
    }

    if (!isFinite(price) || price <= 0) {
        console.warn('Precio inválido', { id: productId, name });
        return;
    }
    
    let notes = '';
    if (button.id === 'modal-add-to-cart-btn') {
        const notesEl = document.getElementById('modal-product-notes');
        if (notesEl) notes = notesEl.value;
    }

    addToCart(productId, name, price, imageSrc, event, (evt) => {
        showAddToCartAnimation(evt);
        showAddedToCartIndicator(button);
    }, notes, null);
}

// Enlazar eventos
export function bindAddToCartEvents(scope = document) {
    const buttons = scope.querySelectorAll('.add-to-cart-btn:not(#modal-add-to-cart-btn)');
    buttons.forEach(btn => {
        if (btn.dataset.bound === 'true') return;
        btn.addEventListener('click', onAddToCartClick);
        btn.dataset.bound = 'true';
    });
}

// Swipe de descuentos
export function initDiscountSwipe() {
    const discountsContainer = document.querySelector('.discounts-container');
    if (!discountsContainer) return;

    let isDown = false;
    let startX, scrollLeft, startTime;
    let velocity = 0;

    const start = (x) => {
        isDown = true;
        startX = x - discountsContainer.offsetLeft;
        scrollLeft = discountsContainer.scrollLeft;
        startTime = Date.now();
    };

    const end = () => {
        isDown = false;
        if (Math.abs(velocity) > 0.5) {
            const momentum = velocity * 100;
            discountsContainer.scrollTo({
                left: discountsContainer.scrollLeft - momentum,
                behavior: 'smooth'
            });
        }
    };

    const move = (x, prevent) => {
        if (!isDown) return;
        if (prevent) prevent();
        const walk = (x - startX) * 2;
        discountsContainer.scrollLeft = scrollLeft - walk;
        velocity = walk / (Date.now() - startTime);
    };

    discountsContainer.addEventListener('mousedown', e => {
        if (window.innerWidth > 768) return;
        start(e.pageX);
        discountsContainer.style.cursor = 'grabbing';
    });
    discountsContainer.addEventListener('mouseleave', () => { isDown = false; discountsContainer.style.cursor = 'grab'; });
    discountsContainer.addEventListener('mouseup', () => { end(); discountsContainer.style.cursor = 'grab'; });
    discountsContainer.addEventListener('mousemove', e => move(e.pageX, () => e.preventDefault()));

    discountsContainer.addEventListener('touchstart', e => start(e.touches[0].pageX));
    discountsContainer.addEventListener('touchend', end);
    discountsContainer.addEventListener('touchmove', e => move(e.touches[0].pageX));
}

// Funciones de navegación de descuentos (Migradas)
export function scrollDiscounts(direction) {
    const container = document.querySelector('.discounts-container');
    if (!container) return;
    const scrollAmount = 300;
    
    if (direction === 'left') {
        container.scrollBy({ left: -scrollAmount, behavior: 'smooth' });
    } else if (direction === 'right') {
        container.scrollBy({ left: scrollAmount, behavior: 'smooth' });
    }
    
    setTimeout(updateDiscountNavButtons, 300);
}

export function updateDiscountNavButtons() {
    const container = document.querySelector('.discounts-container');
    const prevBtn = document.querySelector('.discounts-nav-btn.prev');
    const nextBtn = document.querySelector('.discounts-nav-btn.next');
    
    if (!container || !prevBtn || !nextBtn) return;
    
    // Check if scrollable
    const maxScroll = container.scrollWidth - container.clientWidth;
    if (maxScroll <= 0) {
        prevBtn.style.display = 'none';
        nextBtn.style.display = 'none';
        return;
    } else {
        prevBtn.style.display = '';
        nextBtn.style.display = '';
    }

    const isAtStart = container.scrollLeft <= 5; // Tolerance
    const isAtEnd = container.scrollLeft >= (maxScroll - 5);
    
    prevBtn.disabled = isAtStart;
    nextBtn.disabled = isAtEnd;
    
    prevBtn.style.opacity = isAtStart ? '0.5' : '1';
    nextBtn.style.opacity = isAtEnd ? '0.5' : '1';
}

// Auto-scroll logic
let discountAutoScrollInterval;
let isDiscountAutoScrollPaused = false;

export function initDiscountAutoScroll() {
    const container = document.querySelector('.discounts-container');
    const discountsWrapper = document.querySelector('.discounts-wrapper');
    
    if (!container || !discountsWrapper) return;
    
    const prefersReducedMotion = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    if (prefersReducedMotion) {
        isDiscountAutoScrollPaused = true;
        return;
    }
}

// Dialog helpers
export function openDialog(dialog) {
    dialog.setAttribute('aria-hidden', 'false');
    dialog.style.display = 'flex';
}

export function closeDialog(dialog) {
    dialog.setAttribute('aria-hidden', 'true');
    dialog.style.display = 'none';
}

// Product Modals
export function initProductModals() {
    const modal = document.getElementById('product-modal');
    if (!modal) return;

    const modalImg = document.getElementById('modal-product-image');
    const modalTitle = document.getElementById('modal-product-title');
    const modalDesc = document.getElementById('modal-product-description');
    const modalPrice = document.getElementById('modal-product-price');
    const modalAddBtn = document.getElementById('modal-add-to-cart-btn');
    const closeModalBtn = modal.querySelector('.close-modal');

    if (closeModalBtn) {
        closeModalBtn.addEventListener('click', () => {
            closeDialog(modal);
        });
    }

    // Bind add to cart event once
    if (modalAddBtn && modalAddBtn.dataset.bound !== 'true') {
        modalAddBtn.addEventListener('click', (e) => {
            onAddToCartClick(e);
            closeDialog(modal);
        });
        modalAddBtn.dataset.bound = 'true';
    }
    
    // Close on click outside
    if (modal.dataset.bound !== 'true') {
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                closeDialog(modal);
            }
        });
        modal.dataset.bound = 'true';
    }

    // Event Delegation for Product Cards (Handles static and dynamic content)
    if (document.body.dataset.modalsInitialized !== 'true') {
        document.body.addEventListener('click', (e) => {
            const card = e.target.closest('.product-card');
            if (!card) return;
            
            // Ignore if clicking add-to-cart button or its children
            if (e.target.closest('.add-to-cart-btn')) {
                return;
            }
            
            // Optional: Ignore if selecting text?
            if (window.getSelection().toString().length > 0) return;

            e.preventDefault();
            // e.stopPropagation(); // Optional, but safer to let it bubble if needed, but here we want to capture
            openProductModal(card);
        });
        document.body.dataset.modalsInitialized = 'true';
    }

    // Legacy manual binding removed in favor of delegation
    // This ensures both static "Platos Destacados" and dynamic products work immediately

    function openProductModal(card) {
        const img = card.querySelector('img');
        const title = card.querySelector('h3');
        const desc = card.querySelector('.product-description');
        const price = card.querySelector('.product-price');
        const addBtn = card.querySelector('.add-to-cart-btn');

        if (modalImg && img) modalImg.src = img.src;
        if (modalTitle && title) modalTitle.textContent = title.textContent;
        if (modalDesc && desc) modalDesc.textContent = desc.textContent;
        if (modalPrice && price) modalPrice.textContent = price.textContent;
        
        if (modalAddBtn && addBtn) {
            // Copy data attributes
            modalAddBtn.setAttribute('data-id', addBtn.getAttribute('data-id'));
            modalAddBtn.setAttribute('data-name', addBtn.getAttribute('data-name'));
            modalAddBtn.setAttribute('data-price', addBtn.getAttribute('data-price'));
            const packsAttr = addBtn.getAttribute('data-packs');
            if (packsAttr) modalAddBtn.setAttribute('data-packs', packsAttr);
            else modalAddBtn.removeAttribute('data-packs');
            const stock = parseStockValue(card && card.dataset ? card.dataset.stock : '');
            applyStockStateToButton(modalAddBtn, stock);
            
            // Reset button state
            modalAddBtn.textContent = (Number.isFinite(stock) && stock <= 0) ? 'Sin stock' : 'Añadir al carrito';
            modalAddBtn.classList.remove('added-to-cart');
        }

        const notesInput = document.getElementById('modal-product-notes');
        if (notesInput) {
            notesInput.value = '';
        }

        openDialog(modal);
    }
}

// Interest Filtering
export function initInterestFiltering() {
    const interestSection = document.getElementById('interest-index');
    if (!interestSection) return;
    
    const buttons = interestSection.querySelectorAll('.interest-item');
    const productSection = document.querySelector('.interest-products');
    
    if (!productSection) return;

    buttons.forEach(btn => {
        btn.addEventListener('click', () => {
            // Active state
            buttons.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            
            const term = btn.getAttribute('data-term');
            
            // Scroll to products with offset for header
            const headerOffset = 120; 
            const elementPosition = productSection.getBoundingClientRect().top;
            const offsetPosition = elementPosition + window.pageYOffset - headerOffset;
            
            window.scrollTo({
                top: offsetPosition,
                behavior: "smooth"
            });
            
            // Filter products
            const products = productSection.querySelectorAll('.product-card');
            let hasVisible = false;
            
            products.forEach(card => {
                const categories = card.getAttribute('data-interest-category') || '';
                const cats = categories.split(',').map(c => c.trim());
                
                if (cats.includes(term)) {
                    card.style.display = '';
                    card.style.animation = 'none';
                    card.offsetHeight; /* trigger reflow */
                    card.style.animation = 'fadeIn 0.5s';
                    hasVisible = true;
                } else {
                    card.style.display = 'none';
                }
            });
        });
    });
}

export function closeCartUI() {
    const shoppingCart = document.getElementById('shopping-cart');
    const overlay = document.querySelector('.overlay');
    if (shoppingCart) {
        shoppingCart.classList.remove('active');
        closeDialog(shoppingCart);
    }
    if (overlay) overlay.classList.remove('active');
    updateCartDisplay();
    updateCartCount();
}

export async function initDynamicProducts() {
    const slug = (getBusinessSlug() || '').trim();
    if (!slug) return;
    try {
        const origin = window.location.origin || '';
        const base = /^file:/i.test(origin) ? 'http://127.0.0.1:8000' : origin;
        const url = new URL('/api/products', base);
        url.searchParams.set('tenant_slug', slug);
        url.searchParams.set('include_inactive', 'true');
        const resp = await fetch(url.toString(), { credentials: 'include' });
        if (!resp.ok) return;
        const json = await resp.json();
        const arr = Array.isArray(json.products) ? json.products : [];
        if (!arr.length) {
        // Si el tenant no tiene productos, vaciar las grillas para que la carta quede limpia
        try {
            const featuredGrid = document.querySelector('#featured-dishes .discounts-grid') || document.querySelector('.special-discounts .discounts-grid');
            const mainGrid = document.querySelector('#menu-gastronomia .products-grid') || document.querySelector('#menu-electronica .products-grid');
            const interestGrid = document.querySelector('.interest-products .products-grid');
            if (featuredGrid) featuredGrid.innerHTML = '';
            if (mainGrid) mainGrid.innerHTML = '';
            if (interestGrid) interestGrid.innerHTML = '';
        } catch (_) {}
        return;
        }
        const map = {};
        arr.forEach(p => {
            if (!p || !p.id) return;
            if (p.variants) {
                try {
                    const raw = typeof p.variants === "string" ? p.variants : JSON.stringify(p.variants);
                    p._variants = JSON.parse(raw || "{}") || {};
                } catch (_) {
                    p._variants = {};
                }
            } else {
                p._variants = {};
            }
            map[p.id] = p;
        });
        try {
            window.__tenantCatalogProducts = arr;
        } catch (_) {}
        const cards = document.querySelectorAll('.product-card');
        const existingIds = new Set();
        cards.forEach(card => {
            let id = card.getAttribute('id') || '';
            const btn = card.querySelector('.add-to-cart-btn');
            if (id) existingIds.add(id);
            if (btn) {
                const bid = btn.getAttribute('data-id') || '';
                if (bid) existingIds.add(bid);
            }
            let prod = map[id];
            if (!prod && btn) {
                const bid = btn.getAttribute('data-id') || '';
                prod = map[bid];
            }
            // Si no existe en el inventario del tenant actual, oculta la tarjeta estática
            if (!prod) {
                card.style.display = 'none';
                return;
            }
            if (prod.active === false) {
                card.style.display = 'none';
                return;
            }
            const v = prod._variants || {};
            const h = card.querySelector('.product-info h3');
            if (h && typeof prod.name === 'string') h.textContent = String(prod.name || '');
            const desc = card.querySelector('.product-description');
            if (desc && typeof prod.details === 'string') desc.textContent = prod.details;
            const priceEl = card.querySelector('.product-price');
            const priceVal = isFinite(parseInt(prod.price)) ? parseInt(prod.price) : 0;
            if (priceEl && priceVal > 0) {
                priceEl.textContent = formatMoneyWithCode(priceVal);
            }
            if (btn && priceVal > 0) {
                btn.setAttribute('data-price', String(priceVal));
                btn.setAttribute('data-name', String(prod.name || ''));
            }
            if (btn) {
                const packs = normalizePackList(v.packs || v.pack_options || v.sale_packs);
                const mixBuilder = normalizeMixBuilder(v.mix_builder);
                if (packs.length) {
                    btn.setAttribute('data-packs', encodeURIComponent(JSON.stringify(packs)));
                } else {
                    btn.removeAttribute('data-packs');
                }
                if (mixBuilder) {
                    btn.setAttribute('data-mix-builder', encodeURIComponent(JSON.stringify(mixBuilder)));
                } else {
                    btn.removeAttribute('data-mix-builder');
                }
            }
            applyCardStockState(card, prod);
            const img = card.querySelector('.product-image img');
            if (img && prod.image_url) {
                img.src = prod.image_url;
            }
            const section = v.section || '';
            const fc = v.food_categories;
            let fcStr = '';
            if (Array.isArray(fc)) {
                fcStr = fc.join(', ');
            } else if (typeof fc === 'string') {
                fcStr = fc;
            }
            if (section === 'main' && fcStr) {
                card.setAttribute('data-food-category', fcStr);
                card.setAttribute('data-product-category', fcStr);
            }
        });
        const featuredGrid = document.querySelector('#featured-dishes .discounts-grid') || document.querySelector('.special-discounts .discounts-grid');
        const mainGrid = document.querySelector('#menu-gastronomia .products-grid') || document.querySelector('#menu-electronica .products-grid');
        const interestGrid = document.querySelector('.interest-products .products-grid');
        const existingFeaturedIds = new Set();
        const existingMainIds = new Set();
        const existingInterestIds = new Set();
        try {
            if (featuredGrid) {
                featuredGrid.querySelectorAll('.add-to-cart-btn').forEach(btn => {
                    const bid = (btn.getAttribute('data-id') || '').trim();
                    if (bid) existingFeaturedIds.add(bid);
                });
            }
            if (mainGrid) {
                mainGrid.querySelectorAll('.add-to-cart-btn').forEach(btn => {
                    const bid = (btn.getAttribute('data-id') || '').trim();
                    if (bid) existingMainIds.add(bid);
                });
            }
            if (interestGrid) {
                interestGrid.querySelectorAll('.add-to-cart-btn').forEach(btn => {
                    const bid = (btn.getAttribute('data-id') || '').trim();
                    if (bid) existingInterestIds.add(bid);
                });
            }
        } catch (_) {}
        const normalizeMainCategory = (value) => {
            const text = String(value || '').trim().toLowerCase();
            if (!text) return '';
            return text.normalize ? text.normalize('NFD').replace(/[\u0300-\u036f]/g, '') : text;
        };
        const configuredMainCategories = Array.isArray(window.BusinessConfig && window.BusinessConfig.main_menu_categories)
            ? window.BusinessConfig.main_menu_categories
            : [];
        const mainCategoryOrder = new Map();
        configuredMainCategories.forEach((raw, index) => {
            const catId = normalizeMainCategory(raw && (raw.id || raw.value || raw.slug));
            if (!catId || mainCategoryOrder.has(catId)) return;
            mainCategoryOrder.set(catId, index);
        });
        if (!mainCategoryOrder.size) {
            try {
                document.querySelectorAll('#category-filter .filter-btn').forEach((btn, index) => {
                    const catId = normalizeMainCategory(btn.getAttribute('data-filter'));
                    if (!catId || catId === 'todos' || mainCategoryOrder.has(catId)) return;
                    mainCategoryOrder.set(catId, index);
                });
            } catch (_) {}
        }
        const productById = new Map();
        arr.forEach(p => {
            if (!p || !p.id) return;
            productById.set(String(p.id), p);
        });

        arr.forEach(p => {
            if (!p || !p.id) return;
            if (p.active === false) return;
            const v = p._variants || {};
            const fc = v.food_categories;
            const hasFoodCats = (Array.isArray(fc) && fc.length > 0) || (typeof fc === 'string' && fc.trim());
            const hasInterestTag = !!String(v.interest_tag || '').trim();
            const baseSection = (v.section || '').trim();
            const sections = [];
            if (baseSection) sections.push(baseSection);
            if (hasFoodCats && !sections.includes('main')) sections.push('main');
            if (hasInterestTag && !sections.includes('interest')) sections.push('interest');
            if (!sections.length) sections.push('main');

            const normalizeTag = (value) => {
                const s = String(value || '').trim().toLowerCase();
                return s.normalize ? s.normalize('NFD').replace(/[\u0300-\u036f]/g, '') : s;
            };

            const priceVal = isFinite(parseInt(p.price)) ? parseInt(p.price) : 0;
            const imgSrc = p.image_url || '';
            let priceText = '';
            if (priceVal > 0) {
                priceText = formatMoneyWithCode(priceVal);
            }
            const packs = normalizePackList(v.packs || v.pack_options || v.sale_packs);
            const packsAttr = packs.length ? ' data-packs="' + encodeURIComponent(JSON.stringify(packs)) + '"' : '';
            const mixBuilder = normalizeMixBuilder(v.mix_builder);
            const mixAttr = mixBuilder ? ' data-mix-builder="' + encodeURIComponent(JSON.stringify(mixBuilder)) + '"' : '';

            const buildCard = (section, className) => {
                const card = document.createElement('div');
                card.className = className;
                card.id = `${p.id}--${section}`;
                card.setAttribute('data-product-id', p.id);
                let fcStr = '';
                if (Array.isArray(fc)) {
                    fcStr = fc.join(', ');
                } else if (typeof fc === 'string') {
                    fcStr = fc;
                }
                if (section === 'main' && fcStr) {
                    card.setAttribute('data-food-category', fcStr);
                    card.setAttribute('data-product-category', fcStr);
                }
                if (section === 'interest') {
                    const tag = normalizeTag(v.interest_tag || '');
                    let cat = '';
                    if (tag === '2x1') {
                        cat = '2x1';
                    } else if (tag === 'promocion' || tag === 'promociones' || tag === 'oferta' || tag === 'ofertas' || tag === 'promo') {
                        cat = 'Promociones';
                    } else if (tag === 'especialidad' || tag === 'especialidad_de_la_casa' || tag === 'especialidad de la casa') {
                        cat = 'Especialidad de la casa';
                    } else if (tag === 'combos' || tag === 'combo') {
                        cat = 'Combos';
                    } else if (tag === 'entradas_rapidas' || tag === 'entradas rapidas' || tag === 'entradas' || tag === 'entrada') {
                        cat = 'Entradas rápidas';
                    } else if (!tag) {
                        cat = 'Promociones';
                    }
                    if (cat) {
                        card.setAttribute('data-interest-category', cat);
                    }
                }
                card.innerHTML = '<div class="product-image">' +
                    (imgSrc ? '<img src="' + imgSrc + '" alt="">' : '') +
                    '</div>' +
                    '<div class="product-info">' +
                    '<h3>' + (p.name || '') + '</h3>' +
                    '<p class="product-description">' + (p.details || '') + '</p>' +
                    '<div class="price-container">' +
                    (priceText ? '<p class="product-price">' + priceText + '</p>' : '') +
                    '</div>' +
                    '<button class="add-to-cart-btn" data-id="' + p.id + '" data-name="' + (p.name || '') + '" data-price="' + priceVal + '"' + packsAttr + mixAttr + '>Añadir al carrito</button>' +
                    '</div>';
                applyCardStockState(card, p);
                return card;
            };

            sections.forEach(section => {
                let targetGrid = null;
                let className = 'product-card searchable-item';
                if (section === 'featured') {
                    targetGrid = featuredGrid;
                    className = 'product-card discount-card searchable-item';
                } else if (section === 'main') {
                    targetGrid = mainGrid;
                } else if (section === 'interest') {
                    targetGrid = interestGrid;
                }
                if (!targetGrid) return;
                if (section === 'featured' && existingFeaturedIds.has(p.id)) return;
                if (section === 'main' && existingMainIds.has(p.id)) return;
                if (section === 'interest' && existingInterestIds.has(p.id)) return;
                const card = buildCard(section, className);
                targetGrid.appendChild(card);
                if (section === 'featured') existingFeaturedIds.add(p.id);
                if (section === 'main') existingMainIds.add(p.id);
                if (section === 'interest') existingInterestIds.add(p.id);
            });
        });

        const productOrder = new Map();
        arr.forEach((p, index) => {
            if (!p || !p.id) return;
            productOrder.set(String(p.id), index);
        });
        const getCardProductId = (el) => {
            const direct = String(el.getAttribute('data-product-id') || '').trim();
            if (direct) return direct;
            const btn = el.querySelector('.add-to-cart-btn');
            return String(btn && btn.getAttribute('data-id') || '').trim();
        };
        const getMainCardSortMeta = (card) => {
            const productId = getCardProductId(card);
            const product = productById.get(productId);
            const fallback = {
                categoryRank: Number.MAX_SAFE_INTEGER,
                positionRank: Number.MAX_SAFE_INTEGER,
                name: '',
                id: productId
            };
            if (!product) return fallback;
            const variants = product._variants || {};
            const rawCats = variants.food_categories;
            const categories = Array.isArray(rawCats)
                ? rawCats.map(normalizeMainCategory).filter(Boolean)
                : String(rawCats || '').split(',').map(normalizeMainCategory).filter(Boolean);
            const primaryCategory = categories.length ? categories[0] : '';
            const parsedPosition = parseInt(product.position, 10);
            return {
                categoryRank: mainCategoryOrder.has(primaryCategory) ? mainCategoryOrder.get(primaryCategory) : Number.MAX_SAFE_INTEGER - 1,
                positionRank: Number.isFinite(parsedPosition) && parsedPosition > 0 ? parsedPosition : Number.MAX_SAFE_INTEGER,
                name: String(product.name || '').toLowerCase(),
                id: productId
            };
        };
        const reorderGrid = (grid) => {
            if (!grid) return;
            const cards = Array.from(grid.children || []);
            cards.sort((a, b) => {
                if (grid === mainGrid) {
                    const aMeta = getMainCardSortMeta(a);
                    const bMeta = getMainCardSortMeta(b);
                    if (aMeta.categoryRank !== bMeta.categoryRank) return aMeta.categoryRank - bMeta.categoryRank;
                    if (aMeta.positionRank !== bMeta.positionRank) return aMeta.positionRank - bMeta.positionRank;
                    if (aMeta.name !== bMeta.name) return aMeta.name.localeCompare(bMeta.name);
                    return aMeta.id.localeCompare(bMeta.id);
                }
                const aIdx = productOrder.has(getCardProductId(a)) ? productOrder.get(getCardProductId(a)) : Number.MAX_SAFE_INTEGER;
                const bIdx = productOrder.has(getCardProductId(b)) ? productOrder.get(getCardProductId(b)) : Number.MAX_SAFE_INTEGER;
                return aIdx - bIdx;
            });
            cards.forEach(card => grid.appendChild(card));
        };
        reorderGrid(featuredGrid);
        reorderGrid(mainGrid);
        reorderGrid(interestGrid);
        bindAddToCartEvents(document);

        // Re-initialize modals and search items after dynamic content is loaded
        initProductModals();
        refreshSearchableItems();
        
        // Disparar evento para notificar que los productos se cargaron
        document.dispatchEvent(new CustomEvent('productsLoaded'));
        
    } catch (_) {}
}
