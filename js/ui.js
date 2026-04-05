/**
 * UI Animations and Interactions
 */
import { addToCart, updateCartDisplay, updateCartCount } from './cart.js?v=8';
import { getBusinessSlug, formatMoneyWithCode } from './config.js?v=8';
import { refreshSearchableItems } from './search.js?v=8';

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
    
    const cartIcon = document.querySelector('.cart-icon');
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
    setTimeout(() => {
        element.classList.remove('highlight-element');
    }, 2000);
}

// Handler de click "Añadir al carrito"
export function onAddToCartClick(event) {
    const button = event.currentTarget;
    const productCard = button.closest('.product-card');
    const productImage = productCard ? productCard.querySelector('.product-image img') : null;
    const titleEl = productCard ? productCard.querySelector('h3') : null;
    const priceEl = productCard ? productCard.querySelector('.product-price') : null;

    let id = button.getAttribute('data-id') || (productCard ? productCard.id : '') || `auto-${Date.now()}`;
    let name = button.getAttribute('data-name') || (titleEl ? titleEl.textContent.trim() : '') || (productImage ? (productImage.alt || '').trim() : '') || 'Producto';
    const attrPrice = button.getAttribute('data-price');
    let price = parseFloat(attrPrice);

    if (!isFinite(price) || price <= 0) {
        const priceText = priceEl ? priceEl.textContent : '';
        const match = priceText && priceText.match(/\d+[\.,]?\d*/);
        price = match ? parseFloat(match[0].replace('.', '').replace(',', '.')) : NaN;
    }

    if (!isFinite(price) || price <= 0) {
        console.warn('Precio inválido', { id, name });
        return;
    }

    const imageSrc = productImage ? productImage.getAttribute('src') : '';
    
    let notes = '';
    if (button.id === 'modal-add-to-cart-btn') {
        const notesEl = document.getElementById('modal-product-notes');
        if (notesEl) notes = notesEl.value;
    }

    addToCart(id, name, price, imageSrc, event, (evt) => {
        showAddToCartAnimation(evt);
        showAddedToCartIndicator(button);
    }, notes);
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
            
            // Reset button state
            modalAddBtn.textContent = 'Añadir al carrito';
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
                    '<button class="add-to-cart-btn" data-id="' + p.id + '" data-name="' + (p.name || '') + '" data-price="' + priceVal + '">Añadir al carrito</button>' +
                    '</div>';
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
        bindAddToCartEvents(document);

        // Re-initialize modals and search items after dynamic content is loaded
        initProductModals();
        refreshSearchableItems();
        
        // Disparar evento para notificar que los productos se cargaron
        document.dispatchEvent(new CustomEvent('productsLoaded'));
        
    } catch (_) {}
}
