// Página actual disponible de forma global para guardas por página
const PAGE = (document.body && document.body.dataset && document.body.dataset.page) || '';
let statusActionInterval = null;
let isPaymentMode = false;
let celebrationShown = false;


// Esperar a que el DOM esté completamente cargado
let backToTopForceVisibleUntil = 0; // Visibilidad forzada tras clic en círculos
document.addEventListener('DOMContentLoaded', function() {
    // Elementos del DOM
    const searchForm = document.getElementById('search-form');
    const searchInput = document.getElementById('search-input');
    const resultsContainer = document.getElementById('results-container');
    let searchableItems = document.querySelectorAll('.searchable-item');
    const cartIcon = document.querySelector('.cart-icon');
    const cartCount = document.getElementById('cart-count');
    const shoppingCart = document.getElementById('shopping-cart');
    const closeCart = document.getElementById('close-cart');
    const cartItems = document.getElementById('cart-items');
    const cartTotalPrice = document.getElementById('cart-total-price');
    const checkoutBtn = document.getElementById('checkout-btn');
    const clearCartBtn = document.getElementById('clear-cart-btn');
    const addToCartButtons = document.querySelectorAll('.add-to-cart-btn');
    const clearSearchBtn = document.getElementById('clear-search-btn');
    const floatingCart = document.getElementById('floating-cart');
    const floatingCartCount = document.getElementById('floating-cart-count');
    
    // Refrescar cache de elementos buscables tras render dinámico
    function refreshSearchableItems() {
        searchableItems = document.querySelectorAll('.searchable-item');
    }
    
    // Escuchar evento de productos cargados (desde ui.js)
    document.addEventListener('productsLoaded', () => {
        refreshSearchableItems();
    });
    const cartAnnouncements = document.getElementById('cart-announcements');
    // Página actual para activar componentes específicos
    // PAGE ya definido arriba

    // Región de anuncios accesibles para el carrito
    function announceCart(message) {
        if (!cartAnnouncements) return;
        // Limpiar y volver a establecer para forzar anuncio en lectores
        cartAnnouncements.textContent = '';
        setTimeout(() => {
            cartAnnouncements.textContent = message;
        }, 10);
    }

    // Crear overlay para cuando el carrito está abierto
    const overlay = document.createElement('div');
    overlay.className = 'overlay';
    document.body.appendChild(overlay);

    // Inicializar toggles de modalidad Mesa/Dirección/Espera
    const orderTypeRadios = document.querySelectorAll('input[name="orderType"]');
    const mesaFields = document.getElementById('order-mesa-fields');
    const addressFields = document.getElementById('order-address-fields');
    const esperaFields = document.getElementById('order-espera-fields');
    if (orderTypeRadios && (mesaFields || addressFields || esperaFields)) {
        orderTypeRadios.forEach(radio => {
            radio.addEventListener('change', () => {
                const val = radio.value;
                if (mesaFields) mesaFields.style.display = val === 'mesa' ? 'block' : 'none';
                if (addressFields) addressFields.style.display = val === 'direccion' ? 'block' : 'none';
                if (esperaFields) esperaFields.style.display = val === 'espera' ? 'block' : 'none';
                updateCartDisplay();
            });
        });
        // Estado inicial
        const checked = document.querySelector('input[name="orderType"]:checked');
        const val = checked ? checked.value : 'mesa';
        if (mesaFields) mesaFields.style.display = val === 'mesa' ? 'block' : 'none';
        if (addressFields) addressFields.style.display = val === 'direccion' ? 'block' : 'none';
        if (esperaFields) esperaFields.style.display = val === 'espera' ? 'block' : 'none';
    }

    // Configuración por rubro/negocio (parametrizable desde HTML/JS)
    const CATEGORY = window.CATEGORY || document.body.getAttribute('data-category') || 'general';
    const VENDOR_ID = window.VENDOR_ID || document.body.getAttribute('data-vendor') || 'default';
    const VENDOR_SLUG = window.VENDOR_SLUG || document.body.getAttribute('data-slug') || '';
    const THEME = window.THEME || document.body.getAttribute('data-theme') || '';
    const CART_KEY_PREFIX = window.CART_KEY_PREFIX || 'cart';
    // Helpers de configuración con precedencia: BusinessConfig > window.* > defaults
    function getBusinessSlug() {
        return window.BUSINESS_SLUG || VENDOR_SLUG || (document.body && document.body.dataset && document.body.dataset.slug) || '';
    }
    function getWhatsappNumber() {
        return (window.BusinessConfig && window.BusinessConfig.checkout && window.BusinessConfig.checkout.whatsappNumber)
            || window.WHATSAPP_NUMBER
            || '';
    }
    function getCheckoutMode() {
        const modeFromConfig = (window.BusinessConfig && window.BusinessConfig.checkout && window.BusinessConfig.checkout.mode) || undefined;
        const fallbackByCategory = (CATEGORY === 'servicios' ? 'whatsapp' : CATEGORY === 'comercio' ? 'whatsapp' : CATEGORY === 'gastronomia' ? 'mesa' : 'general');
        return modeFromConfig || window.CHECKOUT_MODE || fallbackByCategory;
    }
    // Intentar cargar configuración JSON por negocio de forma transparente (no rompe si no existe)
    (function maybeLoadBusinessConfig() {
        const slug = getBusinessSlug();
        if (!slug || (window.BusinessConfig && window.BusinessConfig.__loaded)) return;
        const url = `/api/config?slug=${slug}`;
        fetch(url).then(res => {
            if (!res.ok) throw new Error('No config JSON found');
            return res.json();
        }).then(json => {
            window.BusinessConfig = Object.assign({}, window.BusinessConfig || {}, json, { __loaded: true });
            document.dispatchEvent(new CustomEvent('businessconfig:ready'));
            console.info('BusinessConfig loaded from', url);
            if (typeof updateCartDisplay === 'function') updateCartDisplay();
        }).catch(() => {
            // Silencio si no hay config, se usan fallbacks
        });
    })();
    // Clave nueva con slug si existe, sino vendor id (compatibilidad)
    const LEGACY_CART_STORAGE_KEY = `${CART_KEY_PREFIX}_${CATEGORY}_${VENDOR_ID}`;
    // Nueva clave con namespace por categoría + comercio + tema/página para independencia entre locales
    const KEY_NAMESPACE = [CATEGORY, (VENDOR_SLUG || VENDOR_ID || 'default'), (THEME || PAGE || '')].filter(Boolean).join('_');
    const CART_STORAGE_KEY = window.CART_STORAGE_KEY || (`${CART_KEY_PREFIX}_${KEY_NAMESPACE}`);
    const SEARCH_HISTORY_KEY = `searchHistory_${KEY_NAMESPACE}`;
    let CHECKOUT_MODE = getCheckoutMode();
    console.info('Cart key:', CART_STORAGE_KEY, 'Legacy key:', LEGACY_CART_STORAGE_KEY, 'Slug:', VENDOR_SLUG || '(none)', 'Theme:', THEME || '(none)', 'Page:', PAGE || '(none)', 'Mode:', CHECKOUT_MODE);

    // Etiquetas del botón de checkout según modo
    if (checkoutBtn) {
        const modeNow = CHECKOUT_MODE;
        if (modeNow === 'whatsapp') {
            checkoutBtn.textContent = '🍽️ Realizar pedido';
        } else if (modeNow === 'envio') {
            checkoutBtn.textContent = '🚚 Finalizar compra';
        } else if (modeNow === 'mesa') {
            checkoutBtn.textContent = '🍽️ Realizar pedido';
        } else {
            checkoutBtn.textContent = '🛒 Finalizar';
        }
        // Actualizar etiqueta si llega BusinessConfig después
        document.addEventListener('businessconfig:ready', () => {
            CHECKOUT_MODE = getCheckoutMode();
            const modeLater = CHECKOUT_MODE;
            if (modeLater === 'whatsapp') {
                checkoutBtn.textContent = '🍽️ Realizar pedido';
            } else if (modeLater === 'envio') {
                checkoutBtn.textContent = '🚚 Finalizar compra';
            } else if (modeLater === 'mesa') {
                checkoutBtn.textContent = '🍽️ Realizar pedido';
            } else {
                checkoutBtn.textContent = '🛒 Finalizar';
            }
        });
    }

    // Carrito de compras
    let cart = [];

    // Funcionalidad de deslizamiento para descuentos especiales en móviles
    function initDiscountSwipe() {
        const discountsContainer = document.querySelector('.discounts-container');
        const discountsGrid = document.querySelector('.discounts-grid');
        
        if (!discountsContainer || !discountsGrid) return;
        
        let isDown = false;
        let startX;
        let scrollLeft;
        let startTime;
        let velocity = 0;
        
        // Eventos para mouse (desktop)
        discountsContainer.addEventListener('mousedown', (e) => {
            if (window.innerWidth > 768) return; // Solo en móviles
            isDown = true;
            startX = e.pageX - discountsContainer.offsetLeft;
            scrollLeft = discountsContainer.scrollLeft;
            startTime = Date.now();
            discountsContainer.style.cursor = 'grabbing';
        });
        
        discountsContainer.addEventListener('mouseleave', () => {
            isDown = false;
            discountsContainer.style.cursor = 'grab';
        });
        
        discountsContainer.addEventListener('mouseup', () => {
            isDown = false;
            discountsContainer.style.cursor = 'grab';
            applyMomentum();
        });
        
        discountsContainer.addEventListener('mousemove', (e) => {
            if (!isDown || window.innerWidth > 768) return;
            e.preventDefault();
            const x = e.pageX - discountsContainer.offsetLeft;
            const walk = (x - startX) * 2;
            discountsContainer.scrollLeft = scrollLeft - walk;
            
            // Calcular velocidad para momentum
            const currentTime = Date.now();
            const timeDiff = currentTime - startTime;
            velocity = walk / timeDiff;
        });
        
        // Eventos para touch (móviles)
        discountsContainer.addEventListener('touchstart', (e) => {
            isDown = true;
            startX = e.touches[0].pageX - discountsContainer.offsetLeft;
            scrollLeft = discountsContainer.scrollLeft;
            startTime = Date.now();
        });
        
        discountsContainer.addEventListener('touchmove', (e) => {
            if (!isDown) return;
            const x = e.touches[0].pageX - discountsContainer.offsetLeft;
            const walk = (x - startX) * 1.5;
            discountsContainer.scrollLeft = scrollLeft - walk;
            
            // Calcular velocidad para momentum
            const currentTime = Date.now();
            const timeDiff = currentTime - startTime;
            velocity = walk / timeDiff;
        });
        
        discountsContainer.addEventListener('touchend', () => {
            isDown = false;
            applyMomentum();
        });
        
        // Aplicar momentum al final del deslizamiento
        function applyMomentum() {
            if (Math.abs(velocity) > 0.5) {
                const momentum = velocity * 100;
                const targetScroll = discountsContainer.scrollLeft - momentum;
                
                discountsContainer.scrollTo({
                    left: Math.max(0, Math.min(targetScroll, discountsContainer.scrollWidth - discountsContainer.clientWidth)),
                    behavior: 'smooth'
                });
            }
        }
        
        // Agregar indicadores de scroll en móviles
        if (window.innerWidth <= 768) {
            updateScrollIndicators();
            discountsContainer.addEventListener('scroll', updateScrollIndicators);
        }
        
        function updateScrollIndicators() {
            const container = discountsContainer;
            
            // Remover indicadores existentes (ya no los necesitamos)
            const existingIndicators = container.querySelectorAll('.scroll-indicator');
            existingIndicators.forEach(indicator => indicator.remove());
            
            // Ya no agregamos indicadores de texto que causan problemas
            // Los botones de navegación ya proporcionan la funcionalidad necesaria
        }
    }
    
    // Inicializar funcionalidad de deslizamiento
    initDiscountSwipe();

    // Filtro de categorías (gastronomía) activado solo en página gastronomía
    if (PAGE === 'gastronomia') {
        const categoryFilter = document.getElementById('category-filter');
        let selectedCategory = 'todos';

        function itemMatchesSelectedCategory(item) {
            const catAttr = (item.getAttribute('data-food-category') || '').toLowerCase();
            const categories = catAttr.split(',').map(c => c.trim());
            if (selectedCategory === 'todos') return true;
            
            // Lógica simplificada: coincidencia directa
            return categories.includes(selectedCategory);
        }

        function applyCategoryFilter() {
            const menuSection = document.getElementById('menu-gastronomia');
            searchableItems.forEach(item => {
                const isInMenuSection = menuSection && menuSection.contains(item);
                if (!isInMenuSection) {
                    // No aplicar filtros fuera del menú gastronomía (ej. recomendados por interés)
                    item.style.display = '';
                    return;
                }
                item.style.display = itemMatchesSelectedCategory(item) ? '' : 'none';
            });
        }

        if (categoryFilter) {
            const filterButtons = categoryFilter.querySelectorAll('.filter-btn');
            filterButtons.forEach(btn => {
                btn.addEventListener('click', () => {
                    filterButtons.forEach(b => b.classList.remove('active'));
                    btn.classList.add('active');
                    selectedCategory = btn.getAttribute('data-filter') || 'todos';
                    applyCategoryFilter();

                    // Reaplicar búsqueda si está activa, respetando el filtro
                    const searchTerm = searchInput.value.trim().toLowerCase();
                    const searchResultsSection = document.querySelector('.search-results');
                    if (searchTerm !== '' && searchResultsSection.classList.contains('active')) {
                        // Aplicar búsqueda incluyendo recomendados por interés y filtrando solo el menú gastronomía
                        const menuSection = document.getElementById('menu-gastronomia');
                        const interestSection = document.querySelector('.interest-products');

                        const menuItemsForSearch = Array.from(searchableItems).filter(item => {
                            const isInMenuSection = menuSection && menuSection.contains(item);
                            return isInMenuSection && itemMatchesSelectedCategory(item);
                        });

                        const interestItemsForSearch = Array.from(searchableItems).filter(item => {
                            const isInInterestSection = interestSection && interestSection.contains(item);
                            return isInInterestSection;
                        });

                        const filteredItemsForSearch = [...menuItemsForSearch, ...interestItemsForSearch];
                        const results = performSearch(searchTerm, filteredItemsForSearch);
                        displayResults(results, searchTerm, resultsContainer);
                    }
                });
            });

            // Aplicar filtro inicial
            applyCategoryFilter();
            
            // Reaplicar filtro cuando lleguen productos dinámicos
            document.addEventListener('productsLoaded', () => {
                applyCategoryFilter();
            });
        }
    }

    // Filtro de categorías (Index/Comercio) activo solo en esas páginas
    if (PAGE === 'index' || PAGE === 'comercio') {
        const indexCategoryFilter = document.getElementById('index-category-filter');
        let selectedIndexCategory = 'todos';

        function itemMatchesIndexSelectedCategory(item) {
            const catAttr = (item.getAttribute('data-product-category') || '').toLowerCase();
            const categories = catAttr.split(',').map(c => c.trim());
            if (selectedIndexCategory === 'todos') return true;
            return categories.includes(selectedIndexCategory);
        }

        function applyIndexCategoryFilter() {
            const menuSection = document.getElementById('menu-electronica');
            searchableItems.forEach(item => {
                const isInMenuSection = menuSection && menuSection.contains(item);
                if (!isInMenuSection) {
                    // No aplicar filtros fuera del menú de electrónica
                    item.style.display = '';
                    return;
                }
                item.style.display = itemMatchesIndexSelectedCategory(item) ? '' : 'none';
            });
        }

        if (indexCategoryFilter) {
            const filterButtons = indexCategoryFilter.querySelectorAll('.filter-btn');
            const toggleBtn = document.getElementById('index-category-toggle');
            const inlineContainer = toggleBtn ? toggleBtn.parentElement : null; // .category-filter-inline

            // Toggle del menú desplegable
            if (toggleBtn && inlineContainer) {
                toggleBtn.addEventListener('click', () => {
                    const isOpen = inlineContainer.classList.toggle('open');
                    toggleBtn.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
                });

                // Cerrar al hacer click fuera
                document.addEventListener('click', (e) => {
                    if (!inlineContainer.contains(e.target)) {
                        inlineContainer.classList.remove('open');
                        toggleBtn.setAttribute('aria-expanded', 'false');
                    }
                });
            }

            filterButtons.forEach(btn => {
                btn.addEventListener('click', () => {
                    // Estado activo visual
                    filterButtons.forEach(b => b.classList.remove('active'));
                    btn.classList.add('active');

                    // Aplicar categoría
                    selectedIndexCategory = btn.getAttribute('data-filter') || 'todos';
                    applyIndexCategoryFilter();

                    // Cerrar el menú tras seleccionar
                    if (inlineContainer && toggleBtn) {
                        inlineContainer.classList.remove('open');
                        toggleBtn.setAttribute('aria-expanded', 'false');
                    }

                    // Reaplicar búsqueda si está activa, respetando el filtro de Index
                    const searchTerm = searchInput.value.trim().toLowerCase();
                    const searchResultsSection = document.querySelector('.search-results');
                    if (searchTerm !== '' && searchResultsSection.classList.contains('active')) {
                        const menuSection = document.getElementById('menu-electronica');
                        const interestSection = document.querySelector('.interest-products');

                        const menuItemsForSearch = Array.from(searchableItems).filter(item => {
                            const isInMenuSection = menuSection && menuSection.contains(item);
                            return isInMenuSection && itemMatchesIndexSelectedCategory(item);
                        });

                        const interestItemsForSearch = Array.from(searchableItems).filter(item => {
                            const isInInterestSection = interestSection && interestSection.contains(item);
                            return isInInterestSection;
                        });

                        const filteredItemsForSearch = [...menuItemsForSearch, ...interestItemsForSearch];
                        const results = performSearch(searchTerm, filteredItemsForSearch);
                        displayResults(results, searchTerm, resultsContainer);
                    }
                });
            });

            // Aplicar filtro inicial
            applyIndexCategoryFilter();
        }
    }
    
    // Función para escapar caracteres especiales en una expresión regular
    function escapeRegExp(string) {
        return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }
    
    // Construir regex insensible a acentos para resaltar correctamente
    function buildAccentInsensitiveRegex(term) {
        const map = {
            'a': '[aáàäâ]','e': '[eéèëê]','i': '[iíìïî]','o': '[oóòöô]','u': '[uúùüû]',
            'n': '[nñ]','c': '[cç]'
        };
        let pattern = '';
        for (const ch of term) {
            const lower = ch.toLowerCase();
            if (map[lower]) {
                pattern += map[lower];
            } else {
                pattern += escapeRegExp(ch);
            }
        }
        return new RegExp('(' + pattern + ')', 'gi');
    }

    // Función para resaltar el término (insensible a acentos y mayúsculas)
    function highlightTerm(text, term) {
        if (!text || !term) return text || '';
        const regex = buildAccentInsensitiveRegex(term);
        return text.replace(regex, '<span class="highlight">$1</span>');
    }
    
    // Función para resaltar un elemento
    function highlightElement(element) {
        // Agregar clase para resaltar
        element.classList.add('highlight-element');
        
        // Desplazarse al elemento
        element.scrollIntoView({ behavior: 'smooth', block: 'start' });
        
        // Quitar la clase después de 2 segundos
        setTimeout(function() {
            element.classList.remove('highlight-element');
        }, 2000);
    }
    
    // Función para extraer un fragmento de texto que contiene el término de búsqueda
    function extractSnippet(text, term) {
        // Encontrar la posición del término de búsqueda en el texto
        const termIndex = text.indexOf(term);
        
        // Determinar el inicio del fragmento (máximo 50 caracteres antes del término)
        const snippetStart = Math.max(0, termIndex - 50);
        
        // Determinar el final del fragmento (máximo 50 caracteres después del término)
        const snippetEnd = Math.min(text.length, termIndex + term.length + 50);
        
        // Extraer el fragmento
        let snippet = text.substring(snippetStart, snippetEnd);
        
        // Agregar puntos suspensivos si el fragmento no comienza desde el inicio del texto
        if (snippetStart > 0) {
            snippet = '...' + snippet;
        }
        
        // Agregar puntos suspensivos si el fragmento no termina al final del texto
        if (snippetEnd < text.length) {
            snippet = snippet + '...';
        }
        
        return snippet;
    }
    
    // Utilidades para búsqueda mejorada
    function normalizeForSearch(str) {
        return (str || '')
            .toLowerCase()
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '');
    }

    // Grupos de sinónimos (normalizados) para ampliar coincidencias reales del catálogo
    const SYNONYMS_GROUPS = [
        // Móviles / teléfonos
        ['smartphone','celu','celular','telefono','movil','móvil','phone'],
        // Portátiles / computadoras
        ['laptop','notebook','portatil','portátil','lapto','compu','computadora','pc'],
        // Audio
        ['auriculares','headphones','cascos'],
        ['altavoz','parlante','speaker'],
        // Imagen
        ['camara','cámara','dslr','fotocamara'],
        // Pantalla
        ['monitor','pantalla'],
        // Periféricos
        ['teclado','keyboard'],
        ['mouse','raton','ratón'],
        // Ofertas
        ['promo','promocion','oferta','liquidacion','liquidaciones'],
        ['2x1','dos por uno'],
        // Consolas
        ['xbox','consola'],
        ['playstation','ps4','ps5'],
        ['nintendo','switch'],
        // Televisores
        ['tv','televisor','tele','smart tv'],
        // Wearables
        ['smartwatch','reloj inteligente','watch'],
        // Tablets
        ['tablet','tableta','ipad'],
        // Impresión
        ['impresora','printer'],
        // Almacenamiento y memoria
        ['memoria','ram'],
        ['disco','ssd','solid state','estado solido','estado sólido'],
        // Redes
        ['router','modem','ruteador'],
        // Electrohogar
        ['heladera','refrigerador','nevera','frigorifico','frigorífico'],
        ['lavarropas','lavadora'],
        ['microondas','micro'],
        // Gastronomía
        ['hamburguesa','hamb'],
        ['pizza','pizzas'],
        ['empanada','empanadas'],
        ['coctel','cóctel','bebida','trago','drink'],
        ['cafe','café','cafetera','espresso','expreso'],
        ['cerveza','birra'],
        ['vino','tinto','blanco'],
        ['postre','dulce','dessert'],
        ['ensalada','salad'],
        ['sandwich','sándwich','tostado'],
        ['gaseosa','refresco','soda'],
        ['helado','ice cream'],
        ['combo','pack']
    ].map(group => group.map(normalizeForSearch));

    function expandSynonyms(term) {
        const t = normalizeForSearch(term);
        const variants = new Set([t]);
        SYNONYMS_GROUPS.forEach(group => {
            if (group.some(word => t.includes(word))) {
                group.forEach(word => variants.add(t.replace(group.find(w => t.includes(w)) || word, word)));
                // También agregar cada sinónimo como término independiente
                group.forEach(word => variants.add(word));
            }
        });
        return Array.from(variants);
    }

    function buildSearchText(item) {
        const texts = [
            item.textContent,
            item.getAttribute('data-food-category'),
            item.getAttribute('data-product-category'),
            item.getAttribute('data-tags')
        ];
        const img = item.querySelector('.product-image img');
        if (img) texts.push(img.alt);
        const btn = item.querySelector('.add-to-cart-btn');
        if (btn) {
            texts.push(btn.getAttribute('data-name'));
            texts.push(btn.getAttribute('data-id'));
        }
        return normalizeForSearch(texts.filter(Boolean).join(' '));
    }

    function matchesSearch(term, item) {
        const variants = expandSynonyms(term);
        const text = buildSearchText(item);
        return variants.some(v => text.includes(v));
    }

    function findMatchedVariant(term, item) {
        const variants = expandSynonyms(term);
        const text = buildSearchText(item);
        return variants.find(v => text.includes(v)) || null;
    }

    // Función para realizar la búsqueda
    function performSearch(term, items) {
        const results = [];
        
        items.forEach(item => {
            const addToCartBtn = item.querySelector('.add-to-cart-btn');
            const productImage = item.querySelector('.product-image img');
            const productDescription = item.querySelector('.product-description');
            const productPrice = item.querySelector('.product-price');

            if (matchesSearch(term, item)) {
                const titleEl = item.querySelector('h3');
                const itemTitle = titleEl ? titleEl.textContent : (addToCartBtn ? (addToCartBtn.getAttribute('data-name') || '') : '');
                const itemId = item.id;
                const matchedVariant = findMatchedVariant(term, item);
                const snippet = extractSnippet((item.textContent || '').toLowerCase(), (matchedVariant || term));

                results.push({
                    id: itemId,
                    title: itemTitle,
                    snippet: snippet,
                    image: productImage ? productImage.src : '',
                    imageAlt: productImage ? productImage.alt : '',
                    description: productDescription ? productDescription.textContent : '',
                    price: productPrice ? productPrice.textContent : '',
                    productId: addToCartBtn ? addToCartBtn.getAttribute('data-id') : '',
                    productName: addToCartBtn ? addToCartBtn.getAttribute('data-name') : '',
                    productPrice: addToCartBtn ? addToCartBtn.getAttribute('data-price') : '',
                    matchedVariant: matchedVariant
                });
            }
        });
        
        return results;
    }
    
    // Función para mostrar los resultados
    function displayResults(results, term, container) {
        // Limpiar el contenedor de resultados
        container.innerHTML = '';
        
        // Verificar si hay resultados
        if (results.length === 0) {
            // Mostrar mensaje de que no hay resultados
            container.innerHTML = '<p class="no-results">No se encontraron resultados para "' + term + '".</p>';
            return;
        }
        

        
        // Crear un elemento para cada resultado con diseño horizontal
        results.forEach(result => {
            // Crear el elemento del resultado con diseño horizontal
            const resultItem = document.createElement('div');
            resultItem.className = 'search-result-item';
            
            // Crear la imagen del producto
            const resultImageContainer = document.createElement('div');
            resultImageContainer.className = 'search-result-image';
            
            if (result.image) {
                const resultImage = document.createElement('img');
                resultImage.src = result.image;
                resultImage.alt = result.imageAlt;
                resultImage.loading = 'lazy';
                resultImageContainer.appendChild(resultImage);
            } else {
                // Placeholder si no hay imagen
                const placeholder = document.createElement('div');
                placeholder.className = 'image-placeholder';
                placeholder.innerHTML = '<i class="fas fa-image"></i>';
                resultImageContainer.appendChild(placeholder);
            }
            
            // Crear el contenedor de información del producto
            const resultInfo = document.createElement('div');
            resultInfo.className = 'search-result-info';
            
            // Crear el título del resultado
            const resultTitle = document.createElement('h3');
            resultTitle.className = 'search-result-title';
            const titleHighlightTerm = result.matchedVariant || term;
            resultTitle.innerHTML = highlightTerm(result.title, titleHighlightTerm);
            
            // Crear la descripción del producto
            const resultDescription = document.createElement('p');
            resultDescription.className = 'search-result-description';
            const descHighlightTerm = result.matchedVariant || term;
            resultDescription.innerHTML = highlightTerm(result.description, descHighlightTerm);
            
            // Crear el precio del producto
            const resultPrice = document.createElement('p');
            resultPrice.className = 'search-result-price';
            const priceHighlightTerm = result.matchedVariant || term;
            resultPrice.innerHTML = highlightTerm(result.price, priceHighlightTerm);
            
            // Agregar elementos al contenedor de información
            resultInfo.appendChild(resultTitle);
            resultInfo.appendChild(resultDescription);
            resultInfo.appendChild(resultPrice);
            
            // Crear el contenedor de acciones
            const resultActions = document.createElement('div');
            resultActions.className = 'search-result-actions';
            
            // Crear botón de agregar al carrito
            if (result.productId) {
                const addToCartBtn = document.createElement('button');
                addToCartBtn.className = 'search-add-to-cart-btn';
                addToCartBtn.innerHTML = '<i class="fas fa-cart-plus"></i> Agregar';
                addToCartBtn.setAttribute('data-id', result.productId);
                addToCartBtn.setAttribute('data-name', result.productName);
                addToCartBtn.setAttribute('data-price', result.productPrice);
                
                // Event listener para agregar al carrito
                addToCartBtn.addEventListener('click', function(e) {
                    e.preventDefault();
                    const productId = this.getAttribute('data-id');
                    const productName = this.getAttribute('data-name');
                    const productPrice = this.getAttribute('data-price');
                    
                    // Convertir el precio a número (viene como string sin formato)
                    const priceNumber = parseInt(productPrice);
                    
                    // Llamar a addToCart con los parámetros correctos
                    addToCart(productId, productName, priceNumber, result.image, e);
                    
                    // Feedback visual
                    this.innerHTML = '<i class="fas fa-check"></i> Agregado';
                    this.style.backgroundColor = '#28a745';
                    setTimeout(() => {
                        this.innerHTML = '<i class="fas fa-cart-plus"></i> Agregar';
                        this.style.backgroundColor = '';
                    }, 2000);
                });
                
                resultActions.appendChild(addToCartBtn);
            }
            
            // Crear enlace para ver más detalles
            const resultLink = document.createElement('button');
            resultLink.className = 'search-view-more-btn';
            resultLink.innerHTML = '<i class="fas fa-eye"></i> Ver más';
            resultLink.addEventListener('click', function(event) {
                event.preventDefault();
                
                // Ocultar los resultados
                const searchResultsSection = document.querySelector('.search-results');
                searchResultsSection.classList.remove('active');
                
                // Resaltar el elemento encontrado
                const targetElement = document.getElementById(result.id);
                if (targetElement) {
                    highlightElement(targetElement);
                    // Scroll suave al elemento
                    targetElement.scrollIntoView({ behavior: 'smooth', block: 'center' });
                }
            });
            
            resultActions.appendChild(resultLink);
            
            // Ensamblar el resultado completo
            resultItem.appendChild(resultImageContainer);
            resultItem.appendChild(resultInfo);
            resultItem.appendChild(resultActions);
            
            // Agregar el resultado al contenedor
            container.appendChild(resultItem);
        });
    }
    
    // Cargar carrito desde localStorage si existe
    function loadCart() {
        // Intentar cargar desde la clave nueva
        let savedCart = localStorage.getItem(CART_STORAGE_KEY);
        // Si no existe y la clave legacy es diferente, intentar migrar
        if (!savedCart && LEGACY_CART_STORAGE_KEY !== CART_STORAGE_KEY) {
            const legacyCart = localStorage.getItem(LEGACY_CART_STORAGE_KEY);
            if (legacyCart) {
                try {
                    // Migrar a la nueva clave
                    localStorage.setItem(CART_STORAGE_KEY, legacyCart);
                    // Opcional: mantener legacy por compatibilidad sin borrar
                    savedCart = legacyCart;
                    console.info('Migrado carrito desde clave legacy a nueva:', LEGACY_CART_STORAGE_KEY, '=>', CART_STORAGE_KEY);
                } catch (e) {
                    console.warn('No se pudo migrar carrito legacy:', e);
                }
            }
        }
        if (savedCart) {
            try {
                cart = JSON.parse(savedCart);
            } catch (e) {
                console.error('Error parseando carrito almacenado, reinicio:', e);
                cart = [];
            }
            updateCartCount();
        }
    }
    
    // Guardar carrito en localStorage
    function saveCart() {
        localStorage.setItem(CART_STORAGE_KEY, JSON.stringify(cart));
    }
    
    // Actualizar contador del carrito
    function updateCartCount() {
        const totalItems = cart.reduce((total, item) => total + item.quantity, 0);
        if (cartCount) cartCount.textContent = totalItems;
        if (floatingCartCount) floatingCartCount.textContent = totalItems;
        
        // Mostrar u ocultar el carrito flotante según si hay productos
        if (floatingCart) {
            if (totalItems > 0) {
                floatingCart.classList.add('show');
            } else {
                floatingCart.classList.remove('show');
            }
        }
    }
    
    // Actualizar la visualización del carrito
    function updateCartDisplay() {
        // Salir si no hay UI del carrito
        if (!cartItems || !cartTotalPrice) return;
        // Limpiar el contenedor de elementos del carrito
        cartItems.innerHTML = '';
        
        // Verificar si el carrito está vacío
        if (cart.length === 0) {
            cartItems.innerHTML = '<p class="empty-cart">Tu carrito está vacío</p>';
            cartTotalPrice.textContent = '$0 ARS';
            announceCart('Carrito vacío. Total $0 ARS');
            return;
        }
        
        // Variable para el precio total
        let totalPrice = 0;
        
        // Crear un elemento para cada producto en el carrito
        cart.forEach(item => {
            // Crear el elemento del producto
            const cartItem = document.createElement('div');
            cartItem.className = 'cart-item';
            cartItem.setAttribute('data-id', item.id);
            
            // Crear la imagen del producto
            const itemImage = document.createElement('div');
            itemImage.className = 'cart-item-image';
            if (item.image) {
                const img = document.createElement('img');
                img.src = item.image;
                img.alt = item.name;
                img.loading = 'lazy';
                itemImage.appendChild(img);
            }
            
            // Crear contenedor de información del producto
            const itemInfo = document.createElement('div');
            itemInfo.className = 'cart-item-info';
            
            // Crear el nombre del producto
            const itemName = document.createElement('div');
            itemName.className = 'cart-item-name';
            itemName.textContent = item.name;
            
            // Crear el precio del producto
            const itemPrice = document.createElement('div');
            itemPrice.className = 'cart-item-price';
            itemPrice.textContent = '$' + parseInt(item.price).toLocaleString('es-AR') + ' ARS';
            
            // Crear el contenedor para la cantidad
            const itemQuantityContainer = document.createElement('div');
            itemQuantityContainer.className = 'cart-item-quantity-container';
            
            // Crear el botón para disminuir la cantidad
            const decreaseBtn = document.createElement('button');
            decreaseBtn.className = 'quantity-btn decrease';
            decreaseBtn.textContent = '-';
            decreaseBtn.addEventListener('click', function() {
                // Disminuir la cantidad del producto
                if (item.quantity > 1) {
                    item.quantity--;
                } else {
                    // Eliminar el producto si la cantidad es 1
                    const itemIndex = cart.findIndex(cartItem => cartItem.id === item.id);
                    if (itemIndex !== -1) {
                        cart.splice(itemIndex, 1);
                        announceCart('Producto eliminado: ' + item.name);
                    }
                }
                
                // Actualizar el carrito
                saveCart();
                updateCartDisplay();
                updateCartCount();
                if (item.quantity > 0) {
                    announceCart('Cantidad de ' + item.name + ' disminuida a ' + item.quantity);
                }
            });
            
            // Crear el elemento para mostrar la cantidad
            const itemQuantity = document.createElement('span');
            itemQuantity.className = 'cart-item-quantity';
            itemQuantity.textContent = item.quantity;
            
            // Crear el botón para aumentar la cantidad
            const increaseBtn = document.createElement('button');
            increaseBtn.className = 'quantity-btn increase';
            increaseBtn.textContent = '+';
            increaseBtn.addEventListener('click', function() {
                // Aumentar la cantidad del producto
                item.quantity++;
                
                // Actualizar el carrito
                saveCart();
                updateCartDisplay();
                updateCartCount();
                announceCart('Cantidad de ' + item.name + ' aumentada a ' + item.quantity);
            });
            
            // Agregar los botones y la cantidad al contenedor
            itemQuantityContainer.appendChild(decreaseBtn);
            itemQuantityContainer.appendChild(itemQuantity);
            itemQuantityContainer.appendChild(increaseBtn);
            
            // Crear el botón para eliminar el producto
            const removeBtn = document.createElement('button');
            removeBtn.className = 'remove-item-btn';
            removeBtn.innerHTML = '&times;';
            removeBtn.addEventListener('click', function() {
                // Eliminar el producto del carrito
                const itemIndex = cart.findIndex(cartItem => cartItem.id === item.id);
                if (itemIndex !== -1) {
                    cart.splice(itemIndex, 1);
                    announceCart('Producto eliminado: ' + item.name);
                }
                
                // Actualizar el carrito
                saveCart();
                updateCartDisplay();
                updateCartCount();
            });
            
            // Agregar elementos al contenedor de información
            itemInfo.appendChild(itemName);
            itemInfo.appendChild(itemPrice);
            itemInfo.appendChild(itemQuantityContainer);
            const itemNotesContainer = document.createElement('div');
            itemNotesContainer.className = 'cart-item-notes-container';
            const itemNotesLabel = document.createElement('label');
            itemNotesLabel.className = 'cart-item-notes-label';
            itemNotesLabel.textContent = 'Detalle';
            const itemNotesInput = document.createElement('input');
            itemNotesInput.type = 'text';
            itemNotesInput.className = 'cart-item-notes-input';
            itemNotesInput.placeholder = 'Ej: sin condimentos';
            itemNotesInput.value = (item.notes || '');
            itemNotesInput.addEventListener('input', function(){ item.notes = this.value || ''; saveCart(); });
            itemNotesContainer.appendChild(itemNotesLabel);
            itemNotesContainer.appendChild(itemNotesInput);
            itemInfo.appendChild(itemNotesContainer);
            
            // Agregar los elementos al elemento del producto
            cartItem.appendChild(itemImage);
            cartItem.appendChild(itemInfo);
            cartItem.appendChild(removeBtn);
            
            // Agregar el elemento del producto al contenedor
            cartItems.appendChild(cartItem);
            
            // Actualizar el precio total
        totalPrice += item.price * item.quantity;
        });

        // Calcular costo de envío si es a domicilio
        let shippingCost = 0;
        let currentOrderType = 'mesa';
        const checkedRadio = document.querySelector('input[name="orderType"]:checked');
        if (checkedRadio) currentOrderType = checkedRadio.value;

        if (currentOrderType === 'direccion' && window.BusinessConfig && window.BusinessConfig.shipping_cost) {
            shippingCost = parseInt(window.BusinessConfig.shipping_cost) || 0;
        }

        // Mostrar costo de envío si corresponde
        if (shippingCost > 0) {
            const shippingRow = document.createElement('div');
            shippingRow.className = 'cart-item shipping-row';
            shippingRow.style.cssText = 'border-top: 1px dashed #eee; margin-top: 10px; padding-top: 10px; background: none;';
            shippingRow.innerHTML = `
                <div class="cart-item-info" style="width:100%; display:flex; justify-content:space-between; align-items:center;">
                    <div class="cart-item-name" style="font-weight:bold; color: #666;">Costo de envío</div>
                    <div class="cart-item-price">$${shippingCost.toLocaleString('es-AR')} ARS</div>
                </div>
            `;
            cartItems.appendChild(shippingRow);
            totalPrice += shippingCost;
        }
        
        // Actualizar el precio total
         cartTotalPrice.textContent = '$' + parseInt(totalPrice).toLocaleString('es-AR') + ' ARS';
         announceCart('Total actualizado: $' + parseInt(totalPrice).toLocaleString('es-AR') + ' ARS');
         
         // No necesitamos mostrar métodos de pago ya que se envía por WhatsApp
    }
    
    // Función para añadir un producto al carrito
    function addToCart(id, name, price, imageSrc, event) {
        // Validaciones defensivas
        id = id || `auto-${Date.now()}`;
        name = (name && name.trim()) ? name : 'Producto';
        if (!isFinite(price) || price <= 0) {
            console.warn('Intento de añadir producto con precio inválido, operación cancelada.', { id, name, price });
            return;
        }
        // Verificar si el producto ya está en el carrito
        const existingItem = cart.find(item => item.id === id);
        
        if (existingItem) {
            // Incrementar la cantidad si el producto ya está en el carrito
            existingItem.quantity++;
        } else {
            // Agregar el producto al carrito si no está
            cart.push({
                id: id,
                name: name,
                price: price,
                image: imageSrc,
                quantity: 1,
                notes: ''
            });
        }
        
        // Actualizar el carrito
        saveCart();
        updateCartDisplay();
        updateCartCount();
        announceCart('Producto agregado: ' + name);
        
        // Mostrar animación de añadir al carrito
        if (event) {
            showAddToCartAnimation(event);
            
            // Mostrar indicador visual en el botón
            const button = event.currentTarget;
            showAddedToCartIndicator(button);
        }
    }
    
    // Función para vaciar el carrito
    function clearCart() {
        // Vaciar el array del carrito
        cart = [];
        
        // Actualizar el carrito
        saveCart();
        updateCartDisplay();
        updateCartCount();
        announceCart('Carrito vaciado. Total $0 ARS');
    }
    
    // Función para mostrar la animación de añadir al carrito
    function showAddToCartAnimation(event) {
        // Crear un elemento para la animación
        const animationElement = document.createElement('div');
        animationElement.className = 'add-to-cart-animation';
        
        // Obtener coordenadas del evento (compatible con touch y mouse)
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
        
        // Posicionar el elemento en la posición del clic/touch
        animationElement.style.left = clientX + 'px';
        animationElement.style.top = clientY + 'px';
        
        // Agregar el elemento al body
        document.body.appendChild(animationElement);
        
        // Obtener la posición del icono del carrito
        const cartIconRect = cartIcon ? cartIcon.getBoundingClientRect() : null;
        const cartIconX = cartIconRect ? (cartIconRect.left + cartIconRect.width / 2) : clientX;
        const cartIconY = cartIconRect ? (cartIconRect.top + cartIconRect.height / 2) : clientY;
        
        // Usar requestAnimationFrame para mejor rendimiento
        requestAnimationFrame(() => {
            // Animar el elemento hacia el icono del carrito
            animationElement.style.transition = 'all 0.6s cubic-bezier(0.25, 0.46, 0.45, 0.94)';
            animationElement.style.left = cartIconX + 'px';
            animationElement.style.top = cartIconY + 'px';
            animationElement.style.opacity = '0';
            animationElement.style.transform = 'scale(0.1)';
        });
        
        // Eliminar el elemento después de la animación
        setTimeout(function() {
            if (animationElement.parentNode) {
                document.body.removeChild(animationElement);
            }
        }, 600);
    }
    
    // Función para mostrar indicador visual en el botón de añadir al carrito
    function showAddedToCartIndicator(button) {
        // Guardar el texto original del botón
        const originalText = button.textContent;
        
        // Cambiar el texto y estilo del botón
        button.textContent = '¡Añadido!';
        button.classList.add('added-to-cart');
        
        // Restaurar el botón después de 1.5 segundos
        setTimeout(function() {
            button.textContent = originalText;
            button.classList.remove('added-to-cart');
        }, 1500);
    }
    
    // Estilo para resaltar elementos
    const style = document.createElement('style');
    style.textContent = `
        .highlight {
            color: #007bff;
            font-weight: 600;
            background-color: rgba(0, 123, 255, 0.1);
            padding: 2px 4px;
            border-radius: 3px;
            border-bottom: 2px solid #007bff;
        }
        
        .highlight-element {
            animation: highlight-pulse 2s;
        }
        
        @keyframes highlight-pulse {
            0% { box-shadow: 0 0 0 0 rgba(0, 123, 255, 0.4); }
            70% { box-shadow: 0 0 0 10px rgba(0, 123, 255, 0); }
            100% { box-shadow: 0 0 0 0 rgba(0, 123, 255, 0); }
        }
        
        .added-to-cart {
            background-color: #4CAF50 !important;
            color: white !important;
            animation: pulse-green 1.5s;
        }
        
        @keyframes pulse-green {
            0% { transform: scale(1); }
            50% { transform: scale(1.05); }
            100% { transform: scale(1); }
        }
    `;
    document.head.appendChild(style);
    
    // Utilidad de accesibilidad: foco atrapado en diálogos
    function getFocusableElements(container) {
        if (!container) return [];
        return Array.from(container.querySelectorAll(
            'a[href], area[href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), button:not([disabled]), iframe, [tabindex]:not([tabindex="-1"])'
        ));
    }
    let previouslyFocusedElement = null;
    function openDialog(dialog) {
        if (!dialog) return;
        previouslyFocusedElement = document.activeElement;
        dialog.setAttribute('aria-hidden', 'false');
        if (!dialog.hasAttribute('tabindex')) dialog.setAttribute('tabindex', '-1');
        const focusables = getFocusableElements(dialog);
        const first = focusables[0] || dialog;
        const last = focusables[focusables.length - 1] || dialog;
        function trap(e) {
            if (e.key === 'Tab') {
                if (focusables.length === 0) { e.preventDefault(); return; }
                if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
                else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
            }
        }
        dialog.addEventListener('keydown', trap);
        dialog._trapHandler = trap;
        first.focus();
    }
    function closeDialog(dialog) {
        if (!dialog) return;
        dialog.setAttribute('aria-hidden', 'true');
        if (dialog._trapHandler) {
            dialog.removeEventListener('keydown', dialog._trapHandler);
            dialog._trapHandler = null;
        }
        if (previouslyFocusedElement && previouslyFocusedElement.focus) {
            previouslyFocusedElement.focus();
        }
        previouslyFocusedElement = null;
    }

    // Eventos para el carrito de compras
    
    // Abrir carrito al hacer clic en el icono
    if (cartIcon && shoppingCart) {
        cartIcon.addEventListener('click', function() {
            shoppingCart.classList.add('active');
            overlay.classList.add('active');
            openDialog(shoppingCart);
            // Ocultar carrito flotante cuando se abre el carrito principal
            if (floatingCart) {
                floatingCart.classList.remove('show');
            }
        });
    }
    
    // Abrir carrito al hacer clic en el carrito flotante
    if (floatingCart) {
        floatingCart.addEventListener('click', function() {
            if (!shoppingCart) return;
            shoppingCart.classList.add('active');
            overlay.classList.add('active');
            openDialog(shoppingCart);
            // Ocultar carrito flotante cuando se abre el carrito principal
            floatingCart.classList.remove('show');
        });
    }
    
    // Cerrar carrito al hacer clic en el botón de cerrar
    if (closeCart && shoppingCart) {
        closeCart.addEventListener('click', function() {
            shoppingCart.classList.remove('active');
            overlay.classList.remove('active');
            closeDialog(shoppingCart);
            // Mostrar carrito flotante si hay productos
            if (floatingCart && cart.length > 0) {
                floatingCart.classList.add('show');
            }
        });
    }
    
    // Cerrar carrito al hacer clic en el overlay
    overlay.addEventListener('click', function() {
        if (!shoppingCart) return;
        shoppingCart.classList.remove('active');
        overlay.classList.remove('active');
        closeDialog(shoppingCart);
        // Mostrar carrito flotante si hay productos
        if (floatingCart && cart.length > 0) {
            floatingCart.classList.add('show');
        }
    });
    
    // Handler reutilizable para botones "Añadir al carrito"
    function onAddToCartClick(event) {
        const button = event.currentTarget;
        const productCard = button.closest('.product-card');
        const productImage = productCard ? productCard.querySelector('.product-image img') : null;
        const titleEl = productCard ? productCard.querySelector('h3') : null;
        const priceEl = productCard ? productCard.querySelector('.product-price') : null;

        // Leer atributos y aplicar fallbacks seguros
        let id = button.getAttribute('data-id') || (productCard ? productCard.id : '') || `auto-${Date.now()}`;
        let name = button.getAttribute('data-name') || (titleEl ? titleEl.textContent.trim() : '') || (productImage ? (productImage.alt || '').trim() : '') || 'Producto';
        const attrPrice = button.getAttribute('data-price');
        let price = parseFloat(attrPrice);

        if (!isFinite(price) || price <= 0) {
            // Intentar extraer desde el texto visible del precio
            const priceText = priceEl ? priceEl.textContent : '';
            const match = priceText && priceText.match(/\d+[\.,]?\d*/);
            price = match ? parseFloat(match[0].replace('.', '').replace(',', '.')) : NaN;
        }

        // Si sigue inválido, no agregar y avisar en consola
        if (!isFinite(price) || price <= 0) {
            console.warn('Precio inválido al añadir al carrito. Verifica data-price o texto de precio.', { id, name, attrPrice });
            return;
        }

        // Imagen
        let imageSrc = '';
        if (productImage) {
            const fullSrc = productImage.getAttribute('src');
            imageSrc = fullSrc || '';
        }

        addToCart(id, name, price, imageSrc, event);
        // Marcar como enlazado
        button.dataset.bound = 'true';
    }

    // Función para enlazar eventos a todos los botones de carrito existentes/no enlazados
    function bindAddToCartEvents(context) {
        const scope = context || document;
        const buttons = scope.querySelectorAll('.add-to-cart-btn:not(#modal-add-to-cart-btn)');
        buttons.forEach(btn => {
            if (btn.dataset.bound === 'true') return;
            btn.addEventListener('click', onAddToCartClick);
            btn.dataset.bound = 'true';
        });
    }

    // Enlazar inicialmente los botones presentes en el DOM
    bindAddToCartEvents(document);
    
    // Evento para el botón de vaciar carrito
    if (clearCartBtn) {
        clearCartBtn.addEventListener('click', clearCart);
    }
    
    // Evento para el botón de enviar por WhatsApp
    if (checkoutBtn) {
    checkoutBtn.addEventListener('click', function() {
        if (cart.length === 0) {
            alert('Tu carrito está vacío');
            return;
        }

        // Obtener modalidad de pedido y datos adicionales
        const orderTypeEl = document.querySelector('input[name="orderType"]:checked');
        // Si no hay selección (páginas sin radios), usar 'mesa' solo en gastronomía; en comercio/general no requerir dirección
        const orderType = orderTypeEl ? orderTypeEl.value : (CHECKOUT_MODE === 'mesa' ? 'mesa' : 'none');
        const mesaNumberEl = document.getElementById('mesa-number');
        const addressEl = document.getElementById('delivery-address');
        const localityEl = document.getElementById('delivery-locality');
        const contactPhoneEl = document.getElementById('contact-phone');
        const esperaNameEl = document.getElementById('espera-name');
        const esperaPhoneEl = document.getElementById('espera-phone');

        const mesaNumber = mesaNumberEl ? (mesaNumberEl.value || '').trim() : '';
        const address = addressEl ? (addressEl.value || '').trim() : '';
        const locality = localityEl ? (localityEl.value || '').trim() : '';
        const contactPhone = contactPhoneEl ? (contactPhoneEl.value || '').trim() : '';
        const esperaName = esperaNameEl ? (esperaNameEl.value || '').trim() : '';
        const esperaPhone = esperaPhoneEl ? (esperaPhoneEl.value || '').trim() : '';

        const orderNotesEl = document.getElementById('order-notes');
        const orderNotes = orderNotesEl ? (orderNotesEl.value || '').trim() : '';

        if (orderType === 'mesa' && !mesaNumber) {
            alert('Por favor, ingresa el número de mesa.');
            return;
        }
        if (orderType === 'direccion') {
            if (!address) {
                alert('Por favor, ingresa la dirección de entrega.');
                return;
            }
            if (!contactPhone) {
                alert('Por favor, ingresa el teléfono de contacto.');
                return;
            }
        }
        if (orderType === 'espera') {
            if (!esperaName) {
                alert('Por favor, ingresa tu nombre.');
                return;
            }
            if (!esperaPhone) {
                alert('Por favor, ingresa tu teléfono.');
                return;
            }
        }
        
        // Crear el mensaje de WhatsApp
        let mensaje = '¡Hola! 👋 Espero que estés muy bien.\n\n';
        mensaje += '🛒 Me gustaría realizar el siguiente pedido:\n\n';

        // Modalidad de pedido (solo si aplica)
        if (orderType === 'mesa') {
            mensaje += `📍 Modalidad: Mesa\n`;
            mensaje += `   🪑 Mesa N°: ${mesaNumber}\n\n`;
        } else if (orderType === 'direccion') {
            mensaje += `📍 Modalidad: Dirección\n`;
            mensaje += `   🏠 Dirección: ${address}\n`;
            if (locality) mensaje += `   📍 Localidad: ${locality}\n`;
            mensaje += `\n`;
        } else if (orderType === 'espera') {
            mensaje += `📍 Modalidad: Espera en local\n`;
            mensaje += `   👤 Nombre: ${esperaName}\n`;
            mensaje += `   📞 Teléfono: ${esperaPhone}\n\n`;
        }
        
        // Agregar cada producto del carrito
        cart.forEach((item, index) => {
            const precioFormateado = '$' + parseInt(item.price).toLocaleString('es-AR') + ' ARS';
            mensaje += `${index + 1}. 📦 ${item.name}\n`;
            mensaje += `   📊 Cantidad: ${item.quantity}\n`;
            mensaje += `   💵 Precio unitario: ${precioFormateado}\n`;
            mensaje += `   💰 Subtotal: $${parseInt(item.price * item.quantity).toLocaleString('es-AR')} ARS\n`;
            if ((item.notes || '').trim()) { mensaje += `   📝 Detalle: ${(item.notes||'').trim()}\n`; }
            mensaje += '\n';
        });
        
        // Agregar el total y sugerencia de propina si corresponde
        let shippingCost = 0;
        if (orderType === 'direccion' && window.BusinessConfig && window.BusinessConfig.shipping_cost) {
            shippingCost = parseInt(window.BusinessConfig.shipping_cost) || 0;
        }

        if (shippingCost > 0) {
            mensaje += `🚚 Costo de envío: $${shippingCost.toLocaleString('es-AR')} ARS\n`;
        }

        const totalNumber = cart.reduce((sum, item) => sum + (item.price * item.quantity), 0) + shippingCost;
        const totalText = '$' + parseInt(totalNumber).toLocaleString('es-AR') + ' ARS';
        // En comercio y general (plantilla base) no mostrar "(sin propina)" (comparación robusta por minúsculas)
        const currentCategory = (CATEGORY || (document.body && document.body.dataset && document.body.dataset.category) || '').toLowerCase();
        const isCommerce = currentCategory === 'comercio' || currentCategory === 'general';
        if (isCommerce) {
            mensaje += `💰 TOTAL: ${totalText}\n\n`;
        } else {
            mensaje += `💰 TOTAL (sin propina): ${totalText}\n`;
        }
        if (orderType === 'mesa') {
            const tip = Math.round(totalNumber * 0.10);
            const tipText = '$' + parseInt(tip).toLocaleString('es-AR') + ' ARS';
            const totalWithTip = totalNumber + tip;
            const totalWithTipText = '$' + parseInt(totalWithTip).toLocaleString('es-AR') + ' ARS';
            mensaje += `💁 Propina sugerida (10%): ${tipText}\n`;
            mensaje += `🍽️ TOTAL con propina sugerida: ${totalWithTipText}\n\n`;
        } else {
            mensaje += `\n`;
        }
        if (orderNotes) { mensaje += `📝 Detalle adicional: ${orderNotes}\n\n`; }
        if (orderType !== 'mesa') {
            mensaje += '¿Podrías confirmarme la disponibilidad y el método de entrega?\n\n';
        }
        // En comercio, consultar métodos de pago disponibles
        if (isCommerce) {
            mensaje += '¿Qué métodos de pago aceptan? (efectivo, débito, crédito, transferencia)\n\n';
        }
        mensaje += '¡Muchas gracias! 😊';
        
        // Codificar el mensaje para URL
        const mensajeCodificado = encodeURIComponent(mensaje);
        
        // Crear el enlace de WhatsApp con precedencia de BusinessConfig
        const urlWhatsApp = `https://wa.me/${getWhatsappNumber()}?text=${mensajeCodificado}`;

        // Enviar el pedido al backend (no bloquea WhatsApp si falla)
        try {
            // Determinar slug dinámico: data-tenant en <body> o nombre de archivo .html
            function getTenantSlug() {
                const dataSlug = (document.body && document.body.dataset && document.body.dataset.tenant) ? document.body.dataset.tenant.trim() : '';
                let slug = dataSlug;
                if (!slug) {
                    try {
                        const name = (window.location.pathname.split('/').pop() || '').replace(/\.html$/,'');
                        if (name) slug = name;
                    } catch (_) {}
                }
                // Normalizar alias comunes
                const alias = {
                    'gatrolocal1': 'gastronomia-local1',
                    'gastro-local1': 'gastronomia-local1',
                    'gastro1': 'gastronomia-local1'
                };
                slug = alias[slug] || slug || 'gastronomia-local1';
                return slug;
            }

            const payload = {
                tenant_slug: getTenantSlug(),
                order_type: orderType,
                table_number: orderType === 'mesa' ? mesaNumber : '',
                address: orderType === 'direccion' ? { address, locality } : {},
                customer_phone: orderType === 'direccion' ? contactPhone : (orderType === 'espera' ? esperaPhone : ''),
                customer_name: orderType === 'espera' ? esperaName : '',
                items: cart.map(it => ({ id: it.id, name: it.name, price: it.price, quantity: it.quantity, notes: it.notes || '' })),
                order_notes: orderNotes
            };
            // Usar mismo origen (Flask en 8000)
            const API_BASE = window.location.origin;
            fetch(new URL('/api/orders', API_BASE).toString(), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            }).then(async (resp) => {
                if (resp.ok) {
                    const data = await resp.json();
                    console.log('Pedido registrado en backend', data);
                    // Guardar ID para consulta de estado
                    if (data.order_id) {
                        localStorage.setItem('last_order_id', data.order_id);
                    }
                    try { announceCart(`Pedido #${data.order_id} registrado (Total: $${data.total}).`); } catch (_) {}
                } else {
                    const errText = await resp.text();
                    console.warn('No se pudo registrar el pedido en backend', resp.status, errText);
                    alert(`No se pudo registrar el pedido en el sistema (${resp.status}: ${errText}). Se continuará por WhatsApp.`);
                }
            }).catch(err => console.warn('Error de red al registrar pedido', err));
        } catch (e) {
            console.warn('Excepción al preparar el payload del pedido', e);
        }

        // Abrir WhatsApp
        window.open(urlWhatsApp, '_blank');
        
        // Limpiar el carrito después de enviar
        clearCart();
        shoppingCart.classList.remove('active');
        overlay.classList.remove('active');
    });
    }
    
    // Escuchar el evento de envío del formulario de búsqueda
    searchForm.addEventListener('submit', function(event) {
        // Prevenir el comportamiento predeterminado del formulario
        event.preventDefault();

        // Obtener el término de búsqueda y eliminar espacios en blanco al inicio y final
        const searchTerm = searchInput.value.trim().toLowerCase();

        // Obtener la sección de resultados
        const searchResultsSection = document.querySelector('.search-results');

        // Verificar si el término de búsqueda está vacío
        if (searchTerm === '') {
            // Ocultar la sección de resultados
            searchResultsSection.classList.remove('active');
            // Mostrar mensaje de que no hay resultados
            resultsContainer.innerHTML = '<p class="no-results">Por favor, ingresa un término de búsqueda.</p>';
            return;
        }

        // Mostrar la sección de resultados
        searchResultsSection.classList.add('active');

        // Determinar secciones según página y respetar filtro activo
        let menuSectionEl = null;
        let activeFilter = { type: null, value: 'todos' };

        if (PAGE === 'gastronomia') {
            menuSectionEl = document.getElementById('menu-gastronomia');
            const activeBtn = document.querySelector('#category-filter .filter-btn.active');
            activeFilter = { type: 'food', value: activeBtn ? (activeBtn.getAttribute('data-filter') || 'todos') : 'todos' };
        } else if (PAGE === 'index' || PAGE === 'comercio') {
            menuSectionEl = document.getElementById('menu-electronica');
            const activeBtn = document.querySelector('#index-category-filter .filter-btn.active');
            activeFilter = { type: 'product', value: activeBtn ? (activeBtn.getAttribute('data-filter') || 'todos') : 'todos' };
        }

        // Función local para validar si el item coincide con el filtro activo
        function matchesActiveFilter(item) {
            if (!activeFilter || activeFilter.value === 'todos') return true;
            if (activeFilter.type === 'food') {
                const catAttr = (item.getAttribute('data-food-category') || '').toLowerCase();
                const categories = catAttr.split(',').map(c => c.trim());
                if (activeFilter.value === 'bebidas-cocteles') {
                    return categories.includes('bebidas') || categories.includes('cocteles');
                }
                if (activeFilter.value === 'al-plato') {
                    return !categories.includes('bebidas') && !categories.includes('cocteles');
                }
                return categories.includes(activeFilter.value);
            }
            if (activeFilter.type === 'product') {
                const catAttr = (item.getAttribute('data-product-category') || '').toLowerCase();
                const categories = catAttr.split(',').map(c => c.trim());
                return categories.includes(activeFilter.value);
            }
            return true;
        }

        // Construir conjunto de items del menú según página más los recomendados por interés
        const interestSection = document.querySelector('.interest-products');

        const menuItemsForSearch = Array.from(searchableItems).filter(item => {
            const isInMenuSection = menuSectionEl && menuSectionEl.contains(item);
            return isInMenuSection && matchesActiveFilter(item);
        });

        const interestItemsForSearch = Array.from(searchableItems).filter(item => {
            const isInInterestSection = interestSection && interestSection.contains(item);
            return isInInterestSection;
        });

        const filteredItemsForSearch = [...menuItemsForSearch, ...interestItemsForSearch];
        const results = performSearch(searchTerm, filteredItemsForSearch);

        // Mostrar los resultados
        displayResults(results, searchTerm, resultsContainer);

        // Mostrar el botón de limpiar búsqueda
        clearSearchBtn.style.display = 'inline-block';
    });
    
    // Escuchar cambios en el campo de búsqueda para ocultar resultados cuando esté vacío
    searchInput.addEventListener('input', function() {
        const searchResultsSection = document.querySelector('.search-results');
        if (searchInput.value.trim() === '') {
            searchResultsSection.classList.remove('active');
            clearSearchBtn.style.display = 'none';
        }
    });
    
    // Funcionalidad del botón limpiar búsqueda
    clearSearchBtn.addEventListener('click', function() {
        // Limpiar el campo de búsqueda
        searchInput.value = '';
        
        // Ocultar la sección de resultados
        const searchResultsSection = document.querySelector('.search-results');
        searchResultsSection.classList.remove('active');
        
        // Ocultar el botón de limpiar
        clearSearchBtn.style.display = 'none';
        
        // Limpiar el contenedor de resultados
        resultsContainer.innerHTML = '';
        
        // Enfocar el campo de búsqueda
        searchInput.focus();
    });
    
    // Funcionalidad del modal de producto
    const productModal = document.getElementById('product-modal');
    const modalProductImage = document.getElementById('modal-product-image');
    const modalProductTitle = document.getElementById('modal-product-title');
    const modalProductDescription = document.getElementById('modal-product-description');
    const modalProductFeatures = document.getElementById('modal-product-features');
    const modalProductPrice = document.getElementById('modal-product-price');
    const modalAddToCartBtn = document.getElementById('modal-add-to-cart-btn');
    const closeModal = document.querySelector('.close-modal');

    // Algunas páginas no incluyen el modal; continuar sin error
    // if (!productModal) {
    //     console.debug('Página sin modal de producto');
    // }

    // Datos detallados de productos
    const productDetails = {
        1: {
            features: [
                'Pantalla AMOLED de 6.5" con resolución 2400x1080',
                'Cámara principal de 108MP con estabilización óptica',
                'Batería de 5000mAh con carga rápida de 65W',
                'Procesador Snapdragon 8 Gen 2',
                '8GB de RAM y 256GB de almacenamiento',
                'Resistente al agua IP68',
                'Conectividad 5G y WiFi 6'
            ]
        },
        2: {
            features: [
                'Procesador Intel Core i7 de 12va generación',
                '16GB de RAM DDR5 expandible hasta 32GB',
                'SSD NVMe de 512GB de alta velocidad',
                'Pantalla de 14" Full HD con tecnología IPS',
                'Tarjeta gráfica integrada Intel Iris Xe',
                'Batería de hasta 12 horas de duración',
                'Peso ultraligero de solo 1.2kg'
            ]
        },
        3: {
            features: [
                'Cancelación activa de ruido adaptativa',
                'Hasta 30 horas de reproducción con estuche',
                'Drivers de 40mm para sonido de alta fidelidad',
                'Conectividad Bluetooth 5.3 con codec aptX',
                'Carga rápida: 15 min = 3 horas de música',
                'Resistentes al sudor y agua IPX4',
                'Control táctil intuitivo'
            ]
        },
        4: {
            features: [
                'Monitor de ritmo cardíaco 24/7',
                'GPS integrado para seguimiento de rutas',
                'Más de 20 modos deportivos predefinidos',
                'Pantalla AMOLED de 1.4" siempre activa',
                'Batería de hasta 14 días de duración',
                'Resistente al agua hasta 50 metros',
                'Monitoreo del sueño y estrés'
            ]
        },
        5: {
            features: [
                'Pantalla IPS de 10.5" con resolución 2K',
                'Procesador octa-core de alto rendimiento',
                '128GB de almacenamiento expandible',
                '6GB de RAM para multitarea fluida',
                'Cámaras de 13MP trasera y 8MP frontal',
                'Batería de 8000mAh con carga rápida',
                'Soporte para stylus incluido'
            ]
        },
        6: {
            features: [
                'Sensor CMOS de 24.2MP de formato completo',
                'Grabación de video 4K a 60fps',
                'Sistema de enfoque automático de 693 puntos',
                'Estabilización de imagen en 5 ejes',
                'Pantalla LCD táctil de 3.2" articulada',
                'Conectividad WiFi y Bluetooth integrada',
                'Batería de larga duración (hasta 610 fotos)'
            ]
        },
        7: {
            features: [
                'Consola de nueva generación con 1TB de almacenamiento',
                'Procesador AMD Zen 2 de 8 núcleos',
                'GPU personalizada RDNA 2 con ray tracing',
                'Soporte para resolución 4K y 120fps',
                'SSD ultra rápido para tiempos de carga mínimos',
                'Retrocompatibilidad con miles de juegos',
                'Control inalámbrico con retroalimentación háptica'
            ]
        },
        8: {
            features: [
                'Pantalla OLED de 55" con tecnología 4K HDR',
                'Procesador α9 Gen 5 AI con Deep Learning',
                'Dolby Vision IQ y Dolby Atmos integrados',
                'webOS 22 con asistente de voz ThinQ AI',
                'HDMI 2.1 para gaming a 120Hz',
                'Diseño ultra delgado Gallery Design',
                'Certificación NVIDIA G-SYNC Compatible'
            ]
        },
        9: {
            features: [
                'Refrigerador No Frost de 350 litros',
                'Tecnología Twin Cooling Plus',
                'Dispensador de agua y hielo automático',
                'Control de temperatura digital preciso',
                'Cajones FreshZone para frutas y verduras',
                'Eficiencia energética clase A++',
                'Garantía extendida de 10 años en compresor'
            ]
        },
        10: {
            features: [
                'Capacidad de 8kg para familias grandes',
                '14 programas de lavado especializados',
                'Tecnología EcoBubble para lavado eficiente',
                'Motor Digital Inverter ultra silencioso',
                'Función de vapor para eliminar bacterias',
                'Pantalla LED con temporizador',
                'Garantía de 20 años en motor'
            ]
        }
    };

    // Función simple para mostrar el modal
    function showModal(productData) {
        if (!productModal) return;
        
        if (modalProductImage) modalProductImage.src = productData.imageSrc;
        if (modalProductImage) modalProductImage.alt = productData.name || 'Imagen del producto';
        if (modalProductTitle) modalProductTitle.textContent = productData.name;
        if (modalProductDescription) modalProductDescription.textContent = productData.description;
        if (modalProductPrice) modalProductPrice.textContent = '$' + parseInt(productData.price).toLocaleString('es-AR') + ' ARS';
        
        // Limpiar características anteriores
        if (modalProductFeatures) modalProductFeatures.innerHTML = '';
        
        // Agregar características si existen
        if (modalProductFeatures && productDetails[productData.id] && productDetails[productData.id].features) {
            productDetails[productData.id].features.forEach(feature => {
                const li = document.createElement('li');
                li.textContent = feature;
                modalProductFeatures.appendChild(li);
            });
        }
        
        // Configurar botón del modal
        if (modalAddToCartBtn) {
            modalAddToCartBtn.setAttribute('data-id', productData.id);
            modalAddToCartBtn.setAttribute('data-name', productData.name);
            modalAddToCartBtn.setAttribute('data-price', productData.price.replace(/[^0-9]/g, ''));
        }
        
        // Mostrar modal
        productModal.style.display = 'flex';
        productModal.classList.add('active');
        productModal.setAttribute('aria-hidden', 'false');
        openDialog(productModal);
        document.body.style.overflow = 'hidden';
    }
    
    // Vincular eventos de clic a las tarjetas de producto (incluye contenido dinámico)
    function bindProductCardClicks(root = document) {
        const cards = root.querySelectorAll ? root.querySelectorAll('.product-card') : [];
        cards.forEach((card) => {
            // Evitar duplicar listeners
            if (card.dataset.modalBound === 'true') return;
            card.dataset.modalBound = 'true';
            card.style.cursor = 'pointer';

            // Accesibilidad: hacer la tarjeta navegable por teclado y anunciar correctamente
            if (!card.hasAttribute('tabindex')) {
                card.setAttribute('tabindex', '0');
            }
            if (!card.hasAttribute('role')) {
                card.setAttribute('role', 'button');
            }
            const titleEl = card.querySelector('.product-info h3');
            const computedLabel = titleEl ? (titleEl.textContent || '').trim() : 'Ver detalles del producto';
            if (!card.hasAttribute('aria-label')) {
                card.setAttribute('aria-label', computedLabel);
            }
            card.addEventListener('keydown', function(e) {
                if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    // Simular clic para reutilizar la misma lógica
                    this.click();
                }
            });
            card.addEventListener('click', function(e) {
                if (e.target.classList.contains('add-to-cart-btn')) {
                    return;
                }
                const productCard = this;
                const addToCartBtn = productCard.querySelector('.add-to-cart-btn');
                if (!addToCartBtn) {
                    console.debug('Tarjeta sin botón add-to-cart, se omite modal');
                    return;
                }
                const productId = addToCartBtn.getAttribute('data-id') || '';
                const productName = addToCartBtn.getAttribute('data-name') || '';
                const productPrice = addToCartBtn.getAttribute('data-price') || '0';
                const descEl = productCard.querySelector('.product-description');
                const productDescription = descEl ? descEl.textContent : '';
                const imgEl = productCard.querySelector('.product-image img');
                const productImageSrc = imgEl ? imgEl.src : '';
                const productData = {
                    id: productId,
                    name: productName,
                    price: productPrice,
                    description: productDescription,
                    imageSrc: productImageSrc
                };
                showModal(productData);
            });
        });
    }
    // Inicial: vincular en documento completo
    bindProductCardClicks(document);

    // Insertar Skip Link y preparar landmarks accesibles
    (function setupSkipLink(){
        try {
            const mainEl = document.querySelector('main');
            if (mainEl && !mainEl.id) {
                mainEl.id = 'main-content';
            }
            if (!document.querySelector('.skip-link')) {
                const a = document.createElement('a');
                a.className = 'skip-link';
                a.href = '#main-content';
                a.textContent = 'Saltar al contenido';
                document.body.insertBefore(a, document.body.firstChild);
            }
        } catch (e) {
            console.warn('Skip link: no se pudo insertar', e);
        }
    })();

    // Cerrar modal
    function closeProductModal() {
        if (!productModal) return;
        productModal.classList.remove('active');
        productModal.style.display = 'none';
        closeDialog(productModal);
        productModal.setAttribute('aria-hidden', 'true');
        document.body.style.overflow = 'auto';
    }

    if (closeModal) {
        closeModal.addEventListener('click', closeProductModal);
    }

    // Cerrar modal al hacer clic fuera del contenido
    if (productModal) {
        productModal.addEventListener('click', function(e) {
            if (e.target === productModal) {
                closeProductModal();
            }
        });
    }

    // Cerrar modal con la tecla Escape
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') {
            if (productModal && productModal.classList.contains('active')) {
                closeProductModal();
            } else if (shoppingCart && shoppingCart.classList.contains('active')) {
                shoppingCart.classList.remove('active');
                overlay.classList.remove('active');
                closeDialog(shoppingCart);
                if (floatingCart && cart.length > 0) {
                    floatingCart.classList.add('show');
                }
            }
        }
    });

    // Funcionalidad del botón agregar al carrito del modal
    if (modalAddToCartBtn) {
        modalAddToCartBtn.addEventListener('click', function(event) {
        const productId = this.getAttribute('data-id');
        const productName = this.getAttribute('data-name');
        const productPrice = parseInt(this.getAttribute('data-price'));
        
        // Obtener la imagen del modal
        const modalImage = document.getElementById('modal-product-image');
        const imageSrc = modalImage ? modalImage.src : '';
        
        addToCart(productId, productName, productPrice, imageSrc);
        
        // Mostrar indicador visual en el botón
        showAddedToCartIndicator(this);
        
        // Mostrar animación de éxito
        showAddToCartAnimation(event);
        
        // Cerrar modal después de un breve delay para que se vea el feedback
        setTimeout(() => {
            closeProductModal();
        }, 800);
        });
    }

    // Cargar carrito al iniciar
    loadCart();
    updateCartDisplay();
    
    // Variables del carrusel
    let currentSlideIndex = 0;
    let carouselInterval;
    let isDragging = false;
    let isAutoPlayActive = true;
    let autoPlayDuration = 5000; // 5 segundos (valor fijo)
    let isCarouselVisible = true;
    let wasAutoPlayActiveBeforeHidden = false;
    let progressInterval;
    let progressStartTime;
    
    // Funciones del carrusel
    function initializeCarousel() {
        const carouselContainer = document.querySelector('.carousel-container');
        if (!carouselContainer) return;
        
        const slides = document.querySelectorAll('.carousel-slide');
        const indicators = document.querySelectorAll('.indicator');
        const prevBtn = document.querySelector('.carousel-prev');
        const nextBtn = document.querySelector('.carousel-next');
        const slideCount = slides.length;
        const percentPerSlide = slideCount > 0 ? (100 / slideCount) : 100;
        
        if (slides.length === 0) return;
        
        // Eventos de los botones
        if (prevBtn) {
            prevBtn.addEventListener('click', () => {
                previousSlide();
                resetCarouselInterval();
            });
        }
        
        if (nextBtn) {
            nextBtn.addEventListener('click', () => {
                nextSlide();
                resetCarouselInterval();
            });
        }
        
        // Eventos de los indicadores
        indicators.forEach((indicator, index) => {
            indicator.addEventListener('click', () => {
                goToSlide(index);
                resetCarouselInterval();
            });
        });
        
        // Auto-play del carrusel
        startCarouselInterval();
        // Mantener autoplay del carrusel activo por defecto
        
        // Pausar auto-play al hacer hover (solo si está activo)
        carouselContainer.addEventListener('mouseenter', () => {
            if (isAutoPlayActive) {
                stopCarouselInterval();
            }
        });
        
        carouselContainer.addEventListener('mouseleave', () => {
            if (isAutoPlayActive) {
                startCarouselInterval();
            }
        });
        
        // Pausar auto-play en eventos táctiles
        carouselContainer.addEventListener('touchstart', () => {
            if (isAutoPlayActive) {
                stopCarouselInterval();
            }
        });
        
        carouselContainer.addEventListener('touchend', () => {
            if (isAutoPlayActive) {
                // Reanudar después de un breve delay para evitar conflictos
                setTimeout(() => {
                    if (isAutoPlayActive) {
                        startCarouselInterval();
                    }
                }, 1000);
            }
        });
        
        // Pausar auto-play cuando se hace clic en indicadores
        indicators.forEach((indicator, index) => {
            indicator.addEventListener('click', () => {
                if (isAutoPlayActive) {
                    stopCarouselInterval();
                    // Reanudar después de 3 segundos
                    setTimeout(() => {
                        if (isAutoPlayActive) {
                            startCarouselInterval();
                        }
                    }, 3000);
                }
            });
        });
        
        // Soporte para navegación con teclado
        document.addEventListener('keydown', (e) => {
            if (e.key === 'ArrowLeft') {
                previousSlide();
                resetCarouselInterval();
            } else if (e.key === 'ArrowRight') {
                nextSlide();
                resetCarouselInterval();
            }
        });
        
        // Soporte para deslizamiento táctil natural en móviles
        let touchStartX = 0;
        let touchCurrentX = 0;
        let isDragging = false;
        let startTransform = 0;
        const slidesContainer = carouselContainer.querySelector('.carousel-slides');
        
        carouselContainer.addEventListener('touchstart', (e) => {
            touchStartX = e.changedTouches[0].screenX;
            touchCurrentX = touchStartX;
            isDragging = true;
            
            // Usar directamente el índice actual para evitar inconsistencias
            startTransform = -currentSlideIndex * percentPerSlide;
            
            stopCarouselInterval();
            // Eliminar completamente las transiciones durante gestos táctiles
            slidesContainer.style.transition = 'none';
            
            // Feedback visual: reducir ligeramente la escala del carrusel
            carouselContainer.style.transform = 'scale(0.98)';
            carouselContainer.style.transition = 'transform 0.2s ease';
        }, { passive: true });
        
        carouselContainer.addEventListener('touchmove', (e) => {
            if (!isDragging) return;
            
            touchCurrentX = e.changedTouches[0].screenX;
            const deltaX = touchCurrentX - touchStartX;
            const containerWidth = carouselContainer.offsetWidth;
            // Convertir el movimiento del dedo a porcentaje del contenedor de slides
            const dragPercentage = (deltaX / containerWidth) * percentPerSlide;
            
            // Aplicar transformación en tiempo real siguiendo el dedo
            const newTransform = startTransform + dragPercentage;
            slidesContainer.style.transition = 'none'; // Sin transición durante el arrastre
            slidesContainer.style.transform = `translateX(${newTransform}%)`;
            
            // Feedback visual adicional: cambiar opacidad basado en la distancia del arrastre
            const dragIntensity = Math.min(Math.abs(deltaX) / containerWidth, 0.3);
            carouselContainer.style.filter = `brightness(${1 - dragIntensity * 0.2})`;
        }, { passive: true });
        
        carouselContainer.addEventListener('touchend', (e) => {
            if (!isDragging) return;
            
            isDragging = false;
            const swipeDistance = touchCurrentX - touchStartX;
            const containerWidth = carouselContainer.offsetWidth;
            const swipeThreshold = containerWidth * 0.25; // Aumentar umbral para reducir sensibilidad
            
            // Restaurar transición suave
            slidesContainer.style.transition = 'transform 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94)';
            
            // Restaurar efectos visuales
            carouselContainer.style.transform = 'scale(1)';
            carouselContainer.style.filter = 'brightness(1)';
            carouselContainer.style.transition = 'transform 0.3s ease, filter 0.3s ease';
            
            if (Math.abs(swipeDistance) > swipeThreshold) {
                if (swipeDistance > 0) {
                    // Deslizamiento hacia la derecha - slide anterior
                    const newIndex = currentSlideIndex > 0 ? currentSlideIndex - 1 : (slideCount - 1);
                    currentSlideIndex = newIndex;
                    slidesContainer.style.transform = `translateX(-${currentSlideIndex * percentPerSlide}%)`;
                } else {
                    // Deslizamiento hacia la izquierda - slide siguiente
                    const newIndex = currentSlideIndex < (slideCount - 1) ? currentSlideIndex + 1 : 0;
                    currentSlideIndex = newIndex;
                    slidesContainer.style.transform = `translateX(-${currentSlideIndex * percentPerSlide}%)`;
                }
                // Actualizar indicadores sin llamar showSlide para evitar conflictos
                updateIndicators();
                
                // Feedback visual de éxito: breve pulso
                setTimeout(() => {
                    carouselContainer.style.transform = 'scale(1.02)';
                    setTimeout(() => {
                        carouselContainer.style.transform = 'scale(1)';
                    }, 100);
                }, 50);
            } else {
                // Volver a la posición original si no se alcanzó el umbral
                slidesContainer.style.transform = `translateX(-${currentSlideIndex * percentPerSlide}%)`;
            }
            
            resetCarouselInterval();
        }, { passive: true });
        
        // Cancelar arrastre si se sale del área
         carouselContainer.addEventListener('touchcancel', (e) => {
             if (isDragging) {
                 isDragging = false;
                 slidesContainer.style.transition = 'transform 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94)';
                 slidesContainer.style.transform = `translateX(-${currentSlideIndex * percentPerSlide}%)`;
                 
                 // Restaurar efectos visuales
                 carouselContainer.style.transform = 'scale(1)';
                 carouselContainer.style.filter = 'brightness(1)';
                 carouselContainer.style.transition = 'transform 0.3s ease, filter 0.3s ease';
                 
                 resetCarouselInterval();
             }
         }, { passive: true });
    }
    
    function showSlide(index) {
        const slides = document.querySelectorAll('.carousel-slide');
        const indicators = document.querySelectorAll('.indicator');
        const slidesContainer = document.querySelector('.carousel-slides');
        const slideCount = slides.length;
        const percentPerSlide = slideCount > 0 ? (100 / slideCount) : 100;
        
        if (slides.length === 0) return;
        
        // Remover clase active de todos los slides e indicadores
        slides.forEach(slide => slide.classList.remove('active'));
        indicators.forEach(indicator => indicator.classList.remove('active'));
        
        // Asegurar que el índice esté en el rango válido
        if (index >= slides.length) {
            currentSlideIndex = 0;
        } else if (index < 0) {
            currentSlideIndex = slides.length - 1;
        } else {
            currentSlideIndex = index;
        }
        
        // Aplicar transformación CSS para mostrar el slide correcto
        if (slidesContainer) {
            // Solo aplicar transición si no se está arrastrando
            if (!isDragging) {
                slidesContainer.style.transition = 'transform 0.5s ease-in-out';
            }
            // Desplazamiento proporcional según cantidad de slides
        slidesContainer.style.transform = `translateX(-${currentSlideIndex * percentPerSlide}%)`;
        }
        
        // Mostrar slide e indicador activos
        if (slides[currentSlideIndex]) {
            slides[currentSlideIndex].classList.add('active');
        }
        if (indicators[currentSlideIndex]) {
            indicators[currentSlideIndex].classList.add('active');
        }
    }
    
    function nextSlide() {
        showSlide(currentSlideIndex + 1);
        if (isAutoPlayActive) {
            startProgress();
        }
    }
    
    function previousSlide() {
        showSlide(currentSlideIndex - 1);
        if (isAutoPlayActive) {
            startProgress();
        }
    }
    
    function goToSlide(index) {
        showSlide(index);
        if (isAutoPlayActive) {
            startProgress();
        }
    }
    
    function updateIndicators() {
        const slides = document.querySelectorAll('.carousel-slide');
        const indicators = document.querySelectorAll('.indicator');
        
        // Remover clase active de todos los slides e indicadores
        slides.forEach(slide => slide.classList.remove('active'));
        indicators.forEach(indicator => indicator.classList.remove('active'));
        
        // Mostrar slide e indicador activos
        if (slides[currentSlideIndex]) {
            slides[currentSlideIndex].classList.add('active');
        }
        if (indicators[currentSlideIndex]) {
            indicators[currentSlideIndex].classList.add('active');
        }
    }
    
    function startCarouselInterval() {
        // Solo iniciar si el auto-play está activo
        if (!isAutoPlayActive) return;
        
        // Limpiar cualquier intervalo existente antes de crear uno nuevo
        if (carouselInterval) {
            clearInterval(carouselInterval);
        }
        
        startProgress();
        carouselInterval = setInterval(() => {
            nextSlide();
        }, autoPlayDuration);
    }

    // Funciones para el indicador de progreso SVG
    function startProgress() {
        const progressRing = document.querySelector('.progress-ring');
        const progressElement = document.querySelector('.progress-ring-progress');
        console.log('startProgress llamado, progressRing encontrado:', !!progressRing);
        if (progressRing && progressElement) {
            // Primero removemos la clase para detener cualquier animación
            progressRing.classList.remove('active');
            
            // Resetear manualmente el stroke-dasharray al estado inicial
            progressElement.style.strokeDasharray = '0 100.53';
            
            // Forzamos un reflow para asegurar que los cambios se apliquen
            progressRing.offsetHeight;
            
            // Pequeño delay para asegurar el reset completo
            setTimeout(() => {
                // Agregamos la clase nuevamente para iniciar la animación
                progressRing.classList.add('active');
                console.log('Clase active agregada. Clases actuales:', progressRing.className);
                console.log('Indicador de progreso SVG iniciado');
            }, 10);
        } else {
            console.error('No se encontró el elemento .progress-ring o .progress-ring-progress');
        }
    }
    
    function stopProgress() {
        const progressRing = document.querySelector('.progress-ring');
        const progressElement = document.querySelector('.progress-ring-progress');
        console.log('stopProgress llamado, progressRing encontrado:', !!progressRing);
        if (progressRing && progressElement) {
            progressRing.classList.remove('active');
            // Resetear al estado inicial
            progressElement.style.strokeDasharray = '0 100.53';
            console.log('Clase active removida. Clases actuales:', progressRing.className);
            console.log('Indicador de progreso SVG detenido');
        } else {
            console.error('No se encontró el elemento .progress-ring o .progress-ring-progress');
        }
    }
    
    function setupVisibilityObserver() {
        const carouselContainer = document.querySelector('.carousel-container');
        if (!carouselContainer) return;
        
        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    // El carrusel es visible
                    isCarouselVisible = true;
                    
                    // Reanudar auto-play si estaba activo antes de ocultarse
                    if (wasAutoPlayActiveBeforeHidden && !isAutoPlayActive) {
                        isAutoPlayActive = true;
                        startCarouselInterval();
                        
                        // Actualizar elementos visuales y de accesibilidad
                        const playPauseIcon = document.getElementById('play-pause-icon');
                        const progressContainer = document.querySelector('.carousel-play-button');
                        const autoplayStatus = document.getElementById('autoplay-status');
                        
                        if (playPauseIcon) {
                            playPauseIcon.className = 'fas fa-pause';
                        }
                        if (progressContainer) {
                            progressContainer.setAttribute('aria-label', 'Pausar reproducción automática del carrusel');
                        }
                        if (autoplayStatus) {
                            autoplayStatus.textContent = 'Reproducción automática activa';
                        }
                    }
                } else {
                    // El carrusel no es visible
                    isCarouselVisible = false;
                    
                    // Pausar auto-play si está activo
                    if (isAutoPlayActive) {
                        wasAutoPlayActiveBeforeHidden = true;
                        stopCarouselInterval();
                        isAutoPlayActive = false;
                        
                        // Actualizar elementos visuales y de accesibilidad
                        const playPauseIcon = document.getElementById('play-pause-icon');
                        const progressContainer = document.querySelector('.carousel-play-button');
                        const autoplayStatus = document.getElementById('autoplay-status');
                        
                        if (playPauseIcon) {
                            playPauseIcon.className = 'fas fa-play';
                        }
                        if (progressContainer) {
                            progressContainer.setAttribute('aria-label', 'Reanudar reproducción automática del carrusel');
                        }
                        if (autoplayStatus) {
                            autoplayStatus.textContent = 'Reproducción automática pausada (carrusel no visible)';
                        }
                    }
                }
            });
        }, {
            threshold: 0.5, // El carrusel debe estar al menos 50% visible
            rootMargin: '0px 0px -50px 0px' // Margen adicional para activar antes
        });
        
        observer.observe(carouselContainer);
    }
    

    
    function toggleAutoPlay() {
        const playPauseIcon = document.getElementById('play-pause-icon');
        const progressContainer = document.querySelector('.carousel-play-button');
        const autoplayStatus = document.getElementById('autoplay-status');
        
        if (isAutoPlayActive) {
            // Pausar auto-play
            stopCarouselInterval();
            isAutoPlayActive = false;
            
            // Actualizar elementos visuales y de accesibilidad
            if (playPauseIcon) {
                playPauseIcon.className = 'fas fa-play';
            }
            if (progressContainer) {
                progressContainer.setAttribute('aria-label', 'Reanudar reproducción automática del carrusel');
            }
            if (autoplayStatus) {
                autoplayStatus.textContent = 'Reproducción automática pausada';
            }
        } else {
            // Reanudar auto-play
            isAutoPlayActive = true;
            startCarouselInterval();
            
            // Actualizar elementos visuales y de accesibilidad
            if (playPauseIcon) {
                playPauseIcon.className = 'fas fa-pause';
            }
            if (progressContainer) {
                progressContainer.setAttribute('aria-label', 'Pausar reproducción automática del carrusel');
            }
            if (autoplayStatus) {
                autoplayStatus.textContent = 'Reproducción automática activa';
            }
        }
    }
    
    function stopCarouselInterval() {
        if (carouselInterval) {
            clearInterval(carouselInterval);
            carouselInterval = null; // Resetear la variable
        }
        stopProgress();
    }
    
    function resetCarouselInterval() {
        stopCarouselInterval();
        startCarouselInterval();
    }
    
    // Inicializar carrusel
    initializeCarousel();

    // Accesibilidad: asegurar anuncios del estado de autoplay
    (function setupAutoplayStatusA11y(){
        try {
            const statusEl = document.getElementById('autoplay-status');
            if (statusEl) {
                statusEl.setAttribute('role', 'status');
                statusEl.setAttribute('aria-live', 'polite');
            }
        } catch (e) {
            console.warn('Autoplay a11y: no se pudo configurar aria-live', e);
        }
    })();

    // ==========================================
    // SISTEMA DE BÚSQUEDA INTELIGENTE
    // ==========================================

    // Base de datos de sugerencias y palabras clave
    const searchSuggestions = [
        { text: 'laptop', type: 'producto', icon: 'fas fa-laptop' },
        { text: 'notebook', type: 'producto', icon: 'fas fa-laptop' },
        { text: 'computadora', type: 'producto', icon: 'fas fa-desktop' },
        { text: 'gaming', type: 'categoría', icon: 'fas fa-gamepad' },
        { text: 'asus', type: 'marca', icon: 'fas fa-tag' },
        { text: 'xbox', type: 'producto', icon: 'fab fa-xbox' },
        { text: 'consola', type: 'producto', icon: 'fas fa-gamepad' },
        { text: 'procesador', type: 'componente', icon: 'fas fa-microchip' },
        { text: 'memoria', type: 'componente', icon: 'fas fa-memory' },
        { text: 'ram', type: 'componente', icon: 'fas fa-memory' },
        { text: 'ssd', type: 'componente', icon: 'fas fa-hdd' },
        { text: 'disco', type: 'componente', icon: 'fas fa-hdd' },
        { text: 'gráfica', type: 'componente', icon: 'fas fa-tv' },
        { text: 'monitor', type: 'producto', icon: 'fas fa-desktop' },
        { text: 'teclado', type: 'accesorio', icon: 'fas fa-keyboard' },
        { text: 'mouse', type: 'accesorio', icon: 'fas fa-mouse' },
        { text: 'auriculares', type: 'accesorio', icon: 'fas fa-headphones' }
    ];

    // Variables para el sistema de búsqueda inteligente
    let searchHistory = JSON.parse(localStorage.getItem(SEARCH_HISTORY_KEY)) || [];
    let currentSuggestionIndex = -1;
    let filteredSuggestions = [];
    let searchTimeout;

    // Elementos del DOM para búsqueda inteligente (reutilizando searchInput ya definido)
    const suggestionsDropdown = document.getElementById('search-suggestions-dropdown');
    const suggestionsList = document.getElementById('suggestions-list');
    const historyList = document.getElementById('history-list');
    const hasSearchDropdown = !!(suggestionsDropdown && suggestionsList && historyList);

    // Función para guardar historial en localStorage
    function saveSearchHistory() {
        localStorage.setItem(SEARCH_HISTORY_KEY, JSON.stringify(searchHistory));
    }

    // Función para agregar término al historial
    function addToHistory(term) {
        // Remover si ya existe
        searchHistory = searchHistory.filter(item => item !== term);
        // Agregar al inicio
        searchHistory.unshift(term);
        // Mantener solo los últimos 10
        searchHistory = searchHistory.slice(0, 10);
        saveSearchHistory();
    }

    // Función para remover término del historial
    function removeFromHistory(term) {
        searchHistory = searchHistory.filter(item => item !== term);
        saveSearchHistory();
        updateHistoryDisplay();
    }

    // Función para limpiar todo el historial
    function clearAllHistory() {
        // Agregar animación de feedback al botón
        const clearBtn = document.getElementById('clear-all-history');
        if (clearBtn) {
            clearBtn.style.transform = 'scale(0.95)';
            clearBtn.style.opacity = '0.7';
            
            setTimeout(() => {
                clearBtn.style.transform = '';
                clearBtn.style.opacity = '';
            }, 150);
        }
        
        // Limpiar historial
        searchHistory = [];
        saveSearchHistory();
        updateHistoryDisplay();
        
        // Ocultar dropdown si no hay historial
        if (suggestionsDropdown.classList.contains('active')) {
            const query = searchInput.value.trim();
            if (!query || filterSuggestions(query).length === 0) {
                suggestionsDropdown.classList.remove('active');
            }
        }
    }

    // Función para filtrar sugerencias
    function filterSuggestions(query) {
        if (!query || query.length < 1) return [];
        
        const lowerQuery = query.toLowerCase();
        return searchSuggestions.filter(suggestion => 
            suggestion.text.toLowerCase().includes(lowerQuery)
        ).slice(0, 6); // Máximo 6 sugerencias
    }

    // Función para crear elemento de sugerencia
    function createSuggestionElement(suggestion, isHistory = false) {
        const item = document.createElement('div');
        item.className = 'suggestion-item';
        
        if (isHistory) {
            item.innerHTML = `
                <i class="suggestion-icon fas fa-history"></i>
                <span class="suggestion-text">${suggestion}</span>
                <i class="history-remove fas fa-times" data-term="${suggestion}"></i>
            `;
        } else {
            item.innerHTML = `
                <i class="suggestion-icon ${suggestion.icon}"></i>
                <span class="suggestion-text">${suggestion.text}</span>
                <span class="suggestion-type">${suggestion.type}</span>
            `;
        }
        
        return item;
    }

    // Función para actualizar display de sugerencias
    function updateSuggestionsDisplay(query) {
        if (!hasSearchDropdown) return;
        suggestionsList.innerHTML = '';
        
        if (!query || query.length < 1) {
            return;
        }

        filteredSuggestions = filterSuggestions(query);
        
        filteredSuggestions.forEach((suggestion, index) => {
            const item = createSuggestionElement(suggestion);
            item.addEventListener('click', () => {
                selectSuggestion(suggestion.text);
            });
            suggestionsList.appendChild(item);
        });
    }

    // Función para actualizar display de historial
    function updateHistoryDisplay() {
        if (!hasSearchDropdown) return;
        historyList.innerHTML = '';
        
        searchHistory.slice(0, 5).forEach(term => {
            const item = createSuggestionElement(term, true);
            
            // Click en el término
            item.addEventListener('click', (e) => {
                if (!e.target.classList.contains('history-remove')) {
                    selectSuggestion(term);
                }
            });
            
            // Click en el botón de remover
            const removeBtn = item.querySelector('.history-remove');
            removeBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                removeFromHistory(term);
            });
            
            historyList.appendChild(item);
        });
    }

    // Función para seleccionar una sugerencia
    function selectSuggestion(text) {
        searchInput.value = text;
        hideSuggestions();
        // Disparar búsqueda automáticamente
        searchForm.dispatchEvent(new Event('submit'));
    }

    // Función para mostrar sugerencias
    function showSuggestions() {
        if (!hasSearchDropdown) return;
        updateHistoryDisplay();
        suggestionsDropdown.classList.add('active');
    }

    // Función para ocultar sugerencias
    function hideSuggestions() {
        if (!hasSearchDropdown) return;
        suggestionsDropdown.classList.remove('active');
        currentSuggestionIndex = -1;
        clearHighlight();
    }

    // Función para limpiar resaltado
    function clearHighlight() {
        if (!hasSearchDropdown) return;
        const highlighted = suggestionsDropdown.querySelectorAll('.suggestion-item.highlighted');
        highlighted.forEach(item => item.classList.remove('highlighted'));
    }

    // Función para resaltar sugerencia
    function highlightSuggestion(index) {
        if (!hasSearchDropdown) return;
        clearHighlight();
        const allItems = suggestionsDropdown.querySelectorAll('.suggestion-item');
        if (allItems[index]) {
            allItems[index].classList.add('highlighted');
            allItems[index].scrollIntoView({ block: 'nearest' });
        }
    }

    // Función para navegar con teclado
    function navigateWithKeyboard(direction) {
        if (!hasSearchDropdown) return;
        const allItems = suggestionsDropdown.querySelectorAll('.suggestion-item');
        const totalItems = allItems.length;
        
        if (totalItems === 0) return;
        
        if (direction === 'down') {
            currentSuggestionIndex = (currentSuggestionIndex + 1) % totalItems;
        } else if (direction === 'up') {
            currentSuggestionIndex = currentSuggestionIndex <= 0 ? totalItems - 1 : currentSuggestionIndex - 1;
        }
        
        highlightSuggestion(currentSuggestionIndex);
    }

    // Función para seleccionar sugerencia resaltada
    function selectHighlightedSuggestion() {
        if (!hasSearchDropdown) return;
        const highlighted = suggestionsDropdown.querySelector('.suggestion-item.highlighted');
        if (highlighted) {
            const text = highlighted.querySelector('.suggestion-text').textContent;
            selectSuggestion(text);
        }
    }

    // Event listeners para el input de búsqueda
    if (searchInput && hasSearchDropdown) {
        searchInput.addEventListener('focus', () => {
            showSuggestions();
        });

        searchInput.addEventListener('input', (e) => {
            const query = e.target.value.trim();
            
            // Debounce para evitar demasiadas actualizaciones
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(() => {
                updateSuggestionsDisplay(query);
                if (query.length > 0) {
                    showSuggestions();
                }
            }, 150);
        });

        searchInput.addEventListener('keydown', (e) => {
            if (!suggestionsDropdown.classList.contains('active')) return;
            
            switch (e.key) {
                case 'ArrowDown':
                    e.preventDefault();
                    navigateWithKeyboard('down');
                    break;
                case 'ArrowUp':
                    e.preventDefault();
                    navigateWithKeyboard('up');
                    break;
                case 'Enter':
                    if (currentSuggestionIndex >= 0) {
                        e.preventDefault();
                        selectHighlightedSuggestion();
                    }
                    break;
                case 'Escape':
                    hideSuggestions();
                    searchInput.blur();
                    break;
            }
        });
    }

    // Event listener para el botón de limpiar todo el historial
    const clearAllHistoryBtn = document.getElementById('clear-all-history');
    if (clearAllHistoryBtn) {
        clearAllHistoryBtn.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            
            // Limpiar historial directamente
            clearAllHistory();
        });
    }

    // Ocultar sugerencias al hacer click fuera
    if (hasSearchDropdown) {
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.search-container')) {
                hideSuggestions();
            }
        });
    }

    // Configuración del botón flotante "volver arriba"
    const backToTopBtn = document.getElementById('back-to-top-float');
    if (backToTopBtn) {
        backToTopBtn.addEventListener('click', (e) => {
            e.preventDefault();
            window.scrollTo({ top: 0, behavior: 'smooth' });
            backToTopBtn.classList.remove('visible');
            backToTopForceVisibleUntil = 0; // cancelar estado forzado
        });

        // Mostrar el botón al alcanzar "Recomendados por interés"
        const interestProductsTitle = document.getElementById('interest-products-index-title');
        const interestProductsSection = interestProductsTitle ? interestProductsTitle.closest('.interest-products.searchable-section') : null;
        if (interestProductsSection) {
            const interestObserver = new IntersectionObserver((entries) => {
                entries.forEach((entry) => {
                    if (entry.isIntersecting) {
                        backToTopBtn.classList.add('visible');
                    }
                });
            }, { threshold: 0.4, rootMargin: '0px 0px -30% 0px' });
            interestObserver.observe(interestProductsSection);

            // Estado inicial por si ya estamos en la sección
            const y = window.scrollY;
            const top = interestProductsSection.offsetTop;
            const bottom = top + interestProductsSection.offsetHeight;
            if (y + window.innerHeight * 0.4 >= top && y <= bottom) {
                backToTopBtn.classList.add('visible');
            }
        }
    }
    // Mostrar/ocultar el botón según scroll
    window.addEventListener('scroll', () => {
        if (!backToTopBtn) return;
        const isMobile = window.matchMedia('(max-width: 768px)').matches;

        // Ocultar cerca del tope siempre
        if (window.scrollY < 120) {
            backToTopBtn.classList.remove('visible');
            return;
        }

        // Si está forzado visible por interacción reciente, mantener visible
        if (backToTopForceVisibleUntil > Date.now()) {
            backToTopBtn.classList.add('visible');
            return;
        }

        if (isMobile) {
            const bottomDistance = document.documentElement.scrollHeight - (window.scrollY + window.innerHeight);
            // Mostrar automáticamente cuando está a ~600px del final del contenido en móviles
            if (bottomDistance < 600) {
                backToTopBtn.classList.add('visible');
            }
            // No ocultar si no está cerca del final, para respetar estados previos (p. ej., clic en círculos)
        } else {
            // Escritorio: mostrar cuando se alcance el 80% del documento (progreso de lectura)
            const docEl = document.documentElement;
            const scrolledBottom = window.scrollY + window.innerHeight;
            const progress = scrolledBottom / docEl.scrollHeight;

            if (progress >= 0.8) {
                backToTopBtn.classList.add('visible');
            } else {
                // Histeresis suave: si se baja bastante, ocultar
                if (progress < 0.75) {
                    backToTopBtn.classList.remove('visible');
                }
            }
        }
    });

    // Modificar el event listener del formulario existente para agregar al historial
    const originalSubmitHandler = searchForm.onsubmit;
    searchForm.addEventListener('submit', function(e) {
        const searchTerm = searchInput.value.trim();
        const skipHistory = searchForm?.dataset?.skipHistory === 'true';
        if (searchTerm) {
            if (!skipHistory) {
                addToHistory(searchTerm);
            }
            hideSuggestions();
        }
        // Limpiar flag para próximos envíos
        if (skipHistory) {
            delete searchForm.dataset.skipHistory;
        }
    });

    // Inicializar display de historial (solo si existe UI)
    updateHistoryDisplay();
    
    // Configurar observer de visibilidad para el carrusel (solo si existe)
    if (document.querySelector('.carousel-container')) {
        setupVisibilityObserver();
    }
    
    // Inicializar banda de intereses solo si existe en la página
    const hasInterestSection = !!document.getElementById('interest-index') || !!document.querySelector('.interest-products');
    if (hasInterestSection) {
        // Inicializar la sección de intereses (círculos)
        initInterestStrip();
        // Inicializar flechas de navegación para la sección de intereses (móviles)
        initInterestNav();
        // Oscurecer banda de intereses cuando se centra en pantalla
        initInterestFocusState();

        // Formatear etiquetas de intereses para móviles (una palabra por línea)
        formatInterestLabelsForMobile();

        // Reaplicar formato al cambiar tamaño de ventana
        window.addEventListener('resize', () => {
            // Pequeño debounce para evitar llamadas excesivas
            clearTimeout(window.__formatLabelsResizeTimer);
            window.__formatLabelsResizeTimer = setTimeout(() => {
                formatInterestLabelsForMobile();
            }, 150);
        });
    }

    // Configuración del botón flotante "volver a destacados" (sector Gastronomía)
    const backToFeaturedBtn = document.getElementById('back-to-featured-float');
    if (backToFeaturedBtn && PAGE === 'gastronomia') {
        backToFeaturedBtn.addEventListener('click', (e) => {
            e.preventDefault();
            window.scrollTo({ top: 0, behavior: 'smooth' });
            backToFeaturedBtn.classList.remove('visible');
        });

        const interestProductsTitle = document.getElementById('interest-products-index-title');
        const interestProductsSection = interestProductsTitle ? interestProductsTitle.closest('.interest-products.searchable-section') : null;
        if (interestProductsSection) {
            // Mostrar cuando la sección "Recomendados por interés" esté en pantalla de forma estable
            const featuredObserver = new IntersectionObserver((entries) => {
                entries.forEach((entry) => {
                    if (entry.isIntersecting) {
                        backToFeaturedBtn.classList.add('visible');
                    } else {
                        backToFeaturedBtn.classList.remove('visible');
                    }
                });
            }, { threshold: 0.25, rootMargin: '0px' });
            featuredObserver.observe(interestProductsSection);

            // Fallback móvil/resize: calcular visibilidad por bounding rect
            const updateFeaturedButtonVisibility = () => {
                const rect = interestProductsSection.getBoundingClientRect();
                const viewportHeight = window.innerHeight || document.documentElement.clientHeight;
                const visibleHeight = Math.min(viewportHeight, rect.bottom) - Math.max(0, rect.top);
                const ratio = Math.max(0, visibleHeight) / Math.max(1, rect.height);
                if (ratio > 0.25) {
                    backToFeaturedBtn.classList.add('visible');
                } else {
                    backToFeaturedBtn.classList.remove('visible');
                }
            };
            updateFeaturedButtonVisibility();
            window.addEventListener('scroll', updateFeaturedButtonVisibility, { passive: true });
            window.addEventListener('resize', updateFeaturedButtonVisibility);
        }
    }



    // Hacer funciones globales para acceso desde HTML
    window.toggleAutoPlay = toggleAutoPlay;
    window.previousSlide = previousSlide;
    window.nextSlide = nextSlide;
    window.goToSlide = goToSlide;
    window.scrollDiscounts = scrollDiscounts;

    // Nota: funciones accesibles vía window para compatibilidad con onclick en HTML

    // =============================
    // Render dinámico Gastronomía
    // =============================
    if (PAGE === 'gastronomia') {
        function prettyLabelForCategory(cat) {
            const map = {
                'todos': 'Todos',
                'sandwich': 'Sándwich',
                'pizzas': 'Pizzas',
                'comen-dos': 'Comen dos',
                'al-plato': 'Al plato',
                'bebidas-cocteles': 'Bebidas',
                'postres': 'Postres',
            };
            return map[cat] || (cat.charAt(0).toUpperCase() + cat.slice(1));
        }

        async function renderCatalogFromBusinessConfig() {
            const cfg = window.BusinessConfig;
            const grid = document.querySelector('#menu-gastronomia .products-grid');
            if (!cfg || !Array.isArray(cfg.catalog) || !grid) return;

            // No sobreescribir el HTML estático si el catálogo está vacío
            const catalog = cfg.catalog.filter(p => p && typeof p === 'object');
            if (catalog.length === 0) {
                console.info('BusinessConfig: catálogo vacío, se mantiene el menú estático');
                return;
            }

            // Si ningún producto tiene imagen definida, mantener contenido estático
            const anyImagePresent = catalog.some(p => (p.imageSrc || p.image || '').trim() !== '');
            if (!anyImagePresent) {
                console.info('BusinessConfig: productos sin imagen, se mantiene el menú estático');
                return;
            }

            // Overrides desde API de productos
            let overrides = {};
            try {
                const origin = window.location.origin || '';
                const base = /^file:/i.test(origin) ? 'http://127.0.0.1:8000' : origin;
                const slug = (document.body && document.body.dataset && (document.body.dataset.tenant || document.body.dataset.slug)) || getBusinessSlug() || '';
                if (slug) {
                    const u = new URL('/api/products', base);
                    u.searchParams.set('tenant_slug', slug);
                    const resp = await fetch(u.toString(), { credentials: 'include' });
                    if (resp.ok) {
                        const json = await resp.json();
                        const arr = Array.isArray(json.products) ? json.products : [];
                        arr.forEach(p => { overrides[p.id] = p; });
                    }
                }
            } catch (_) {}

            const parts = catalog.map((p, idx) => {
                const id = p.id || `prod-${idx+1}`;
                const cats = (p.categories || []).join(', ');
                const ov = overrides[id] || {};
                if (ov && ov.active === false) {
                    return '';
                }
                const price = isFinite(parseInt(ov.price)) ? parseInt(ov.price) : parseInt(p.price);
                const stockValRaw = ov && typeof ov.stock !== 'undefined' ? ov.stock : undefined;
                const stockVal = isFinite(parseInt(stockValRaw)) ? parseInt(stockValRaw) : undefined;
                const badgeHtml = (typeof stockVal !== 'undefined') ? (stockVal <= 0 ? '<span class="stock-badge out">Sin stock</span>' : (stockVal <= 5 ? '<span class="stock-badge low">Últimas unidades</span>' : '')) : '';
                const btnDisabledAttr = (typeof stockVal !== 'undefined' && stockVal <= 0) ? 'disabled' : '';
                const priceText = isFinite(price) ? `$${price.toLocaleString('es-AR')} ARS` : (p.priceText || '');
                // Soportar claves imageSrc o image, y normalizar espacios
                const rawImg = p.imageSrc || p.image || '';
                const normalizedImg = rawImg ? rawImg.replace(/\s/g, '%20') : '';
                // Si no hay imagen, usar una segura para evitar espacios en blanco
                const imgSrc = normalizedImg || 'Imagenes/asus-proart-p16.png';
                const imgAlt = p.imageAlt || p.name || '';
                const desc = (typeof ov.details === 'string' && ov.details) ? ov.details : (p.description || '');
                const name = (typeof ov.name === 'string' && ov.name.trim()) ? ov.name : (p.name || `Producto ${idx+1}`);
                return `
                <div class="product-card searchable-item" id="${id}" data-food-category="${cats}">
                    <div class="product-image">
                        <img src="${imgSrc}" alt="${imgAlt}" width="800" height="600" loading="lazy">
                    </div>
                    <div class="product-info">
                        <h3>${name}</h3>
                        ${desc ? `<p class=\"product-description\">${desc}</p>` : ''}
                        ${badgeHtml}
                        ${priceText ? `<p class=\"product-price\">${priceText}</p>` : ''}
                        <button class="add-to-cart-btn" ${btnDisabledAttr} data-id="${id}" data-name="${name}" data-price="${isFinite(price) ? price : 0}">Añadir al carrito</button>
                </div>
                </div>`;
            });

            // Evitar limpiar si no hay productos renderizados
            const html = parts.join('\n');
            if (!html.trim()) {
                console.info('BusinessConfig: sin productos válidos, se mantiene el menú estático');
                return;
            }

            grid.innerHTML = html;
            // Refrescar buscables y botones
            refreshSearchableItems();
            bindAddToCartEvents(grid);

            // Segunda pasada: garantizar overrides desde API sobre el DOM final
            try {
                const origin = window.location.origin || '';
                const base = /^file:/i.test(origin) ? 'http://127.0.0.1:8000' : origin;
                const slug = (document.body && document.body.dataset && (document.body.dataset.tenant || document.body.dataset.slug)) || getBusinessSlug() || '';
                if (slug) {
                    const u = new URL('/api/products', base);
                    u.searchParams.set('tenant_slug', slug);
                    const resp = await fetch(u.toString(), { credentials: 'include' });
                    if (resp.ok) {
                        const json = await resp.json();
                        const arr = Array.isArray(json.products) ? json.products : [];
                        const map = {};
                        arr.forEach(p => { map[p.id] = p; });
                const cards = grid.querySelectorAll('.product-card');
                cards.forEach(card => {
                    let id = card.getAttribute('id') || '';
                    const btn = card.querySelector('.add-to-cart-btn');
                    let ov = map[id];
                    if (!ov && btn) {
                        const bid = btn.getAttribute('data-id') || '';
                        ov = map[bid];
                    }
                    if (!ov && /^destacado(\d+)/.test(id)) {
                        try {
                            const n = id.match(/^destacado(\d+)/)[1];
                            ov = map[`dest${n}`];
                        } catch (_) {}
                    }
                    if (!ov) return;
                    if (ov.active === false) { card.style.display = 'none'; return; }
                    const h = card.querySelector('.product-info h3');
                    if (h && typeof ov.name === 'string') h.textContent = String(ov.name || '');
                    const desc = card.querySelector('.product-description');
                    if (desc && typeof ov.details === 'string') desc.textContent = ov.details;
                    const pr = card.querySelector('.product-price');
                    const priceVal = isFinite(parseInt(ov.price)) ? parseInt(ov.price) : 0;
                    if (pr) pr.textContent = `$${priceVal.toLocaleString('es-AR')} ARS`;
                    if (btn) {
                        btn.setAttribute('data-price', String(priceVal));
                        btn.setAttribute('data-name', String(ov.name || ''));
                        if (isFinite(parseInt(ov.stock)) && parseInt(ov.stock) <= 0) { btn.setAttribute('disabled',''); } else { btn.removeAttribute('disabled'); }
                    }
                    const info = card.querySelector('.product-info');
                    if (info && isFinite(parseInt(ov.stock))) {
                        let badge = card.querySelector('.stock-badge');
                        const sval = parseInt(ov.stock);
                        const txt = sval <= 0 ? 'Sin stock' : (sval <= 5 ? 'Últimas unidades' : '');
                        const cls = sval <= 0 ? 'stock-badge out' : (sval <= 5 ? 'stock-badge low' : '');
                        if (txt) {
                            if (!badge) {
                                badge = document.createElement('span');
                                badge.className = cls;
                                badge.textContent = txt;
                                const priceEl = card.querySelector('.product-price');
                                if (priceEl && priceEl.parentElement === info) info.insertBefore(badge, priceEl);
                                else info.appendChild(badge);
                            } else {
                                badge.className = cls;
                                badge.textContent = txt;
                                badge.style.display = '';
                            }
                        } else if (badge) {
                            badge.style.display = 'none';
                        }
                    }
                });
                    }
                }
            } catch (_) {}
        }

        function renderFiltersFromBusinessConfig() {
            const cfg = window.BusinessConfig;
            const filterEl = document.getElementById('category-filter');
            if (!cfg || !cfg.filters || !Array.isArray(cfg.filters.categories) || !filterEl) return;

            const cats = cfg.filters.categories;
            const html = ['todos', ...cats].map((cat, idx) => {
                const label = prettyLabelForCategory(cat);
                const activeCls = idx === 0 ? 'active' : '';
                return `<button class="filter-btn ${activeCls}" data-filter="${cat}">${label}</button>`;
            }).join('\n');
            filterEl.innerHTML = html;

            // Enlazar lógica de filtros
            let selectedCategory = 'todos';
            function itemMatchesSelectedCategory(item) {
                const catAttr = (item.getAttribute('data-food-category') || '').toLowerCase();
                const categories = catAttr.split(',').map(c => c.trim());
                if (selectedCategory === 'todos') return true;
                if (selectedCategory === 'bebidas-cocteles') return categories.includes('bebidas') || categories.includes('cocteles');
                if (selectedCategory === 'al-plato') return !categories.includes('bebidas') && !categories.includes('cocteles');
                return categories.includes(selectedCategory);
            }
            function applyCategoryFilter() {
                const menuSection = document.getElementById('menu-gastronomia');
                const items = document.querySelectorAll('.searchable-item');
                items.forEach(item => {
                    const isInMenuSection = menuSection && menuSection.contains(item);
                    if (!isInMenuSection) {
                        item.style.display = '';
                        return;
                    }
                    item.style.display = itemMatchesSelectedCategory(item) ? '' : 'none';
                });
            }
            const filterButtons = filterEl.querySelectorAll('.filter-btn');
            filterButtons.forEach(btn => {
                btn.addEventListener('click', () => {
                    filterButtons.forEach(b => b.classList.remove('active'));
                    btn.classList.add('active');
                    selectedCategory = btn.getAttribute('data-filter') || 'todos';
                    applyCategoryFilter();

                    // Si hay búsqueda activa, volver a ejecutarla con el filtro
                    const term = (searchInput && searchInput.value || '').trim().toLowerCase();
                    const searchResultsSection = document.querySelector('.search-results');
                    if (term !== '' && searchResultsSection && searchResultsSection.classList.contains('active')) {
                        const menuSection = document.getElementById('menu-gastronomia');
                        const interestSection = document.querySelector('.interest-products');
                        const allItems = Array.from(document.querySelectorAll('.searchable-item'));
                        const menuItemsForSearch = allItems.filter(item => {
                            const isInMenuSection = menuSection && menuSection.contains(item);
                            return isInMenuSection && itemMatchesSelectedCategory(item);
                        });
                        const interestItemsForSearch = allItems.filter(item => {
                            const isInInterestSection = interestSection && interestSection.contains(item);
                            return isInInterestSection;
                        });
                        const filteredItemsForSearch = [...menuItemsForSearch, ...interestItemsForSearch];
                        const results = performSearch(term, filteredItemsForSearch);
                        displayResults(results, term, resultsContainer);
                    }
                });
            });
            // Aplicar filtro inicial
            applyCategoryFilter();
        }

        async function renderGastronomyFromBusinessConfig() {
            await renderCatalogFromBusinessConfig();
            renderFiltersFromBusinessConfig();
            // Re-inicializar loaders para imágenes recién renderizadas
            try {
                if (window.__reinitMediaLoaders) window.__reinitMediaLoaders(document.getElementById('menu-gastronomia'));
                if (window.__reinitImageLoader) window.__reinitImageLoader(document.getElementById('menu-gastronomia'));
                // Re-vincular clics del modal para nuevas tarjetas
                bindProductCardClicks(document.getElementById('menu-gastronomia'));
            } catch (e) {
                // silencioso
            }
            await applyOverridesToDocumentProducts();
        }

        // Render al estar lista la BusinessConfig
        document.addEventListener('businessconfig:ready', renderGastronomyFromBusinessConfig);
        // Si ya está cargada, render inmediato
        if (window.BusinessConfig && window.BusinessConfig.__loaded) {
            renderGastronomyFromBusinessConfig();
        }

        async function applyOverridesToDocumentProducts() {
            try {
                const origin = window.location.origin || '';
                const base = /^file:/i.test(origin) ? 'http://127.0.0.1:8000' : origin;
                const slug = (document.body && document.body.dataset && (document.body.dataset.tenant || document.body.dataset.slug)) || getBusinessSlug() || '';
                if (!slug) return;
                const u = new URL('/api/products', base);
                u.searchParams.set('tenant_slug', slug);
                const resp = await fetch(u.toString(), { credentials: 'include' });
                if (!resp.ok) return;
                const json = await resp.json();
                const arr = Array.isArray(json.products) ? json.products : [];
                const map = {};
                arr.forEach(p => { map[p.id] = p; });
                const cards = document.querySelectorAll('.product-card');
                cards.forEach(card => {
                    let id = card.getAttribute('id') || '';
                    const btn = card.querySelector('.add-to-cart-btn');
                    let ov = map[id];
                    if (!ov && btn) {
                        const bid = btn.getAttribute('data-id') || '';
                        ov = map[bid];
                    }
                    if (!ov && /^destacado(\d+)/.test(id)) {
                        try {
                            const n = id.match(/^destacado(\d+)/)[1];
                            ov = map[`dest${n}`];
                        } catch (_) {}
                    }
                    if (!ov) return;
                    if (ov.active === false) { card.style.display = 'none'; return; }
                    const h = card.querySelector('.product-info h3');
                    if (h && typeof ov.name === 'string') h.textContent = String(ov.name || '');
                    const desc = card.querySelector('.product-description');
                    if (desc && typeof ov.details === 'string') desc.textContent = ov.details;
                    const pr = card.querySelector('.product-price');
                    const priceVal = isFinite(parseInt(ov.price)) ? parseInt(ov.price) : 0;
                    if (pr) pr.textContent = `$${priceVal.toLocaleString('es-AR')} ARS`;
                    if (btn) {
                        btn.setAttribute('data-price', String(priceVal));
                        btn.setAttribute('data-name', String(ov.name || ''));
                        // Stock check for button
                        if (typeof ov.stock !== 'undefined') {
                            const sval = parseInt(ov.stock);
                            if (isFinite(sval) && sval <= 0) { btn.setAttribute('disabled',''); } else { btn.removeAttribute('disabled'); }
                        }
                    }

                    const info = card.querySelector('.product-info');
                    if (info && typeof ov.stock !== 'undefined') {
                        const sval = parseInt(ov.stock);
                        if (isFinite(sval)) {
                             let badge = card.querySelector('.stock-badge');
                             const txt = sval <= 0 ? 'Sin stock' : (sval <= 5 ? 'Últimas unidades' : '');
                             const cls = sval <= 0 ? 'stock-badge out' : (sval <= 5 ? 'stock-badge low' : '');
                             
                             if (txt) {
                                 if (!badge) {
                                     badge = document.createElement('span');
                                 }
                                 badge.className = cls;
                                 badge.textContent = txt;
                                 badge.style.display = '';

                                 const priceEl = card.querySelector('.product-price');
                                 const priceContainer = card.querySelector('.price-container');

                                 if (priceContainer && priceContainer.parentElement === info) {
                                     info.insertBefore(badge, priceContainer);
                                 } else if (priceEl && priceEl.parentElement === info) {
                                     info.insertBefore(badge, priceEl);
                                 } else {
                                     if (badge.parentElement !== info) info.appendChild(badge);
                                 }
                             } else if (badge) {
                                 badge.style.display = 'none';
                             }
                        }
                    }
                });
            } catch (_) {
            } finally {
                try { document.documentElement.classList.remove('hydrating'); } catch (e) {}
            }
        }
        document.addEventListener('DOMContentLoaded', () => { applyOverridesToDocumentProducts(); });
    }
    
    // =============================
    // Render dinámico Comercio
    // =============================
    if (PAGE === 'comercio') {
        function prettyLabelForTag(tag) {
            const map = {
                'todos': 'Todos',
                'destacados': 'Destacados',
                'liquidaciones': 'Liquidaciones',
                'más vendidos': 'Más vendidos',
                'mas vendidos': 'Más vendidos',
                'nuevo': 'Nuevo'
            };
            const key = (tag || '').toLowerCase();
            return map[key] || (tag.charAt(0).toUpperCase() + tag.slice(1));
        }

        function renderCommerceCatalogFromBusinessConfig() {
            const cfg = window.BusinessConfig;
            const grid = document.querySelector('#menu-electronica .products-grid');
            if (!cfg || !Array.isArray(cfg.catalog) || !grid) return;

            // No sobreescribir si el catálogo está vacío
            const catalog = cfg.catalog.filter(p => p && typeof p === 'object');
            if (catalog.length === 0) {
                console.info('BusinessConfig: catálogo vacío (comercio), se mantiene el contenido estático');
                return;
            }

            const anyImagePresent = catalog.some(p => (p.imageSrc || p.image || '').trim() !== '');
            if (!anyImagePresent) {
                console.info('BusinessConfig: productos sin imagen (comercio), se mantiene el contenido estático');
                return;
            }

            const parts = catalog.map((p, idx) => {
                const id = p.id || `prod-${idx+1}`;
                const tagsAttr = (p.tags || []).join(', ');
                const price = parseInt(p.price);
                const priceText = isFinite(price) ? `$${price.toLocaleString('es-AR')} ARS` : (p.priceText || '');
                const rawImg = p.imageSrc || p.image || '';
                const normalizedImg = rawImg ? rawImg.replace(/\s/g, '%20') : '';
                const imgSrc = normalizedImg || 'Imagenes/asus-proart-p16.png';
                const imgAlt = p.imageAlt || p.name || '';
                const desc = p.description || '';
                const name = p.name || `Producto ${idx+1}`;
                return `
                <div class="product-card searchable-item" id="${id}" data-product-category="${tagsAttr}">
                    <div class="product-image">
                        <img src="${imgSrc}" alt="${imgAlt}" width="800" height="600" loading="lazy">
                    </div>
                    <div class="product-info">
                        <h3>${name}</h3>
                        ${desc ? `<p class=\"product-description\">${desc}</p>` : ''}
                        ${priceText ? `<p class=\"product-price\">${priceText}</p>` : ''}
                        <button class="add-to-cart-btn" data-id="${id}" data-name="${name}" data-price="${isFinite(price) ? price : 0}">Añadir al carrito</button>
                    </div>
                </div>`;
            });

            const html = parts.join('\n');
            if (!html.trim()) {
                console.info('BusinessConfig: sin productos válidos (comercio), se mantiene el contenido estático');
                return;
            }

            grid.innerHTML = html;
            refreshSearchableItems();
            bindAddToCartEvents(grid);
        }

        function renderCommerceFiltersFromBusinessConfig() {
            const cfg = window.BusinessConfig;
            const filterEl = document.getElementById('index-category-filter');
            if (!cfg || !cfg.filters || !Array.isArray(cfg.filters.categories) || !filterEl) return;

            const cats = cfg.filters.categories;
            const html = ['todos', ...cats].map((cat, idx) => {
                const label = prettyLabelForTag(cat);
                const activeCls = idx === 0 ? 'active' : '';
                return `<button class="filter-btn ${activeCls}" data-filter="${cat}">${label}</button>`;
            }).join('\n');
            filterEl.innerHTML = html;

            let selectedCategory = 'todos';
            function itemMatchesSelectedCategory(item) {
                const catAttr = (item.getAttribute('data-product-category') || '').toLowerCase();
                const categories = catAttr.split(',').map(c => c.trim());
                if (selectedCategory === 'todos') return true;
                return categories.includes(selectedCategory.toLowerCase());
            }
            function applyCategoryFilter() {
                const menuSection = document.getElementById('menu-electronica');
                const items = document.querySelectorAll('.searchable-item');
                items.forEach(item => {
                    const isInMenuSection = menuSection && menuSection.contains(item);
                    if (!isInMenuSection) {
                        item.style.display = '';
                        return;
                    }
                    item.style.display = itemMatchesSelectedCategory(item) ? '' : 'none';
                });
            }

            const filterButtons = filterEl.querySelectorAll('.filter-btn');
            filterButtons.forEach(btn => {
                btn.addEventListener('click', () => {
                    filterButtons.forEach(b => b.classList.remove('active'));
                    btn.classList.add('active');
                    selectedCategory = btn.getAttribute('data-filter') || 'todos';
                    applyCategoryFilter();

                    const term = (searchInput && searchInput.value || '').trim().toLowerCase();
                    const searchResultsSection = document.querySelector('.search-results');
                    if (term !== '' && searchResultsSection && searchResultsSection.classList.contains('active')) {
                        const menuSection = document.getElementById('menu-electronica');
                        const interestSection = document.querySelector('.interest-products');
                        const allItems = Array.from(document.querySelectorAll('.searchable-item'));
                        const menuItemsForSearch = allItems.filter(item => {
                            const isInMenuSection = menuSection && menuSection.contains(item);
                            return isInMenuSection && itemMatchesSelectedCategory(item);
                        });
                        const interestItemsForSearch = allItems.filter(item => {
                            const isInInterestSection = interestSection && interestSection.contains(item);
                            return isInInterestSection;
                        });
                        const filteredItemsForSearch = [...menuItemsForSearch, ...interestItemsForSearch];
                        const results = performSearch(term, filteredItemsForSearch);
                        displayResults(results, term, resultsContainer);
                    }
                });
            });
            applyCategoryFilter();
        }

        function renderCommerceFromBusinessConfig() {
            renderCommerceCatalogFromBusinessConfig();
            renderCommerceFiltersFromBusinessConfig();
            try {
                if (window.__reinitMediaLoaders) window.__reinitMediaLoaders(document.getElementById('menu-electronica'));
                if (window.__reinitImageLoader) window.__reinitImageLoader(document.getElementById('menu-electronica'));
                // Vincular clics de tarjetas para abrir modal en comercio
                if (typeof bindProductCardClicks === 'function') {
                    const commerceSection = document.getElementById('menu-electronica');
                    if (commerceSection) {
                        bindProductCardClicks(commerceSection);
                    }
                }
            } catch (e) {
                // silencioso
            }
        }

        document.addEventListener('businessconfig:ready', renderCommerceFromBusinessConfig);
        if (window.BusinessConfig && window.BusinessConfig.__loaded) {
            renderCommerceFromBusinessConfig();
        }
    }
});

// Función para navegar en la sección de descuentos
function scrollDiscounts(direction) {
    const container = document.querySelector('.discounts-container');
    const scrollAmount = 300; // Cantidad de píxeles a desplazar
    
    if (direction === 'left') {
        container.scrollBy({
            left: -scrollAmount,
            behavior: 'smooth'
        });
    } else if (direction === 'right') {
        container.scrollBy({
            left: scrollAmount,
            behavior: 'smooth'
        });
    }
    
    // Actualizar estado de los botones después del scroll
    setTimeout(() => {
        updateDiscountNavButtons();
    }, 300);
}

// Formatear etiquetas de intereses en móviles: una palabra por línea
function formatInterestLabelsForMobile() {
    const section = document.getElementById('interest-index');
    if (!section) return;

    const labels = section.querySelectorAll('.interest-label');
    const isMobile = window.matchMedia('(max-width: 768px)').matches;

    labels.forEach(label => {
        const originalText = label.dataset.originalLabel || label.textContent.trim();

        // Guardar texto original una sola vez
        if (!label.dataset.originalLabel) {
            label.dataset.originalLabel = originalText;
        }

        if (isMobile) {
            // Mantener texto completo sin forzar saltos de palabra
            label.textContent = originalText;
        } else {
            // Restaurar texto original en pantallas grandes
            label.textContent = label.dataset.originalLabel || originalText;
        }
    });
}

// Función para actualizar el estado de los botones de navegación
function updateDiscountNavButtons() {
    const container = document.querySelector('.discounts-container');
    const prevBtn = document.querySelector('.discounts-nav-btn.prev');
    const nextBtn = document.querySelector('.discounts-nav-btn.next');
    
    if (!container || !prevBtn || !nextBtn) return;
    
    const isAtStart = container.scrollLeft <= 0;
    const isAtEnd = container.scrollLeft >= (container.scrollWidth - container.clientWidth - 1);
    
    prevBtn.disabled = isAtStart;
    nextBtn.disabled = isAtEnd;
}

// Inicializar estado de botones cuando se carga la página
document.addEventListener('DOMContentLoaded', () => {
    // Inicializar navegación de descuentos solo si la banda existe
    const container = document.querySelector('.discounts-container');
    if (!container) return;

    updateDiscountNavButtons();

    // Actualizar botones cuando se redimensiona la ventana
    window.addEventListener('resize', updateDiscountNavButtons);

    // Actualizar botones cuando se hace scroll manual
    container.addEventListener('scroll', updateDiscountNavButtons);

    // Inicializar auto-scroll para descuentos
    initDiscountAutoScroll();
});

// Variables para el auto-scroll
let discountAutoScrollInterval;
let isDiscountAutoScrollPaused = false;

// Función para inicializar el auto-scroll de descuentos
function initDiscountAutoScroll() {
    const container = document.querySelector('.discounts-container');
    const discountsWrapper = document.querySelector('.discounts-wrapper');
    
    if (!container || !discountsWrapper) return;
    // Respetar preferencias de reducción de movimiento
    const prefersReducedMotion = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    if (prefersReducedMotion) {
        // No iniciar auto-scroll si el usuario prefiere menos movimiento
        isDiscountAutoScrollPaused = true;
        return;
    }
    
    // Función para hacer auto-scroll
    function autoScrollDiscounts() {
        if (isDiscountAutoScrollPaused) return;
        
        const isAtEnd = container.scrollLeft >= (container.scrollWidth - container.clientWidth - 1);
        
        if (isAtEnd) {
            // Si llegamos al final, volver al inicio
            container.scrollTo({
                left: 0,
                behavior: 'smooth'
            });
        } else {
            // Continuar desplazándose hacia la derecha
            scrollDiscounts('right');
        }
    }
    
    // Iniciar el auto-scroll cada 5 segundos
    discountAutoScrollInterval = setInterval(autoScrollDiscounts, 5000);
    
    // Pausar auto-scroll al hacer hover sobre la sección
    discountsWrapper.addEventListener('mouseenter', () => {
        isDiscountAutoScrollPaused = true;
    });
    
    // Reanudar auto-scroll al salir del hover
    discountsWrapper.addEventListener('mouseleave', () => {
        isDiscountAutoScrollPaused = false;
    });
    
    // Pausar auto-scroll durante interacciones táctiles en móviles
    container.addEventListener('touchstart', () => {
        isDiscountAutoScrollPaused = true;
    });
    
    // Reanudar auto-scroll después de un tiempo sin interacción táctil
    let touchTimeout;
    container.addEventListener('touchend', () => {
        clearTimeout(touchTimeout);
        touchTimeout = setTimeout(() => {
            isDiscountAutoScrollPaused = false;
        }, 3000); // Reanudar después de 3 segundos sin tocar
    });
    
    // Pausar auto-scroll cuando se usan los botones de navegación
    const navButtons = document.querySelectorAll('.discounts-nav-btn');
    navButtons.forEach(button => {
        button.addEventListener('click', () => {
            isDiscountAutoScrollPaused = true;
            // Reanudar después de 5 segundos
            setTimeout(() => {
                isDiscountAutoScrollPaused = false;
            }, 5000);
        });
    });
}

// Función para detener el auto-scroll (útil si se necesita)
function stopDiscountAutoScroll() {
    if (discountAutoScrollInterval) {
        clearInterval(discountAutoScrollInterval);
        discountAutoScrollInterval = null;
    }
}

// Función para reanudar el auto-scroll
function resumeDiscountAutoScroll() {
    if (!discountAutoScrollInterval) {
        initDiscountAutoScroll();
    }
}

// Inicializar comportamiento para la sección de intereses
function getInterestProductMap() {
    const category = (document.body && document.body.dataset && document.body.dataset.category || '').toLowerCase();

    const baseMap = {
        '2x1': 'interest-product-2x1',
        'Liquidaciones': 'interest-product-liquidaciones',
        'Destacados': 'interest-product-destacados',
        'Nuevo': 'interest-product-nuevo',
        'Más vendidos': 'interest-product-mas-vendidos'
    };

    if (category === 'gastronomia') {
        return {
            '2x1': 'interest-product-2x1',
            'Liquidaciones': 'interest-product-liquidaciones',
            'Destacados': 'interest-product-destacados',
            'Nuevo': 'interest-product-nuevo',
            'Más vendidos': 'interest-product-mas-vendidos',
            'Entradas rápidas': 'interest-product-nuevo',
            'Promociones': 'interest-product-liquidaciones',
            'Especialidad de la casa': 'interest-product-destacados',
            'Combos': 'interest-product-mas-vendidos'
        };
    }

    return baseMap;
}

function initInterestStrip() {
    const items = document.querySelectorAll('.interest-item');
    const searchForm = document.getElementById('search-form');
    const searchInput = document.getElementById('search-input');
    const clearSearchBtn = document.getElementById('clear-search-btn');
    const backToTopBtn = document.getElementById('back-to-top-float');

    // Mapeo de círculos de interés a productos de ejemplo
    const interestProductMap = getInterestProductMap();

    if (!items.length || !searchForm || !searchInput) return;

    items.forEach(btn => {
        btn.addEventListener('click', () => {
            const term = (btn.getAttribute('data-term') || '').trim();
            if (!term) return;

            // Limpiar el campo de búsqueda para evitar confusiones
            searchInput.value = '';
            if (clearSearchBtn) clearSearchBtn.style.display = 'none';
            if (document.activeElement === searchInput) {
                searchInput.blur();
            }

            // No disparar búsqueda desde círculos; ocultar resultados y sugerencias
            const suggestionsDropdown = document.getElementById('search-suggestions-dropdown');
            if (suggestionsDropdown) {
                suggestionsDropdown.classList.remove('active');
            }
            const searchResultsSection = document.querySelector('.search-results');
            if (searchResultsSection) {
                searchResultsSection.classList.remove('active');
            }

            // Llevar la vista hacia la sección de productos
            const interestProductsSection = document.querySelector('.interest-products');
            const productsSection = document.querySelector('.products');
            (interestProductsSection || productsSection)?.scrollIntoView({ behavior: 'smooth', block: 'start' });

            // Desplazar y resaltar el producto de ejemplo asignado a este interés (sin agregar al carrito)
            const mappedId = interestProductMap[term];
            if (mappedId) {
                const productCard = document.getElementById(mappedId);
                if (productCard) {
                    productCard.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    productCard.classList.add('interest-highlight');
                    setTimeout(() => {
                        productCard.classList.remove('interest-highlight');
                    }, 1600);
                    // Mostrar botón flotante para volver al inicio
                    if (backToTopBtn) {
                        backToTopForceVisibleUntil = Date.now() + 8000;
                        backToTopBtn.classList.add('visible');
                        // Ocultar después de un tiempo si no se usa
                        setTimeout(() => {
                            backToTopForceVisibleUntil = 0;
                            backToTopBtn.classList.remove('visible');
                        }, 8000);
                    }
                }
            }
        });
    });
}

// Navegación con flechas para la sección de intereses en móviles
function initInterestNav() {
    const section = document.getElementById('interest-index');
    if (!section) return;

    const strip = section.querySelector('.interest-strip');
    const prevBtn = section.querySelector('.interest-nav-btn.prev');
    const nextBtn = section.querySelector('.interest-nav-btn.next');
    if (!strip || !prevBtn || !nextBtn) return;

    function isMobile() {
        return window.matchMedia('(max-width: 768px)').matches;
    }

    // Mostrar/ocultar flechas según ancho
    function syncVisibility() {
        const visible = isMobile();
        prevBtn.style.display = visible ? 'flex' : 'none';
        nextBtn.style.display = visible ? 'flex' : 'none';
        updateState();
    }

    // Actualizar estado de botones (disabled al inicio/fin)
    function updateState() {
        const atStart = strip.scrollLeft <= 1;
        const atEnd = (strip.scrollLeft + strip.clientWidth) >= (strip.scrollWidth - 1);
        prevBtn.disabled = atStart;
        nextBtn.disabled = atEnd;
        prevBtn.classList.toggle('disabled', atStart);
        nextBtn.classList.toggle('disabled', atEnd);
        // Mostrar fades según contenido disponible
        section.classList.toggle('has-left', !atStart);
        section.classList.toggle('has-right', !atEnd);
    }

    // Al presionar, intentar mostrar todos los elementos no visibles
    function scrollInterest(direction) {
        if (direction === 'right') {
            const remainingRight = strip.scrollWidth - (strip.scrollLeft + strip.clientWidth);
            const amount = Math.max(strip.clientWidth, remainingRight);
            strip.scrollTo({ left: strip.scrollLeft + amount, behavior: 'smooth' });
        } else {
            const remainingLeft = strip.scrollLeft;
            const amount = Math.max(strip.clientWidth, remainingLeft);
            strip.scrollTo({ left: Math.max(0, strip.scrollLeft - amount), behavior: 'smooth' });
        }
        setTimeout(updateState, 320);
    }

    prevBtn.addEventListener('click', () => scrollInterest('left'));
    nextBtn.addEventListener('click', () => scrollInterest('right'));

    strip.addEventListener('scroll', updateState);

    // Debounce para resize
    let resizeTimer;
    window.addEventListener('resize', () => {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(syncVisibility, 150);
    });

    // Inicializar visibilidad y estado
    syncVisibility();
}

// Enfoque visual para la sección de intereses: oscurecer al centrarse en viewport
function initInterestFocusState() {
    const interestSection = document.getElementById('interest-index');
    if (!interestSection) return;

    let prevIntensity = 0;
    const clamp01 = (v) => Math.min(1, Math.max(0, v));
    const easeOutCubic = (t) => 1 - Math.pow(1 - t, 3);

    const updateInterestFocusState = () => {
        const rect = interestSection.getBoundingClientRect();
        const viewportCenterY = window.innerHeight / 2;
        const sectionCenterY = rect.top + rect.height / 2;
        const distance = Math.abs(sectionCenterY - viewportCenterY);
        const visible = rect.top < window.innerHeight && rect.bottom > 0;

        // Arranque más temprano y suavizado: de startThreshold (inicio) a fullThreshold (centro)
        const startThreshold = Math.min(window.innerHeight * 0.45, 320);
        const fullThreshold  = Math.min(window.innerHeight * 0.18, 140);

        let rawIntensity = 0;
        if (visible) {
            if (distance <= fullThreshold) {
                rawIntensity = 1;
            } else if (distance >= startThreshold) {
                rawIntensity = 0;
            } else {
                // Mapea linealmente entre startThreshold y fullThreshold
                rawIntensity = 1 - ((distance - fullThreshold) / (startThreshold - fullThreshold));
            }
        }

        // Suavizado con ease-out y pequeño blending para evitar saltos
        const eased = easeOutCubic(clamp01(rawIntensity));
        const blended = prevIntensity + (eased - prevIntensity) * 0.25;

        // Actualiza variables CSS (opacidades de overlay)
        interestSection.style.setProperty('--focus-linear', (0.26 * blended).toFixed(3));
        interestSection.style.setProperty('--focus-radial', (0.16 * blended).toFixed(3));

        // Box-shadow sutil cuando hay algo de intensidad
        interestSection.classList.toggle('focused', blended > 0.08);
        prevIntensity = blended;
    };

    let tickingFocus = false;
    const onScrollOrResize = () => {
        if (!tickingFocus) {
            tickingFocus = true;
            requestAnimationFrame(() => {
                updateInterestFocusState();
                tickingFocus = false;
            });
        }
    };

    window.addEventListener('scroll', onScrollOrResize, { passive: true });
    window.addEventListener('resize', onScrollOrResize);
    // Evaluación inicial
    updateInterestFocusState();
}

// ==========================================
// SISTEMA DE ESTADO DE PEDIDOS (GASTRONOMÍA)
// ==========================================
document.addEventListener('DOMContentLoaded', function() {
    const statusBtn = document.getElementById('order-status-float');
    const statusModal = document.getElementById('order-status-modal');
    const closeBtn = document.querySelector('.close-status-modal');
    const statusBody = document.getElementById('order-status-body');

    if (!statusBtn || !statusModal) return;

    let pollingInterval;
    let currentConfig = {};

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
                } else {
                    btn.innerHTML = '<i class="fab fa-whatsapp"></i> Consultar por este pedido';
                    if (btn.dataset.chatUrl) btn.href = btn.dataset.chatUrl;
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

    async function updateStatusSilently() {
        const orderId = localStorage.getItem('last_order_id');
        if (!orderId) return;

        try {
            // Reutilizamos getOrderData existente
            const data = await getOrderData(orderId);
            
            if (data && data.order) {
                // Si el modal está abierto, asumimos que el usuario vio el nuevo estado
                localStorage.setItem('last_viewed_status', data.order.status);
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

    // Función para cerrar el modal
    function closeStatusModal() {
        stopPolling();
        if (statusActionInterval) { clearInterval(statusActionInterval); statusActionInterval = null; }
        statusModal.classList.remove('active');
        statusModal.style.display = 'none';
        statusModal.setAttribute('aria-hidden', 'true');
        document.body.style.overflow = '';
    }

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

    // Función principal para consultar estado (UI Modal)
    async function fetchOrderStatus() {
        const orderId = localStorage.getItem('last_order_id');
        
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
            const slug = window.BUSINESS_SLUG || 'gastronomia-local1';
            const [orderData, configData] = await Promise.all([
                getOrderData(orderId),
                fetch(`${base}/api/config?slug=${slug}`).then(r => r.json()).catch(() => ({}))
            ]);
            
            currentConfig = configData || {};

            if (orderData && orderData.order) {
                // Actualizar estado visto al abrir modal
                localStorage.setItem('last_viewed_status', orderData.order.status);
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

    // Nueva función auxiliar para obtener datos sin renderizar
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

    // Sistema de Notificaciones en Segundo Plano
    function showNotificationBadge() {
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
        const badge = statusBtn.querySelector('.notification-badge');
        if (badge) badge.remove();
        statusBtn.classList.remove('has-notification');
    }

    async function checkBackgroundStatus() {
        const orderId = localStorage.getItem('last_order_id');
        if (!orderId) return;

        try {
            const data = await getOrderData(orderId);
            if (data && data.order) {
                const currentStatus = data.order.status;
                const lastViewed = localStorage.getItem('last_viewed_status');

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

    // Iniciar polling (cada 30 segundos)
    setInterval(checkBackgroundStatus, 30000);
    // Chequeo inicial rápido (1 seg después de carga)
    setTimeout(checkBackgroundStatus, 1000);

    function renderLoading() {
        if (!statusBody) return;
        statusBody.innerHTML = `
            <div class="status-loading">
                <i class="fas fa-spinner fa-spin"></i> Verificando estado...
            </div>
        `;
    }

    function renderStatusError(msg) {
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
        // Note: statusActionInterval is now managed globally in startPolling/stopPolling
        if (!statusBody) return;
        
        if (order.status === 'entregado' && !celebrationShown) {
            triggerConfetti();
            celebrationShown = true;
        }
        
        const statusMap = {
            'pendiente': { label: 'Pendiente', class: 'pendiente', icon: 'fa-clock' },
            'preparacion': { label: 'En preparación', class: 'preparacion', icon: 'fa-fire' },
            'listo': { label: 'Listo para retirar', class: 'listo', icon: 'fa-shopping-bag' },
            'en_camino': { label: 'En camino', class: 'en_camino', icon: 'fa-motorcycle' },
            'entregado': { label: 'Entregado', class: 'entregado', icon: 'fa-smile-beam' },
            'cancelado': { label: 'Cancelado', class: 'cancelado', icon: 'fa-times-circle' }
        };

        const s = statusMap[order.status] || { label: order.status, class: 'default', icon: 'fa-info-circle' };
        const date = new Date(order.created_at).toLocaleString('es-AR', { 
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
                { key: 'pendiente', label: 'Recibido', icon: 'fa-clipboard-check' },
                { key: 'preparacion', label: 'Cocina', icon: 'fa-fire' },
                { key: 'listo', label: 'Listo', icon: 'fa-check' },
                { key: 'en_camino', label: 'En camino', icon: 'fa-motorcycle' },
                { key: 'entregado', label: 'Entregado', icon: 'fa-smile' }
            ];
        } else {
            steps = [
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
            if (order.status === 'preparacion') currentStepIndex = 1;
            else if (order.status === 'listo') currentStepIndex = 2;
            else if (order.status === 'en_camino') currentStepIndex = 3;
            else if (order.status === 'entregado') currentStepIndex = 4;
            else if (order.status === 'cancelado') currentStepIndex = -1;
        } else {
            // Lógica estándar de 4 pasos
            if (order.status === 'preparacion') currentStepIndex = 1;
            else if (order.status === 'listo') currentStepIndex = 2;
            // Si llega en_camino pero no es delivery (caso raro/fallback), lo mostramos como paso 2 (Listo) o 3 si fuera posible
            // Para mantener consistencia, si no es delivery, en_camino se visualiza igual que listo (ya salió)
            else if (order.status === 'en_camino') currentStepIndex = 2; 
            else if (order.status === 'entregado') currentStepIndex = 3;
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
                itemsHtml += `
                    <div class="order-summary-item">
                        <span class="item-name"><strong>${item.qty}x</strong> ${item.name}</span>
                        <span class="item-price">$${item.unit_price}</span>
                    </div>
                `;
            });
            itemsHtml += '</div>';
        }

        // --- BOTÓN WHATSAPP ---
        const whatsappNumber = String(getWhatsappNumber() || '').replace(/\D+/g, '');
        const whatsappMsg = encodeURIComponent(`Hola, tengo una consulta sobre mi pedido #${order.id}.`);
        const whatsappUrl = whatsappNumber ? `https://wa.me/${whatsappNumber}?text=${whatsappMsg}` : '#';

        const whatsappPaymentMsg = encodeURIComponent(`Hola, quiero realizar el pago del pedido #${order.id}.`);
        const whatsappPaymentUrl = whatsappNumber ? `https://wa.me/${whatsappNumber}?text=${whatsappPaymentMsg}` : '#';

        statusBody.innerHTML = `
            <div class="order-status-card">
                <!-- Header con Badge -->
                <div style="text-align: center; margin-bottom: 1rem;">
                    <div class="order-status-badge ${s.class}">
                        <i class="fas ${s.icon}"></i> ${s.label}
                    </div>
                </div>

                <!-- Stepper Visual -->
                ${stepperHtml}

                <div class="order-header" style="text-align: center; margin-bottom: 1.5rem; border-bottom: none; padding-bottom: 0;">
                    <span class="order-id" style="display: block; font-size: 0.875rem; color: #6b7280;">Pedido #${order.id}</span>
                    <span class="order-time" style="font-size: 0.75rem; color: #9ca3af;">${date}</span>
                </div>

                ${estimatedTimeHtml}

                <div class="order-details">
                    <h4 style="font-size: 1rem; margin-bottom: 1rem; color: var(--gastro-text-dark, #111827);">Detalle del pedido</h4>
                    ${itemsHtml}
                    <div class="order-summary-total">
                        <span>Total</span>
                        <span>$${order.total}</span>
                    </div>
                </div>

                ${order.status === 'listo' ? `
                    <div class="status-alert ready" style="margin-top: 1.5rem; padding: 1rem; background-color: #d1fae5; color: #065f46; border-radius: 0.5rem; text-align: center; font-weight: 600;">
                        <i class="fas fa-bell"></i> ¡Tu pedido está listo! Por favor acércate al mostrador.
                    </div>
                ` : ''}

                <!-- Botón de Ayuda WhatsApp -->
                <a href="${isPaymentMode ? whatsappPaymentUrl : whatsappUrl}" 
                   target="_blank" 
                   class="btn-whatsapp-status"
                   data-chat-url="${whatsappUrl}"
                   data-pay-url="${whatsappPaymentUrl}">
                    ${isPaymentMode ? '<i class="fas fa-credit-card"></i> Realizar pago' : '<i class="fab fa-whatsapp"></i> Consultar por este pedido'}
                </a>
            </div>
        `;
    }
});
