/**
 * Cart Logic and State
 */
import { 
    CART_STORAGE_KEY, 
    LEGACY_CART_STORAGE_KEY, 
    getCheckoutMode,
    formatMoneyWithCode
} from './config.js?v=8';
import { announceCart } from './utils.js';

// Estado del carrito
export let cart = [];

// DOM Elements (se inicializan en initCart)
let cartCount, floatingCartCount, cartItems, cartTotalPrice, floatingCart;

export function initCartElements() {
    cartCount = document.getElementById('cart-count');
    floatingCartCount = document.getElementById('floating-cart-count');
    cartItems = document.getElementById('cart-items');
    cartTotalPrice = document.getElementById('cart-total-price');
    floatingCart = document.getElementById('floating-cart');
}

// Cargar carrito
export function loadCart() {
    let savedCart = localStorage.getItem(CART_STORAGE_KEY);
    
    // Migración de legacy
    if (!savedCart && LEGACY_CART_STORAGE_KEY !== CART_STORAGE_KEY) {
        const legacyCart = localStorage.getItem(LEGACY_CART_STORAGE_KEY);
        if (legacyCart) {
            try {
                localStorage.setItem(CART_STORAGE_KEY, legacyCart);
                savedCart = legacyCart;
                console.info('Migrado carrito desde clave legacy:', LEGACY_CART_STORAGE_KEY);
            } catch (e) {
                console.warn('Error migrando carrito:', e);
            }
        }
    }
    
    if (savedCart) {
        try {
            cart = JSON.parse(savedCart);
        } catch (e) {
            console.error('Error parseando carrito:', e);
            cart = [];
        }
    }
    updateCartCount();
}

// Guardar carrito
export function saveCart() {
    localStorage.setItem(CART_STORAGE_KEY, JSON.stringify(cart));
}

// Añadir al carrito
export function addToCart(id, name, price, imageSrc, event, showAnimationCallback, notes = '') {
    id = id || `auto-${Date.now()}`;
    name = (name && name.trim()) ? name : 'Producto';
    
    if (!isFinite(price) || price <= 0) {
        console.warn('Precio inválido', { id, name, price });
        return;
    }
    
    const existingItem = cart.find(item => item.id === id);
    if (existingItem) {
        existingItem.quantity++;
        if (!existingItem.image && imageSrc) {
            existingItem.image = imageSrc;
        }
        if (!existingItem.name && name) {
            existingItem.name = name;
        }
        if ((!existingItem.price || existingItem.price <= 0) && isFinite(price) && price > 0) {
            existingItem.price = price;
        }
        if (notes) {
         console.log('Adding notes to cart item:', notes);
         existingItem.notes = existingItem.notes ? existingItem.notes + ', ' + notes : notes;
    }
    } else {
        cart.push({
            id: id,
            name: name,
            price: price,
            image: imageSrc,
            quantity: 1,
            notes: notes
        });
    }
    
    saveCart();
    updateCartDisplay();
    updateCartCount();
    announceCart('Producto agregado: ' + name);
    
    if (event && showAnimationCallback) {
        showAnimationCallback(event);
    }
}

// Vaciar carrito
export function clearCart() {
    cart = [];
    saveCart();
    updateCartDisplay();
    updateCartCount();
    announceCart('Carrito vaciado. Total ' + formatMoneyWithCode(0));
}

// Actualizar contador
export function updateCartCount() {
    const totalItems = cart.reduce((total, item) => total + item.quantity, 0);
    if (cartCount) cartCount.textContent = totalItems;
    if (floatingCartCount) floatingCartCount.textContent = totalItems;
    
    if (floatingCart) {
        if (totalItems > 0) {
            floatingCart.classList.add('show');
            floatingCart.style.display = 'flex'; // Ensure it's visible
        } else {
            floatingCart.classList.remove('show');
            floatingCart.style.display = 'none';
        }
    }
}

// Actualizar visualización
export function updateCartDisplay() {
    if (!cartItems || !cartTotalPrice) return;
    
    cartItems.innerHTML = '';
    
    if (cart.length === 0) {
        cartItems.innerHTML = '<p class="empty-cart">Tu carrito está vacío</p>';
        cartTotalPrice.textContent = formatMoneyWithCode(0);
        announceCart('Carrito vacío. Total ' + formatMoneyWithCode(0));
        return;
    }
    
    let totalPrice = 0;
    
    cart.forEach(item => {
        const cartItem = document.createElement('div');
        cartItem.className = 'cart-item';
        cartItem.setAttribute('data-id', item.id);
        
        // Imagen
        const itemImage = document.createElement('div');
        itemImage.className = 'cart-item-image';
        if (item.image) {
            itemImage.classList.add('is-loading');
            const img = document.createElement('img');
            img.src = item.image;
            img.alt = item.name;
            img.loading = 'eager';
            img.decoding = 'async';
            try { img.fetchPriority = 'high'; } catch (_) {}
            const markLoaded = () => itemImage.classList.remove('is-loading');
            const markError = () => {
                itemImage.classList.remove('is-loading');
                itemImage.classList.add('has-error');
            };
            img.addEventListener('load', markLoaded, { once: true });
            img.addEventListener('error', markError, { once: true });
            if (img.complete && img.naturalWidth > 0) {
                markLoaded();
            }
            itemImage.appendChild(img);
            const placeholder = document.createElement('span');
            placeholder.className = 'cart-item-image-placeholder';
            placeholder.setAttribute('aria-hidden', 'true');
            placeholder.innerHTML = '&#128247;';
            itemImage.appendChild(placeholder);
        } else {
            itemImage.classList.add('has-error');
            const placeholder = document.createElement('span');
            placeholder.className = 'cart-item-image-placeholder';
            placeholder.setAttribute('aria-hidden', 'true');
            placeholder.innerHTML = '&#128247;';
            itemImage.appendChild(placeholder);
        }
        
        // Info
        const itemInfo = document.createElement('div');
        itemInfo.className = 'cart-item-info';
        
        const itemName = document.createElement('div');
        itemName.className = 'cart-item-name';
        itemName.textContent = item.name;
        
        const itemPrice = document.createElement('div');
        itemPrice.className = 'cart-item-price';
        itemPrice.textContent = formatMoneyWithCode(parseInt(item.price));
        
        // Controles de cantidad
        const itemQuantityContainer = document.createElement('div');
        itemQuantityContainer.className = 'cart-item-quantity-container';
        
        const decreaseBtn = document.createElement('button');
        decreaseBtn.className = 'quantity-btn decrease';
        decreaseBtn.textContent = '-';
        decreaseBtn.addEventListener('click', () => {
            if (item.quantity > 1) {
                item.quantity--;
            } else {
                const idx = cart.findIndex(c => c.id === item.id);
                if (idx !== -1) {
                    cart.splice(idx, 1);
                    announceCart('Producto eliminado: ' + item.name);
                }
            }
            saveCart();
            updateCartDisplay();
            updateCartCount();
        });
        
        const itemQuantity = document.createElement('span');
        itemQuantity.className = 'cart-item-quantity';
        itemQuantity.textContent = item.quantity;
        
        const increaseBtn = document.createElement('button');
        increaseBtn.className = 'quantity-btn increase';
        increaseBtn.textContent = '+';
        increaseBtn.addEventListener('click', () => {
            item.quantity++;
            saveCart();
            updateCartDisplay();
            updateCartCount();
        });
        
        itemQuantityContainer.appendChild(decreaseBtn);
        itemQuantityContainer.appendChild(itemQuantity);
        itemQuantityContainer.appendChild(increaseBtn);
        
        // Botón eliminar
        const removeBtn = document.createElement('button');
        removeBtn.className = 'remove-item-btn';
        removeBtn.innerHTML = '&times;';
        removeBtn.addEventListener('click', () => {
            const idx = cart.findIndex(c => c.id === item.id);
            if (idx !== -1) {
                cart.splice(idx, 1);
                announceCart('Producto eliminado: ' + item.name);
            }
            saveCart();
            updateCartDisplay();
            updateCartCount();
        });
        
        // Notas
        const itemNotesContainer = document.createElement('div');
        itemNotesContainer.className = 'cart-item-notes-container';
        itemNotesContainer.innerHTML = `<label class="cart-item-notes-label">Detalle</label>`;
        const itemNotesInput = document.createElement('input');
        itemNotesInput.type = 'text';
        itemNotesInput.className = 'cart-item-notes-input';
        itemNotesInput.placeholder = 'Ej: sin condimentos';
        itemNotesInput.value = item.notes || '';
        itemNotesInput.addEventListener('input', function() {
            item.notes = this.value || '';
            saveCart();
        });
        itemNotesContainer.appendChild(itemNotesInput);
        
        // Ensamblaje
        itemInfo.appendChild(itemName);
        itemInfo.appendChild(itemPrice);
        itemInfo.appendChild(itemQuantityContainer);
        itemInfo.appendChild(itemNotesContainer);
        
        cartItem.appendChild(itemImage);
        cartItem.appendChild(itemInfo);
        cartItem.appendChild(removeBtn);
        
        cartItems.appendChild(cartItem);
        
        totalPrice += item.price * item.quantity;
    });
    
    // Costo de envío
    let shippingCost = 0;
    let currentOrderType = 'mesa';
    const checkedRadio = document.querySelector('input[name="orderType"]:checked');
    if (checkedRadio) currentOrderType = checkedRadio.value;
    
    if (currentOrderType === 'direccion' && window.BusinessConfig && window.BusinessConfig.shipping_cost) {
        shippingCost = parseInt(window.BusinessConfig.shipping_cost) || 0;
    }
    
    if (shippingCost > 0) {
        const shippingText = formatMoneyWithCode(parseInt(shippingCost));
        const shippingRow = document.createElement('div');
        shippingRow.className = 'cart-item shipping-row';
        shippingRow.style.cssText = 'border-top: 1px dashed #eee; margin-top: 10px; padding-top: 10px; background: none;';
        shippingRow.innerHTML = `
            <div class="cart-item-info" style="width:100%; display:flex; justify-content:space-between; align-items:center;">
                <div class="cart-item-name" style="font-weight:bold; color: #666;">Costo de envío</div>
                <div class="cart-item-price">${shippingText}</div>
            </div>
        `;
        cartItems.appendChild(shippingRow);
        totalPrice += shippingCost;
    }
    
    // Lógica de Propina (Solo Mesa)
    if (currentOrderType === 'mesa') {
        const tipAmount = Math.round(totalPrice * 0.10);
        const subtotalText = formatMoneyWithCode(parseInt(totalPrice));
        const tipText = formatMoneyWithCode(parseInt(tipAmount));
        
        // Mostrar subtotal (Total sin propina)
        const subtotalRow = document.createElement('div');
        subtotalRow.className = 'cart-item subtotal-row';
        subtotalRow.style.cssText = 'border-top: 1px solid #eee; margin-top: 10px; padding-top: 10px; background: none;';
        subtotalRow.innerHTML = `
             <div class="cart-item-info" style="width:100%; display:flex; justify-content:space-between; align-items:center;">
                <div class="cart-item-name" style="font-weight:bold;">Total (sin propina)</div>
                <div class="cart-item-price">${subtotalText}</div>
            </div>
        `;
        cartItems.appendChild(subtotalRow);

        // Mostrar Propina
        const tipRow = document.createElement('div');
        tipRow.className = 'cart-item tip-row';
        tipRow.style.cssText = 'border-top: 1px dashed #eee; margin-top: 5px; padding-top: 5px; background: none; color: #2e7d32;';
        tipRow.innerHTML = `
            <div class="cart-item-info" style="width:100%; display:flex; justify-content:space-between; align-items:center;">
                <div class="cart-item-name" style="font-weight:bold;">Propina sugerida (10%)</div>
                <div class="cart-item-price">${tipText}</div>
            </div>
        `;
        cartItems.appendChild(tipRow);
        
        totalPrice += tipAmount;
    }
    
    const totalText = formatMoneyWithCode(parseInt(totalPrice));
    cartTotalPrice.textContent = totalText;
    announceCart('Total actualizado: ' + totalText);
}
