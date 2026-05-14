/**
 * Carousel Component
 * Handles the main carousel functionality including auto-play, navigation, and touch interactions.
 */

let currentSlideIndex = 0;
let carouselInterval;
let isDragging = false;
let isAutoPlayActive = true;
let autoPlayDuration = 5000;
let isCarouselVisible = true;
let wasAutoPlayActiveBeforeHidden = false;

// Variables de estado para arrastre táctil
let touchStartX = 0;
let touchCurrentX = 0;
let startTransform = 0;
const percentPerSlide = 100; // Asumimos 1 slide visible por vez (100%)

// Funciones expuestas
export async function loadAndInitCarousel(tenantSlug) {
    if (tenantSlug) {
        try {
            const response = await fetch(`/api/carousel?tenant_slug=${tenantSlug}`);
            if (response.ok) {
                const data = await response.json();
                if (data.slides && data.slides.length > 0) {
                    renderCarouselSlides(data.slides);
                }
            }
        } catch (e) {
            console.error('Error loading carousel:', e);
        }
    }
    initializeCarousel();
}

function renderCarouselSlides(slides) {
    const slidesContainer = document.querySelector('.carousel-slides');
    const indicatorsContainer = document.querySelector('.carousel-indicators');

    if (!slidesContainer || !indicatorsContainer) return;

    let slidesHtml = '';
    let indicatorsHtml = '';

    slides.forEach((slide, index) => {
        const activeClass = index === 0 ? ' active' : '';
        const title = slide.title || '';
        const text = slide.text || '';
        const titleColor = slide.title_color || '';
        const textColor = slide.text_color || '';
        const titleStyle = titleColor ? ` style="color:${titleColor}"` : '';
        const textStyle = textColor ? ` style="color:${textColor}"` : '';
        
        slidesHtml += `
            <div class="carousel-slide${activeClass}">
                <img src="${slide.image_url}" alt="${title}" width="1600" height="900">
                <div class="carousel-content">
                    <h2${titleStyle}>${title}</h2>
                    <p${textStyle}>${text}</p>
                </div>
            </div>
        `;

        indicatorsHtml += `
            <button class="indicator${activeClass}" aria-label="Ir al slide ${index + 1}"></button>
        `;
    });

    slidesContainer.innerHTML = slidesHtml;
    indicatorsContainer.innerHTML = indicatorsHtml;
}

export function initializeCarousel() {
    const carouselContainer = document.querySelector('.carousel-container');
    if (!carouselContainer) return;
    
    const slides = document.querySelectorAll('.carousel-slide');
    const indicators = document.querySelectorAll('.indicator');
    const prevBtn = document.querySelector('.carousel-prev');
    const nextBtn = document.querySelector('.carousel-next');
    
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
    
    // Pausar auto-play al hacer hover
    carouselContainer.addEventListener('mouseenter', () => {
        if (isAutoPlayActive) stopCarouselInterval();
    });
    
    carouselContainer.addEventListener('mouseleave', () => {
        if (isAutoPlayActive) startCarouselInterval();
    });
    
    // Pausar auto-play en eventos táctiles
    carouselContainer.addEventListener('touchstart', () => {
        if (isAutoPlayActive) stopCarouselInterval();
    });
    
    carouselContainer.addEventListener('touchend', () => {
        if (isAutoPlayActive) {
            setTimeout(() => {
                if (isAutoPlayActive) startCarouselInterval();
            }, 2000);
        }
    });
    
    // Inicializar soporte táctil (swipe)
    setupTouchGestures(carouselContainer);
    
    // Inicializar observador de visibilidad
    setupVisibilityObserver();
    
    // Estado inicial
    updateIndicators();
}

export function showSlide(index) {
    const slides = document.querySelectorAll('.carousel-slide');
    const indicators = document.querySelectorAll('.indicator');
    const slidesContainer = document.querySelector('.carousel-slides');
    
    if (slides.length === 0) return;
    
    // Remover clase active
    slides.forEach(slide => slide.classList.remove('active'));
    indicators.forEach(indicator => indicator.classList.remove('active'));
    
    // Validar índice
    if (index >= slides.length) {
        currentSlideIndex = 0;
    } else if (index < 0) {
        currentSlideIndex = slides.length - 1;
    } else {
        currentSlideIndex = index;
    }
    
    // Aplicar transformación
    if (slidesContainer) {
        if (!isDragging) {
            slidesContainer.style.transition = 'transform 0.5s ease-in-out';
        }
        slidesContainer.style.transform = `translateX(-${currentSlideIndex * percentPerSlide}%)`;
    }
    
    // Activar slide actual
    if (slides[currentSlideIndex]) {
        slides[currentSlideIndex].classList.add('active');
    }
    if (indicators[currentSlideIndex]) {
        indicators[currentSlideIndex].classList.add('active');
    }
}

export function nextSlide() {
    showSlide(currentSlideIndex + 1);
    if (isAutoPlayActive) startProgress();
}

export function previousSlide() {
    showSlide(currentSlideIndex - 1);
    if (isAutoPlayActive) startProgress();
}

export function goToSlide(index) {
    showSlide(index);
    if (isAutoPlayActive) startProgress();
}

export function toggleAutoPlay() {
    isAutoPlayActive = !isAutoPlayActive;
    if (isAutoPlayActive) {
        startCarouselInterval();
    } else {
        stopCarouselInterval();
    }
    updatePlayButtonUI();
}

function updatePlayButtonUI() {
    const playBtnIcon = document.querySelector('.carousel-play-button i');
    const statusSpan = document.getElementById('autoplay-status');
    
    if (playBtnIcon) {
        playBtnIcon.className = isAutoPlayActive ? 'fas fa-pause' : 'fas fa-play';
    }
    if (statusSpan) {
        statusSpan.textContent = isAutoPlayActive ? 'Reproducción automática activa' : 'Reproducción automática pausada';
    }
}

function updateIndicators() {
    const slides = document.querySelectorAll('.carousel-slide');
    const indicators = document.querySelectorAll('.indicator');
    
    slides.forEach(slide => slide.classList.remove('active'));
    indicators.forEach(indicator => indicator.classList.remove('active'));
    
    if (slides[currentSlideIndex]) slides[currentSlideIndex].classList.add('active');
    if (indicators[currentSlideIndex]) indicators[currentSlideIndex].classList.add('active');
}

// Control de intervalos y progreso
function startCarouselInterval() {
    if (!isAutoPlayActive) return;
    
    if (carouselInterval) clearInterval(carouselInterval);
    
    startProgress();
    carouselInterval = setInterval(() => {
        nextSlide();
    }, autoPlayDuration);
}

function stopCarouselInterval() {
    if (carouselInterval) {
        clearInterval(carouselInterval);
        carouselInterval = null;
    }
    stopProgress();
}

function resetCarouselInterval() {
    stopCarouselInterval();
    startCarouselInterval();
}

// Indicador de progreso SVG
function startProgress() {
    const progressRing = document.querySelector('.progress-ring');
    const progressElement = document.querySelector('.progress-ring-progress');
    
    if (progressRing && progressElement) {
        progressRing.classList.remove('active');
        progressElement.style.strokeDasharray = '0 100.53';
        progressRing.offsetHeight; // Force reflow
        
        setTimeout(() => {
            progressRing.classList.add('active');
        }, 10);
    }
}

function stopProgress() {
    const progressRing = document.querySelector('.progress-ring');
    const progressElement = document.querySelector('.progress-ring-progress');
    
    if (progressRing && progressElement) {
        progressRing.classList.remove('active');
        progressElement.style.strokeDasharray = '0 100.53';
    }
}

// Gestos táctiles
function setupTouchGestures(carouselContainer) {
    const slidesContainer = document.querySelector('.carousel-slides');
    if (!slidesContainer) return;

    carouselContainer.addEventListener('touchstart', (e) => {
        isDragging = true;
        touchStartX = e.touches[0].screenX;
        touchCurrentX = touchStartX;
        
        const style = window.getComputedStyle(slidesContainer);
        const matrix = new WebKitCSSMatrix(style.transform);
        // Convertir traslación actual a porcentaje aproximado
        const currentTranslateX = matrix.m41;
        const containerWidth = carouselContainer.offsetWidth;
        startTransform = (currentTranslateX / containerWidth) * 100;
        
        slidesContainer.style.transition = 'none';
        
        carouselContainer.style.transform = 'scale(0.98)';
        carouselContainer.style.transition = 'transform 0.2s ease';
    }, { passive: true });
    
    carouselContainer.addEventListener('touchmove', (e) => {
        if (!isDragging) return;
        
        touchCurrentX = e.changedTouches[0].screenX;
        const deltaX = touchCurrentX - touchStartX;
        const containerWidth = carouselContainer.offsetWidth;
        const dragPercentage = (deltaX / containerWidth) * percentPerSlide;
        
        const newTransform = startTransform + dragPercentage;
        slidesContainer.style.transform = `translateX(${newTransform}%)`;
        
        const dragIntensity = Math.min(Math.abs(deltaX) / containerWidth, 0.3);
        carouselContainer.style.filter = `brightness(${1 - dragIntensity * 0.2})`;
    }, { passive: true });
    
    carouselContainer.addEventListener('touchend', (e) => {
        if (!isDragging) return;
        
        isDragging = false;
        const swipeDistance = touchCurrentX - touchStartX;
        const containerWidth = carouselContainer.offsetWidth;
        const swipeThreshold = containerWidth * 0.25;
        
        slidesContainer.style.transition = 'transform 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94)';
        
        carouselContainer.style.transform = 'scale(1)';
        carouselContainer.style.filter = 'brightness(1)';
        carouselContainer.style.transition = 'transform 0.3s ease, filter 0.3s ease';
        
        const slideCount = document.querySelectorAll('.carousel-slide').length;

        if (Math.abs(swipeDistance) > swipeThreshold) {
            if (swipeDistance > 0) {
                // Derecha -> Anterior
                currentSlideIndex = currentSlideIndex > 0 ? currentSlideIndex - 1 : (slideCount - 1);
            } else {
                // Izquierda -> Siguiente
                currentSlideIndex = currentSlideIndex < (slideCount - 1) ? currentSlideIndex + 1 : 0;
            }
            // Actualizar transform final
            slidesContainer.style.transform = `translateX(-${currentSlideIndex * percentPerSlide}%)`;
            updateIndicators();
            
            // Pulso de éxito
            setTimeout(() => {
                carouselContainer.style.transform = 'scale(1.02)';
                setTimeout(() => carouselContainer.style.transform = 'scale(1)', 100);
            }, 50);
        } else {
            // Revertir
            slidesContainer.style.transform = `translateX(-${currentSlideIndex * percentPerSlide}%)`;
        }
        
        resetCarouselInterval();
    }, { passive: true });
}

// Intersection Observer para pausar cuando no es visible
function setupVisibilityObserver() {
    const carouselContainer = document.querySelector('.carousel-container');
    if (!carouselContainer) return;
    
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                isCarouselVisible = true;
                if (wasAutoPlayActiveBeforeHidden && !isAutoPlayActive) {
                    isAutoPlayActive = true;
                    startCarouselInterval();
                }
            } else {
                isCarouselVisible = false;
                if (isAutoPlayActive) {
                    wasAutoPlayActiveBeforeHidden = true;
                    stopCarouselInterval();
                    isAutoPlayActive = false;
                }
            }
        });
    }, { threshold: 0.5 });
    
    observer.observe(carouselContainer);
}

// Inicializar navegación de intereses (Interest Strip)
export function initInterestNav() {
    const section = document.getElementById('interest-index');
    if (!section) return;

    const strip = section.querySelector('.interest-strip');
    const prevBtn = section.querySelector('.interest-nav-btn.prev');
    const nextBtn = section.querySelector('.interest-nav-btn.next');
    if (!strip || !prevBtn || !nextBtn) return;

    function isMobile() {
        return window.matchMedia('(max-width: 768px)').matches;
    }

    function syncVisibility() {
        const visible = isMobile();
        prevBtn.style.display = visible ? 'flex' : 'none';
        nextBtn.style.display = visible ? 'flex' : 'none';
        updateState();
    }

    function updateState() {
        const atStart = strip.scrollLeft <= 1;
        const atEnd = (strip.scrollLeft + strip.clientWidth) >= (strip.scrollWidth - 1);
        prevBtn.disabled = atStart;
        nextBtn.disabled = atEnd;
        prevBtn.classList.toggle('disabled', atStart);
        nextBtn.classList.toggle('disabled', atEnd);
        section.classList.toggle('has-left', !atStart);
        section.classList.toggle('has-right', !atEnd);
        return { atStart, atEnd, maxScroll: Math.max(0, strip.scrollWidth - strip.clientWidth) };
    }

    window.scrollInterest = function(direction) {
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
    };

    prevBtn.addEventListener('click', () => window.scrollInterest('left'));
    nextBtn.addEventListener('click', () => window.scrollInterest('right'));

    strip.addEventListener('scroll', updateState);

    let resizeTimer;
    window.addEventListener('resize', () => {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(syncVisibility, 150);
    });

    syncVisibility();

    if (window.matchMedia('(max-width: 768px)').matches) {
        let shown = false;
        try {
            const slug = String(window.BUSINESS_SLUG || window.location.pathname || 'interest').trim();
            shown = sessionStorage.getItem('scrollHint_interest_' + slug) === '1';
            if (!shown) {
                const state = updateState();
                if (state.maxScroll && state.atStart) {
                    setTimeout(() => {
                        const latest = updateState();
                        if (!latest.maxScroll || !latest.atStart) return;
                        strip.scrollTo({ left: Math.min(26, latest.maxScroll), behavior: 'smooth' });
                        setTimeout(() => {
                            strip.scrollTo({ left: 0, behavior: 'smooth' });
                            updateState();
                        }, 480);
                    }, 850);
                }
                sessionStorage.setItem('scrollHint_interest_' + slug, '1');
            }
        } catch (_) {}
    }
}

// Focus effect for interest section
export function initInterestFocusState() {
    const interestSection = document.getElementById('interest-index');
    if (!interestSection) return;

    const updateInterestFocusState = () => {
        const rect = interestSection.getBoundingClientRect();
        const viewportCenterY = window.innerHeight / 2;
        const sectionCenterY = rect.top + rect.height / 2;
        const distance = Math.abs(sectionCenterY - viewportCenterY);
        const visible = rect.top < window.innerHeight && rect.bottom > 0;
        const startThreshold = Math.min(window.innerHeight * 0.45, 320);

        if (visible && distance < startThreshold) {
            interestSection.classList.add('focused');
        } else {
            interestSection.classList.remove('focused');
        }
    };

    window.addEventListener('scroll', updateInterestFocusState, { passive: true });
    updateInterestFocusState();
}
