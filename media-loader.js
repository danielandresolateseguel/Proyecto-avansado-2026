// Módulo unificado de optimización y lazy-loading de imágenes
document.addEventListener('DOMContentLoaded', function() {
  function applyFeaturedBlurBackground(img) {
    if (!img) return;
    const container = img.closest ? img.closest('.product-image') : null;
    if (!container) return;
    const featuredRoot = container.closest ? container.closest('#featured-dishes') : null;
    if (!featuredRoot) return;
    const src = (img.currentSrc || img.getAttribute('src') || '').trim();
    if (!src) return;
    const safeSrc = src.replace(/"/g, '%22');
    container.style.setProperty('--product-img-bg', `url("${safeSrc}")`);
  }

  function bindFeaturedBlurBackground(img) {
    if (!img || !img.dataset) return;
    if (img.dataset.blurBgBound === '1') return;
    img.dataset.blurBgBound = '1';
    const handler = () => applyFeaturedBlurBackground(img);
    img.addEventListener('load', handler);
    img.addEventListener('error', handler);
    handler();
  }

  // Ajustes base para imágenes de productos
  function setupProductImagesDefaults(root = document) {
    const productImages = root.querySelectorAll ? root.querySelectorAll('.product-image img') : [];
    productImages.forEach(img => {
      try {
        img.decoding = 'async';
        // Por defecto baja prioridad; se elevará cerca del fold
        img.setAttribute('fetchpriority', 'low');
      } catch (e) {
        // silencioso
      }
      bindFeaturedBlurBackground(img);
    });
  }

  // IntersectionObserver para marcar imágenes como cargadas al entrar en el viewport
  function setupIntersectionObserver() {
    if ('IntersectionObserver' in window) {
      const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
          if (entry.isIntersecting) {
            const img = entry.target;
            if (!img.dataset.loaded) {
              img.dataset.loaded = true;
              img.classList.add('loaded');
            }
            bindFeaturedBlurBackground(img);
            observer.unobserve(img);
          }
        });
      }, { rootMargin: '600px 0px', threshold: 0.01 });

      document.querySelectorAll('img[loading="lazy"]').forEach(img => observer.observe(img));

      // Observar mutaciones para nuevas imágenes dinámicas
      const mo = new MutationObserver((mutations) => {
        mutations.forEach(m => {
          m.addedNodes && m.addedNodes.forEach(node => {
            if (node && node.nodeType === 1) {
              // Si es una imagen directamente
              if (node.tagName === 'IMG' && node.getAttribute('loading') === 'lazy') {
                observer.observe(node);
              }
              // Si es un contenedor que incluye imágenes
              const imgs = node.querySelectorAll ? node.querySelectorAll('img[loading="lazy"]') : [];
              imgs.forEach(img => observer.observe(img));
            }
          });
        });
      });
      mo.observe(document.body, { childList: true, subtree: true });
    } else {
      // Fallback: verificar en scroll/resize para navegadores sin IO
      function isInViewport(element) {
        const rect = element.getBoundingClientRect();
        return rect.top < (window.innerHeight || document.documentElement.clientHeight) && rect.bottom > 0;
      }
      function lazyLoadFallback() {
        const images = document.querySelectorAll('img[loading="lazy"]');
        images.forEach(img => {
          if (isInViewport(img) && !img.dataset.loaded) {
            img.dataset.loaded = true;
            img.classList.add('loaded');
          }
        });
      }
      window.addEventListener('scroll', lazyLoadFallback);
      window.addEventListener('resize', lazyLoadFallback);
      lazyLoadFallback();
    }
  }

  // Manejo de errores de carga de imágenes
  function handleImageError(root = document) {
    const images = root.querySelectorAll ? root.querySelectorAll('img') : [];
    images.forEach(img => {
      function fallbackIfReallyBroken() {
        // Si la imagen ya cargó correctamente, no se reemplaza
        if (img.complete && img.naturalWidth > 0) return;
        img.src = 'Imagenes/asus-proart-p16.png';
        img.alt = 'Imagen no disponible';
        // Evitar bucles sólo después de aplicar fallback
        img.onerror = null;
        bindFeaturedBlurBackground(img);
      }
      img.addEventListener('error', fallbackIfReallyBroken);
      bindFeaturedBlurBackground(img);
    });
  }

  // Detección y fallback si el navegador no soporta WebP
  function canUseWebp() {
    try {
      return document.createElement('canvas').toDataURL('image/webp').indexOf('data:image/webp') === 0;
    } catch (e) {
      return false;
    }
  }

  // Priorizar imágenes cercanas al fold
  function boostAboveFoldPriorities(root = document) {
    const viewportH = window.innerHeight || document.documentElement.clientHeight;
    const margin = 150; // adelanta un poco la descarga
    const candidates = root.querySelectorAll ? root.querySelectorAll('.product-image img') : [];
    candidates.forEach(img => {
      const rect = img.getBoundingClientRect();
      if (rect.top < viewportH + margin) {
        img.setAttribute('fetchpriority', 'high');
      }
      bindFeaturedBlurBackground(img);
    });
  }

  // Ajustar prioridades del carrusel
  function adjustCarouselImagePriorities() {
    const carouselImages = document.querySelectorAll('.carousel-slides .carousel-slide img');
    if (!carouselImages.length) return;
    carouselImages.forEach((img, idx) => {
      try {
        img.decoding = 'async';
        if (idx === 0) {
          img.setAttribute('loading', 'eager');
          img.setAttribute('fetchpriority', 'high');
        } else {
          img.setAttribute('loading', 'lazy');
          img.setAttribute('fetchpriority', 'low');
        }
      } catch (e) {
        // silencioso
      }
    });
  }

  // Asegurar que la imagen de modal no se demore
  function prioritizeModalImage() {
    const modalImage = document.getElementById('modal-product-image');
    if (modalImage) {
      try {
        modalImage.decoding = 'async';
        modalImage.setAttribute('loading', 'eager');
        modalImage.setAttribute('fetchpriority', 'high');
      } catch (e) {
        // silencioso
      }
    }
  }

  // Inicialización
  setupProductImagesDefaults();
  adjustCarouselImagePriorities();
  setupIntersectionObserver();
  boostAboveFoldPriorities();
  handleImageError();
  prioritizeModalImage();

  // Fallback para navegadores sin soporte WebP
  if (!canUseWebp()) {
    document.querySelectorAll('.product-image img').forEach(img => {
      const src = img.getAttribute('src') || '';
      if (src.toLowerCase().endsWith('.webp')) {
          img.setAttribute('src', 'Imagenes/asus-proart-p16.png');
          img.setAttribute('srcset', 'Imagenes/asus-proart-p16.png');
        }
    });
  }

  // Watchdog de decodificación: si una imagen termina "cargada" pero no decodifica, aplicar fallback
  (function setupDecodeWatchdog(){
    const FALLBACK_SRC = 'Imagenes/asus-proart-p16.png';
    const applyWatchdog = (img) => {
      function ensureVisible() {
        if (img.complete && img.naturalWidth === 0) {
          img.src = FALLBACK_SRC;
          img.srcset = FALLBACK_SRC;
          img.alt = img.alt || 'Imagen no disponible';
          bindFeaturedBlurBackground(img);
        }
      }
      img.addEventListener('load', ensureVisible);
      bindFeaturedBlurBackground(img);
    };
    document.querySelectorAll('.product-image img').forEach(applyWatchdog);
    // Observer para nuevas imágenes
    const mo = new MutationObserver((mutations) => {
      mutations.forEach(m => {
        m.addedNodes && m.addedNodes.forEach(node => {
          if (node && node.nodeType === 1) {
            if (node.tagName === 'IMG' && node.closest('.product-image')) {
              applyWatchdog(node);
            }
            const imgs = node.querySelectorAll ? node.querySelectorAll('.product-image img') : [];
            imgs.forEach(applyWatchdog);
          }
        });
      });
    });
    mo.observe(document.body, { childList: true, subtree: true });
    setTimeout(() => {
      document.querySelectorAll('.product-image img').forEach(img => {
        if (img.complete && img.naturalWidth === 0) {
          img.src = FALLBACK_SRC;
          img.srcset = FALLBACK_SRC;
          img.alt = img.alt || 'Imagen no disponible';
          bindFeaturedBlurBackground(img);
        }
      });
    }, 1500);
  })();

  // Hook global para reinicializar loaders en contenido dinámico
  window.__reinitMediaLoaders = function(rootEl) {
    const root = rootEl || document;
    setupProductImagesDefaults(root);
    boostAboveFoldPriorities(root);
    handleImageError(root);
    // No es necesario recrear IO; las nuevas imágenes son observadas por MutationObserver
  };
});
