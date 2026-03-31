/**
 * WhatsApp and Address Helpers for Admin Panel
 * Extracted to external file to ensure proper encoding and caching.
 */

function getAddressText(val) {
  const formatObj = (obj) => {
    const address = String(obj && (obj.address || obj.street || obj.line1) || '').trim();
    const locality = String(obj && (obj.locality || obj.city || obj.town || obj.municipality) || '').trim();
    const province = String(obj && (obj.province || obj.state) || '').trim();
    const country = String(obj && obj.country || '').trim();
    const tail = [locality, province, country].filter(Boolean).join(', ');
    if (address && tail) return `${address}, ${tail}`;
    if (address) return address;
    if (tail) return tail;
    return '';
  };
  try {
    if (val && typeof val === 'object') return formatObj(val);
    const raw = String(val || '').trim();
    if (!raw) return '';
    if (raw.startsWith('{')) {
      try {
        const j = JSON.parse(raw);
        return formatObj(j);
      } catch (_) {
        try {
           // Try replacing single quotes with double quotes for Python-style dict strings
           const fixed = raw.replace(/'/g, '"');
           const j = JSON.parse(fixed);
           return formatObj(j);
        } catch(__) {}
      }
    }
    // Fallback regex for tricky formats
    if (raw.includes('address') || raw.includes('calle')) {
       const match = raw.match(/['"]?(?:address|street|line1)['"]?\s*[:=]\s*['"]([^'"]+)['"]/i);
       if (match && match[1]) return match[1].trim();
    }
    return raw;
  } catch (_) {
    return String(val || '').trim();
  }
}

function destinoLabelFor(order) {
  if (order.order_type === 'mesa') {
    const raw = String(order.table_number || '').trim();
    if (!raw) return 'Mesa';
    const lc = raw.toLowerCase();
    if (lc.startsWith('mesa ')) return 'Mesa ' + raw.slice(5).trim();
    if (lc.startsWith('barra ')) return 'Barra ' + raw.slice(6).trim();
    if (/^\d+$/.test(raw)) return `Mesa ${raw}`;
    return raw;
  }
  if (order.order_type === 'espera') return `Espera: ${order.customer_name || ''}`;
  return getAddressText(order.address_json).replace(/\n+/g,' ').slice(0,64);
}

function _getTenantOrderNumber(order) {
  const raw = order && (order.tenant_order_number ?? order.tenantOrderNumber ?? order.order_number ?? order.orderNumber ?? null);
  const n = Number(raw);
  return (Number.isFinite(n) && n > 0) ? Math.trunc(n) : null;
}

function _formatTenantOrderNumber(n) {
  if (!n) return '';
  const s = String(Math.trunc(Number(n)));
  return s.length >= 3 ? s : s.padStart(3, '0');
}

function _displayOrderNumber(order) {
  const n = _getTenantOrderNumber(order);
  if (n) return _formatTenantOrderNumber(n);
  return String(order && order.id || '').trim();
}

function getWaLink(order, phoneDigits) {
  if (!phoneDigits) return '';
  let tenantName = 'Nuestro Local';
  const tInput = document.getElementById('tenant-input');
  if (tInput && tInput.value) {
    // Formatea slug: "gastronomia-local1" -> "Gastronomia Local 1"
    tenantName = tInput.value
        .replace(/-/g, ' ')
        .replace(/(\d+)/g, ' $1')
        .trim()
        .replace(/\b\w/g, c => c.toUpperCase());
  }
  const isDireccion = (String(order.order_type || '').toLowerCase() === 'direccion');
  const address = isDireccion ? getAddressText(order.address_json) : '';
  const cName = (order.customer_name || '').trim();
  
  // Use escaped unicode for emojis to ensure safety
  // \uD83D\uDC4B = Waving Hand
  // \uD83D\uDCCD = Pushpin
  const waveEmoji = '\uD83D\uDC4B'; 
  const pinEmoji = '\uD83D\uDCCD';

  const ord = _displayOrderNumber(order);
  let msg = `Hola${cName ? ' ' + cName : ''}! ${waveEmoji} Te escribimos de *${tenantName}* por tu pedido *#${ord}*.`;
  if (address) {
    msg += ` ${pinEmoji} Dirección de entrega: ${address}.`;
  }

  // SANITIZATION: Check for corruption (diamonds) in inputs
  if (msg.indexOf('\ufffd') !== -1) {
      console.warn('Diamond char detected in WA link, stripping...');
      msg = msg.replace(/\ufffd/g, '');
  }

  // // console.log('WA Link generated (v3):', msg); // Debug log

  return `https://api.whatsapp.com/send?phone=${phoneDigits}&text=${encodeURIComponent(msg)}`;
}
