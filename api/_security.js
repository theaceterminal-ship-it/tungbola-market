const crypto = require('crypto');
const { db } = require('./_db');

function getIP(req) {
  return ((req.headers['x-forwarded-for'] || '').split(',')[0]).trim()
    || req.headers['x-real-ip'] || 'unknown';
}

function checkPassword(input, expected) {
  if (!input || !expected) return false;
  try {
    const key = process.env.HMAC_SECRET || 'tb-cmp-key';
    const h = k => crypto.createHmac('sha256', key).update(String(k)).digest();
    return crypto.timingSafeEqual(h(input), h(expected));
  } catch(e) { return false; }
}

async function rateLimit(req, action, max, windowSecs) {
  try {
    const ip = getIP(req);
    const now = Date.now();
    const windowMs = windowSecs * 1000;
    const windowStart = Math.floor(now / windowMs) * windowMs;
    const key = `${action}:${ip}:${windowStart}`;
    const { data, error } = await db().rpc('increment_rate_limit', {
      p_key: key, p_window_start: windowStart
    });
    if (error) return false;
    return data > max;
  } catch(e) { return false; }
}

function secureHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Content-Security-Policy', "frame-ancestors 'self' https://tungbola.vercel.app https://*.vercel.app");
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Cache-Control', 'no-store');
}

module.exports = { getIP, checkPassword, rateLimit, secureHeaders };
