const crypto = require('crypto');
const https  = require('https');

async function sendPush(subscription) {
  if (!process.env.VAPID_PUBLIC_KEY || !process.env.VAPID_PRIVATE_KEY) return;
  try {
    const endpoint = new URL(subscription.endpoint);
    const subtle   = (globalThis.crypto || crypto.webcrypto).subtle;
    const now      = Math.floor(Date.now() / 1000);
    const h = Buffer.from(JSON.stringify({ typ: 'JWT', alg: 'ES256' })).toString('base64url');
    const c = Buffer.from(JSON.stringify({ aud: endpoint.origin, exp: now + 43200, sub: 'mailto:admin@tungbola.com' })).toString('base64url');
    const sigInput = Buffer.from(`${h}.${c}`);
    const pubBytes = Buffer.from(process.env.VAPID_PUBLIC_KEY, 'base64url');
    const privKey  = await subtle.importKey('jwk',
      { kty: 'EC', crv: 'P-256', d: process.env.VAPID_PRIVATE_KEY,
        x: pubBytes.slice(1, 33).toString('base64url'), y: pubBytes.slice(33, 65).toString('base64url'),
        key_ops: ['sign'] },
      { name: 'ECDSA', namedCurve: 'P-256' }, false, ['sign']
    );
    const sig = Buffer.from(await subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, privKey, sigInput)).toString('base64url');
    const jwt = `${h}.${c}.${sig}`;
    await new Promise((resolve, reject) => {
      const req = https.request({
        hostname: endpoint.hostname, port: endpoint.port || 443,
        path: endpoint.pathname + endpoint.search, method: 'POST',
        headers: { 'Authorization': `vapid t=${jwt},k=${process.env.VAPID_PUBLIC_KEY}`, 'TTL': '86400', 'Urgency': 'high', 'Content-Length': '0' }
      }, res => { res.resume(); resolve(res.statusCode); });
      req.on('error', reject);
      req.end();
    });
  } catch(e) { console.error('sendPush failed:', e.message); }
}

module.exports = { sendPush };
