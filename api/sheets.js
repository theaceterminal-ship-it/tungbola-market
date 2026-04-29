const { Redis } = require('@upstash/redis');
const { del: blobDel } = require('@vercel/blob');
const { handleUpload } = require('@vercel/blob/client');
const { secureHeaders, rateLimit, checkPassword } = require('./_security');
const kv = Redis.fromEnv();

// Each sheet is a field in this hash: field = sheet number (string), value = JSON sheet object.
// HSETNX guarantees atomic "set only if field missing" — no race condition under any concurrency.
const HASH = 'tb:mkt:sheets:hash';

function genId() {
  return Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4);
}

async function getAllSheets() {
  const data = await kv.hgetall(HASH);
  if (!data) return [];
  return Object.values(data)
    .map(v => { try { return typeof v === 'string' ? JSON.parse(v) : v; } catch(e) { return null; } })
    .filter(Boolean)
    .sort((a, b) => a.n - b.n);
}

module.exports = async function(req, res) {
  secureHeaders(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  /* GET: admin lists all sheets */
  if (req.method === 'GET') {
    if (await rateLimit(req, 'listsheets', 60, 60))
      return res.status(429).json({ error: 'Too many requests' });
    const { password } = req.query;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });

    let all = await getAllSheets();

    // One-time migration: if hash is empty but old list exists, migrate it
    if (all.length === 0) {
      const oldList = await kv.get('tb:mkt:sheets');
      if (Array.isArray(oldList) && oldList.length) {
        const pipeline = kv.pipeline();
        for (const s of oldList) pipeline.hsetnx(HASH, String(s.n), JSON.stringify(s));
        await pipeline.exec();
        all = await getAllSheets();
      }
    }

    return res.json({ sheets: all, total: all.length });
  }

  if (req.method !== 'POST') return res.status(405).end();
  const body = req.body || {};

  /* Vercel Blob client upload — token request from browser's upload() */
  if (body.type === 'blob.generate-client-token' || body.type === 'blob.upload-completed') {
    if (body.type === 'blob.generate-client-token') {
      if (await rateLimit(req, 'uploadsheet', 15000, 3600))
        return res.status(429).json({ error: 'Upload limit reached. Wait an hour.' });
    }

    const headers = { get: name => req.headers[name.toLowerCase()] || null };
    try {
      const response = await handleUpload({
        body,
        request: { headers },
        onBeforeGenerateToken: async (pathname, clientPayload) => {
          let payload;
          try { payload = JSON.parse(clientPayload || '{}'); }
          catch(e) { throw new Error('Invalid payload'); }

          if (!checkPassword(payload.password, process.env.ADMIN_PASSWORD))
            throw new Error('Wrong password');

          const num = parseInt(payload.sheetNumber);
          if (!num || num < 1 || num > 99999)
            throw new Error('sheetNumber must be 1–99999');

          // HEXISTS is a single read — fast even at high concurrency
          if (await kv.hexists(HASH, String(num)))
            throw new Error(`Sheet #${num} already uploaded`);

          return { allowedContentTypes: ['application/pdf'] };
        },
        onUploadCompleted: async () => {},
      });
      return res.json(response);
    } catch(e) {
      return res.status(400).json({ error: e.message || 'Token generation failed' });
    }
  }

  const { action } = body;

  /* Register sheet after browser upload — HSETNX makes this atomic, safe under any concurrency */
  if (action === 'register') {
    if (await rateLimit(req, 'uploadsheet', 15000, 3600))
      return res.status(429).json({ error: 'Upload limit reached. Wait an hour.' });
    const { password, sheetNumber, filename, url, size, id } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!url || !sheetNumber || !filename || !id)
      return res.status(400).json({ error: 'url, sheetNumber, filename and id required' });

    const num = parseInt(sheetNumber);
    if (!num || num < 1 || num > 99999)
      return res.status(400).json({ error: 'sheetNumber must be 1–99999' });

    const sheet = { id, n: num, f: String(filename).trim().slice(0, 80), u: url, s: parseInt(size) || 0, ts: Date.now() };

    // HSETNX returns 1 if the field was created (success), 0 if it already existed (duplicate)
    const claimed = await kv.hsetnx(HASH, String(num), JSON.stringify(sheet));
    if (!claimed) return res.status(409).json({ error: `Sheet #${num} already uploaded` });

    // Store individual key for lookups by id
    await kv.set(`tb:mkt:sheet:${id}`, sheet);

    return res.json({ ok: true, sheet });
  }

  /* Bulk delete — pipeline keeps it to 2–3 Redis round-trips regardless of count */
  if (action === 'delete-bulk') {
    if (await rateLimit(req, 'deletesheet', 500, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, ids } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!Array.isArray(ids) || !ids.length)
      return res.status(400).json({ error: 'ids array required' });

    const all = await getAllSheets();
    const idSet = new Set(ids);
    const toDelete = all.filter(s => idSet.has(s.id));

    const blobUrls = toDelete.map(s => s.u).filter(Boolean);
    if (blobUrls.length) {
      try { await blobDel(blobUrls); } catch(e) {}
    }

    if (toDelete.length) {
      // HDEL with multiple fields = single Redis command
      await kv.hdel(HASH, ...toDelete.map(s => String(s.n)));
      // DEL with multiple keys = single Redis command
      await kv.del(...toDelete.map(s => `tb:mkt:sheet:${s.id}`));
    }

    return res.json({ ok: true, deleted: toDelete.length });
  }

  /* Delete single sheet */
  if (action === 'delete') {
    if (await rateLimit(req, 'deletesheet', 100, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, id } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!id) return res.status(400).json({ error: 'id required' });

    const sheet = await kv.get(`tb:mkt:sheet:${id}`);
    if (sheet?.u) { try { await blobDel(sheet.u); } catch(e) {} }
    await kv.del(`tb:mkt:sheet:${id}`);
    if (sheet?.n) await kv.hdel(HASH, String(sheet.n));
    return res.json({ ok: true });
  }

  /* Summary */
  if (action === 'summary') {
    const { password } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    const all = await getAllSheets();
    return res.json({ total: all.length, min: all[0]?.n || 0, max: all[all.length - 1]?.n || 0 });
  }

  return res.status(400).json({ error: 'Unknown action' });
};
