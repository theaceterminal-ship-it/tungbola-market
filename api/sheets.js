const { Redis } = require('@upstash/redis');
const { del: blobDel } = require('@vercel/blob');
const { handleUpload } = require('@vercel/blob/client');
const { secureHeaders, rateLimit, checkPassword } = require('./_security');
const kv = Redis.fromEnv();

function genId() {
  return Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4);
}

module.exports = async function(req, res) {
  secureHeaders(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  /* GET: admin lists sheets (paginated) */
  if (req.method === 'GET') {
    if (await rateLimit(req, 'listsheets', 60, 60))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, page } = req.query;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });

    const all = await kv.get('tb:mkt:sheets') || [];
    if (req.query.all === 'true') return res.json({ sheets: all, total: all.length });
    const PAGE = 100;
    const p = Math.max(0, parseInt(page) || 0);
    const slice = all.slice(p * PAGE, (p + 1) * PAGE);
    return res.json({ sheets: slice, total: all.length, page: p, pages: Math.ceil(all.length / PAGE) });
  }

  if (req.method !== 'POST') return res.status(405).end();
  const body = req.body || {};

  /* Vercel Blob client upload protocol — called internally by @vercel/blob/client in the browser */
  if (body.type === 'blob.generate-client-token' || body.type === 'blob.upload-completed') {
    if (body.type === 'blob.generate-client-token') {
      if (await rateLimit(req, 'uploadsheet', 15000, 3600))
        return res.status(429).json({ error: 'Upload limit reached. Wait an hour.' });
    }

    // Wrap Node.js headers so handleUpload can call headers.get()
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

          const all = await kv.get('tb:mkt:sheets') || [];
          if (all.find(s => s.n === num))
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

  /* Step 2: Browser upload is done — save URL + metadata to Redis */
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

    const all = await kv.get('tb:mkt:sheets') || [];
    if (all.find(s => s.n === num))
      return res.status(409).json({ error: `Sheet #${num} already uploaded` });

    const sheet = { id, n: num, f: String(filename).trim().slice(0, 80), u: url, s: parseInt(size) || 0, ts: Date.now() };
    await kv.set(`tb:mkt:sheet:${id}`, sheet);
    all.push(sheet);
    all.sort((a, b) => a.n - b.n);
    await kv.set('tb:mkt:sheets', all.slice(0, 5500));

    return res.json({ ok: true, sheet });
  }

  /* Admin: bulk delete sheets by ID array */
  if (action === 'delete-bulk') {
    if (await rateLimit(req, 'deletesheet', 500, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, ids } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!Array.isArray(ids) || !ids.length)
      return res.status(400).json({ error: 'ids array required' });

    const all = await kv.get('tb:mkt:sheets') || [];
    const idSet = new Set(ids);
    const toDelete = all.filter(s => idSet.has(s.id));
    const remaining = all.filter(s => !idSet.has(s.id));

    const blobUrls = toDelete.map(s => s.u).filter(Boolean);
    if (blobUrls.length) {
      try { await blobDel(blobUrls); } catch(e) {}
    }

    if (toDelete.length) {
      const pipeline = kv.pipeline();
      toDelete.forEach(s => pipeline.del(`tb:mkt:sheet:${s.id}`));
      await pipeline.exec();
    }

    await kv.set('tb:mkt:sheets', remaining);
    return res.json({ ok: true, deleted: toDelete.length });
  }

  /* Admin: delete a sheet */
  if (action === 'delete') {
    if (await rateLimit(req, 'deletesheet', 100, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, id } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!id) return res.status(400).json({ error: 'id required' });

    const sheet = await kv.get(`tb:mkt:sheet:${id}`);
    if (sheet?.u) {
      try { await blobDel(sheet.u); } catch(e) {}
    }
    await kv.del(`tb:mkt:sheet:${id}`);
    const all = (await kv.get('tb:mkt:sheets') || []).filter(s => s.id !== id);
    await kv.set('tb:mkt:sheets', all);
    return res.json({ ok: true });
  }

  /* Admin: get sheet count summary */
  if (action === 'summary') {
    const { password } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    const all = await kv.get('tb:mkt:sheets') || [];
    return res.json({ total: all.length, min: all[0]?.n || 0, max: all[all.length - 1]?.n || 0 });
  }

  return res.status(400).json({ error: 'Unknown action' });
};
