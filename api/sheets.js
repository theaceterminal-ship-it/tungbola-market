const { Redis } = require('@upstash/redis');
const { put, del: blobDel } = require('@vercel/blob');
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
    const PAGE = 100;
    const p = Math.max(0, parseInt(page) || 0);
    const slice = all.slice(p * PAGE, (p + 1) * PAGE);
    return res.json({ sheets: slice, total: all.length, page: p, pages: Math.ceil(all.length / PAGE) });
  }

  if (req.method !== 'POST') return res.status(405).end();
  const body = req.body || {};
  const { action } = body;

  /* Admin: upload a sheet PDF */
  if (action === 'upload') {
    if (await rateLimit(req, 'uploadsheet', 200, 3600))
      return res.status(429).json({ error: 'Upload limit reached. Wait an hour.' });
    const { password, filename, sheetNumber, data } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!data || !filename || !sheetNumber)
      return res.status(400).json({ error: 'filename, sheetNumber and data required' });

    const num = parseInt(sheetNumber);
    if (!num || num < 1 || num > 99999)
      return res.status(400).json({ error: 'sheetNumber must be 1–99999' });

    // Check for duplicate sheet number
    const all = await kv.get('tb:mkt:sheets') || [];
    if (all.find(s => s.n === num))
      return res.status(409).json({ error: `Sheet #${num} already uploaded` });

    // Decode base64 PDF and upload to Vercel Blob
    let buffer;
    try {
      buffer = Buffer.from(data, 'base64');
    } catch(e) {
      return res.status(400).json({ error: 'Invalid base64 data' });
    }
    if (buffer.length > 4 * 1024 * 1024)
      return res.status(400).json({ error: 'File too large (max 4MB)' });

    const id = genId();
    const blobName = `tungbola/sheet-${String(num).padStart(5, '0')}-${id}.pdf`;

    let blobUrl;
    try {
      const blob = await put(blobName, buffer, { access: 'public', contentType: 'application/pdf' });
      blobUrl = blob.url;
    } catch(e) {
      console.error('Blob upload error:', e);
      return res.status(500).json({ error: 'Storage upload failed. Check BLOB_READ_WRITE_TOKEN.' });
    }

    const sheet = { id, n: num, f: String(filename).trim().slice(0, 80), u: blobUrl, s: buffer.length, ts: Date.now() };
    await kv.set(`tb:mkt:sheet:${id}`, sheet);
    all.push(sheet);
    all.sort((a, b) => a.n - b.n);
    await kv.set('tb:mkt:sheets', all.slice(0, 5500));

    return res.json({ ok: true, sheet });
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
