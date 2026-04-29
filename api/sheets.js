const { Redis } = require('@upstash/redis');
const { del: blobDel } = require('@vercel/blob');
const { generateClientTokenFromReadWriteToken } = require('@vercel/blob/client');
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

  /* Step 1: Generate a short-lived client upload token — no PDF data touches the server */
  if (action === 'generate-upload-token') {
    if (await rateLimit(req, 'uploadsheet', 200, 3600))
      return res.status(429).json({ error: 'Upload limit reached. Wait an hour.' });
    const { password, sheetNumber, filename } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!filename || !sheetNumber)
      return res.status(400).json({ error: 'filename and sheetNumber required' });

    const num = parseInt(sheetNumber);
    if (!num || num < 1 || num > 99999)
      return res.status(400).json({ error: 'sheetNumber must be 1–99999' });

    const all = await kv.get('tb:mkt:sheets') || [];
    if (all.find(s => s.n === num))
      return res.status(409).json({ error: `Sheet #${num} already uploaded` });

    const id = genId();
    const pathname = `tungbola/sheet-${String(num).padStart(5, '0')}-${id}.pdf`;

    let token;
    try {
      token = await generateClientTokenFromReadWriteToken({ pathname });
    } catch(e) {
      console.error('Token generation error:', e);
      return res.status(500).json({ error: 'Failed to generate upload token. Check BLOB_READ_WRITE_TOKEN.' });
    }

    return res.json({ token, pathname, id });
  }

  /* Step 3: Browser upload is done — save URL + metadata to Redis */
  if (action === 'register') {
    if (await rateLimit(req, 'uploadsheet', 200, 3600))
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
