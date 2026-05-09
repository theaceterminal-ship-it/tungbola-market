const { del: blobDel } = require('@vercel/blob');
const { handleUpload } = require('@vercel/blob/client');
const { secureHeaders, rateLimit, checkPassword } = require('./_security');
const { db, sheetFromRow } = require('./_db');

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

    const { data } = await db().from('sheets').select('*').order('n');
    const sheets = (data || []).map(sheetFromRow);
    return res.json({ sheets, total: sheets.length });
  }

  if (req.method !== 'POST') return res.status(405).end();
  const body = req.body || {};

  /* Vercel Blob client upload — token request from browser */
  if (body.type === 'blob.generate-client-token' || body.type === 'blob.upload-completed') {
    if (body.type === 'blob.generate-client-token') {
      if (await rateLimit(req, 'uploadsheet', 15000, 3600))
        return res.status(429).json({ error: 'Upload limit reached. Wait an hour.' });
    }
    const headers = { get: name => req.headers[name.toLowerCase()] || null };
    try {
      const response = await handleUpload({
        body, request: { headers },
        onBeforeGenerateToken: async (pathname, clientPayload) => {
          let payload;
          try { payload = JSON.parse(clientPayload || '{}'); } catch(e) { throw new Error('Invalid payload'); }
          if (!checkPassword(payload.password, process.env.ADMIN_PASSWORD))
            throw new Error('Wrong password');
          const num = parseInt(payload.sheetNumber);
          if (!num || num < 1 || num > 99999) throw new Error('sheetNumber must be 1–99999');
          const { data: existing } = await db().from('sheets').select('id').eq('n', num).single();
          if (existing) throw new Error(`Sheet #${num} already uploaded`);
          return { allowedContentTypes: ['application/pdf'] };
        },
        onUploadCompleted: async () => {}
      });
      return res.json(response);
    } catch(e) { return res.status(400).json({ error: e.message || 'Token generation failed' }); }
  }

  const { action } = body;

  /* Register sheet after browser upload */
  if (action === 'register') {
    if (await rateLimit(req, 'uploadsheet', 15000, 3600))
      return res.status(429).json({ error: 'Upload limit reached. Wait an hour.' });
    const { password, sheetNumber, filename, url, size, id } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!url || !sheetNumber || !filename || !id)
      return res.status(400).json({ error: 'url, sheetNumber, filename and id required' });

    const num = parseInt(sheetNumber);
    if (!num || num < 1 || num > 99999) return res.status(400).json({ error: 'sheetNumber must be 1–99999' });

    const sheet = { id, n: num, f: String(filename).trim().slice(0, 80), u: url, s: parseInt(size) || 0, ts: Date.now() };

    // Use insert (not upsert) to enforce uniqueness on n
    const { error } = await db().from('sheets').insert(sheet);
    if (error?.code === '23505') return res.status(409).json({ error: `Sheet #${num} already uploaded` });
    if (error) return res.status(500).json({ error: error.message });

    return res.json({ ok: true, sheet: sheetFromRow(sheet) });
  }

  /* Bulk delete */
  if (action === 'delete-bulk') {
    if (await rateLimit(req, 'deletesheet', 500, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, ids } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!Array.isArray(ids) || !ids.length)
      return res.status(400).json({ error: 'ids array required' });

    const { data: toDelete } = await db().from('sheets').select('*').in('id', ids);
    const blobUrls = (toDelete || []).map(s => s.u).filter(Boolean);
    if (blobUrls.length) { try { await blobDel(blobUrls); } catch(e) {} }

    await db().from('sheets').delete().in('id', ids);
    return res.json({ ok: true, deleted: (toDelete || []).length });
  }

  /* Delete single sheet */
  if (action === 'delete') {
    if (await rateLimit(req, 'deletesheet', 100, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, id } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!id) return res.status(400).json({ error: 'id required' });

    const { data: sheet } = await db().from('sheets').select('u').eq('id', id).single();
    if (sheet?.u) { try { await blobDel(sheet.u); } catch(e) {} }
    await db().from('sheets').delete().eq('id', id);
    return res.json({ ok: true });
  }

  /* Summary */
  if (action === 'summary') {
    const { password } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    const { data } = await db().from('sheets').select('n').order('n');
    const nums = (data || []).map(s => s.n);
    return res.json({ total: nums.length, min: nums[0] || 0, max: nums[nums.length - 1] || 0 });
  }

  return res.status(400).json({ error: 'Unknown action' });
};
