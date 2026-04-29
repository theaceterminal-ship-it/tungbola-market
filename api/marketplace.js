const { Redis } = require('@upstash/redis');
const { put } = require('@vercel/blob');
const { secureHeaders, rateLimit, checkPassword } = require('./_security');
const kv = Redis.fromEnv();

function genId() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
}
function genToken() {
  return Date.now().toString(36).toUpperCase() + Math.random().toString(36).slice(2, 9).toUpperCase();
}

module.exports = async function(req, res) {
  secureHeaders(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  /* GET: public game listing OR admin dashboard */
  if (req.method === 'GET') {
    if (await rateLimit(req, 'mktget', 120, 60))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, type } = req.query;

    if (!password) {
      const games = await kv.get('tb:mkt:games') || [];
      return res.json({ games: games.filter(g => g.status === 'listed') });
    }

    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });

    if (type === 'purchases') {
      const purchases = await kv.get('tb:mkt:purchases') || [];
      return res.json({ purchases });
    }
    return res.json({ games: await kv.get('tb:mkt:games') || [] });
  }

  if (req.method !== 'POST') return res.status(405).end();
  const body = req.body || {};
  const { action } = body;

  /* ── Admin: upload prize image ── */
  if (action === 'upload-prize-image') {
    if (await rateLimit(req, 'prizeimg', 60, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, data, filename } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!data) return res.status(400).json({ error: 'data required' });

    let buffer;
    try { buffer = Buffer.from(data, 'base64'); }
    catch(e) { return res.status(400).json({ error: 'Invalid base64' }); }
    if (buffer.length > 5 * 1024 * 1024)
      return res.status(400).json({ error: 'Image too large (max 5MB)' });

    const ext = String(filename || 'image.jpg').split('.').pop().toLowerCase();
    const ct = ext === 'png' ? 'image/png' : ext === 'gif' ? 'image/gif' : ext === 'webp' ? 'image/webp' : 'image/jpeg';
    try {
      const blob = await put(`tungbola/prize-${Date.now()}.${ext}`, buffer, { access: 'public', contentType: ct });
      return res.json({ ok: true, url: blob.url });
    } catch(e) {
      console.error('Prize image upload error:', e);
      return res.status(500).json({ error: `Upload failed: ${e.message || String(e)}` });
    }
  }

  /* ── Admin: create a game ── */
  if (action === 'create-game') {
    if (await rateLimit(req, 'creategame', 30, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, name, gameDate, pricePerSheet, description, prizes, thumbnail } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!name) return res.status(400).json({ error: 'Game name required' });

    const id = genId();
    const game = {
      id,
      name: String(name).trim().slice(0, 80),
      gameDate: gameDate ? String(gameDate).trim().slice(0, 40) : null,
      pricePerSheet: Math.max(1, Number(pricePerSheet) || 5),
      description: String(description || '').trim().slice(0, 200),
      prizes: Array.isArray(prizes) ? prizes.slice(0, 12) : [],
      thumbnail: thumbnail ? String(thumbnail).slice(0, 500) : null,
      status: 'draft',
      sheetFrom: 0, sheetTo: 0, sheetCount: 0, soldCount: 0,
      createdAt: Date.now()
    };
    await kv.set(`tb:mkt:game:${id}`, game);
    const games = await kv.get('tb:mkt:games') || [];
    games.unshift(game);
    await kv.set('tb:mkt:games', games.slice(0, 200));
    return res.json({ ok: true, game });
  }

  /* ── Admin: assign sheet range ── */
  if (action === 'assign-sheets') {
    if (await rateLimit(req, 'assignsheets', 60, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, gameId, sheetFrom, sheetTo } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!gameId || !sheetFrom || !sheetTo)
      return res.status(400).json({ error: 'gameId, sheetFrom, sheetTo required' });

    const from = parseInt(sheetFrom), to = parseInt(sheetTo);
    if (from < 1 || to < from) return res.status(400).json({ error: 'Invalid range' });

    const game = await kv.get(`tb:mkt:game:${gameId}`);
    if (!game) return res.status(404).json({ error: 'Game not found' });
    if (game.status === 'ended') return res.status(409).json({ error: 'Game has ended' });

    const sheetsData = await kv.hgetall('tb:mkt:sheets:hash') || {};
    const allSheets = Object.values(sheetsData).map(v => { try { return typeof v === 'string' ? JSON.parse(v) : v; } catch(e) { return null; } }).filter(Boolean);
    const inRange = allSheets.filter(s => s.n >= from && s.n <= to);
    if (!inRange.length) return res.status(400).json({ error: `No uploaded sheets in range ${from}–${to}` });

    game.sheetFrom = from; game.sheetTo = to;
    game.sheetCount = inRange.length; game.soldCount = 0; game.soldSheetNums = [];

    await kv.set(`tb:mkt:game:${gameId}`, game);
    const games = await kv.get('tb:mkt:games') || [];
    const idx = games.findIndex(g => g.id === gameId);
    const compact = { id: game.id, name: game.name, gameDate: game.gameDate,
      pricePerSheet: game.pricePerSheet, description: game.description, prizes: game.prizes,
      thumbnail: game.thumbnail || null,
      status: game.status, sheetCount: game.sheetCount, soldCount: game.soldCount, createdAt: game.createdAt };
    if (idx >= 0) games[idx] = compact; else games.unshift(compact);
    await kv.set('tb:mkt:games', games.slice(0, 200));
    return res.json({ ok: true, game, sheetsFound: inRange.length });
  }

  /* ── Admin: set game status ── */
  if (action === 'set-status') {
    const { password, gameId, status } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!['listed', 'ended', 'draft'].includes(status))
      return res.status(400).json({ error: 'status must be listed, ended or draft' });

    const game = await kv.get(`tb:mkt:game:${gameId}`);
    if (!game) return res.status(404).json({ error: 'Game not found' });
    if (status === 'listed' && !game.sheetCount)
      return res.status(400).json({ error: 'Assign sheets before listing the game' });

    game.status = status;
    await kv.set(`tb:mkt:game:${gameId}`, game);
    const games = await kv.get('tb:mkt:games') || [];
    const idx = games.findIndex(g => g.id === gameId);
    if (idx >= 0) { games[idx].status = status; await kv.set('tb:mkt:games', games); }
    return res.json({ ok: true });
  }

  /* ── Player: purchase sheets ── */
  if (action === 'purchase') {
    if (await rateLimit(req, 'mktbuy', 10, 3600))
      return res.status(429).json({ error: 'Too many requests. Try again later.' });
    const { playerName, phone, gameId, quantity, requestedSheetNums } = body;
    if (!playerName || !gameId || !quantity)
      return res.status(400).json({ error: 'playerName, gameId, quantity required' });

    const games = await kv.get('tb:mkt:games') || [];
    const gameMeta = games.find(g => g.id === gameId);
    if (!gameMeta || gameMeta.status !== 'listed')
      return res.status(404).json({ error: 'Game not available for purchase' });

    const qty = Math.max(1, Math.min(150, parseInt(quantity) || 1));
    const available = gameMeta.sheetCount - (gameMeta.soldCount || 0);
    if (available < qty)
      return res.status(409).json({ error: `Only ${available} sheet${available !== 1 ? 's' : ''} available` });

    const amount = gameMeta.pricePerSheet * qty;
    const purchaseId = genId();
    const reqNums = Array.isArray(requestedSheetNums) && requestedSheetNums.length
      ? requestedSheetNums.slice(0, 150).map(Number).filter(n => n >= gameMeta.sheetFrom && n <= gameMeta.sheetTo)
      : null;
    const purchase = {
      purchaseId, playerName: String(playerName).trim().slice(0, 50),
      phone: String(phone || '').trim().slice(0, 20),
      gameId, gameName: gameMeta.name, quantity: qty, amount,
      requestedSheetNums: reqNums,
      status: 'pending', createdAt: Date.now()
    };
    await kv.set(`tb:mkt:purchase:${purchaseId}`, purchase, { ex: 86400 });
    const purchases = await kv.get('tb:mkt:purchases') || [];
    purchases.unshift(purchase);
    await kv.set('tb:mkt:purchases', purchases.slice(0, 500));
    return res.json({ ok: true, purchaseId, amount, playerName: purchase.playerName, gameName: gameMeta.name });
  }

  /* ── Player: get available sheet numbers for a game ── */
  if (action === 'available-sheets') {
    if (await rateLimit(req, 'availsheets', 120, 60))
      return res.status(429).json({ error: 'Too many requests' });
    const { gameId } = body;
    if (!gameId) return res.status(400).json({ error: 'gameId required' });

    const game = await kv.get(`tb:mkt:game:${gameId}`);
    if (!game || game.status !== 'listed')
      return res.status(404).json({ error: 'Game not found' });

    const sheetsData = await kv.hgetall('tb:mkt:sheets:hash') || {};
    const allSheets = Object.values(sheetsData)
      .map(v => { try { return typeof v === 'string' ? JSON.parse(v) : v; } catch(e) { return null; } })
      .filter(Boolean);

    const inRange = allSheets.filter(s => s.n >= game.sheetFrom && s.n <= game.sheetTo);
    const soldSet = new Set(game.soldSheetNums || []);
    const available = inRange.filter(s => !soldSet.has(s.n)).map(s => s.n).sort((a, b) => a - b);
    const sold = inRange.filter(s => soldSet.has(s.n)).map(s => s.n).sort((a, b) => a - b);

    return res.json({ available, sold, total: inRange.length });
  }

  /* ── Player: poll purchase status ── */
  if (action === 'check-purchase') {
    if (await rateLimit(req, 'checkpurchase', 120, 60))
      return res.status(429).json({ error: 'Too many requests' });
    const { purchaseId } = body;
    if (!purchaseId) return res.status(400).json({ error: 'purchaseId required' });
    const purchase = await kv.get(`tb:mkt:purchase:${purchaseId}`);
    if (!purchase) return res.status(404).json({ error: 'Purchase not found or expired' });
    if (purchase.status === 'approved')
      return res.json({ status: 'approved', downloadToken: purchase.downloadToken, quantity: purchase.quantity });
    return res.json({ status: purchase.status });
  }

  /* ── Player: get downloads using token ── */
  if (action === 'get-downloads') {
    if (await rateLimit(req, 'getdl', 60, 300))
      return res.status(429).json({ error: 'Too many requests' });
    const { downloadToken } = body;
    if (!downloadToken) return res.status(400).json({ error: 'Token required' });
    const dl = await kv.get(`tb:mkt:dl:${downloadToken}`);
    if (!dl) return res.status(404).json({ error: 'Download link expired or invalid. Contact admin.' });
    return res.json({ ok: true, sheets: dl.sheets, gameName: dl.gameName });
  }

  /* ── Admin: approve purchase → assign sheets → generate download token ── */
  if (action === 'approve-purchase') {
    if (await rateLimit(req, 'approvepurchase', 60, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, purchaseId } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!purchaseId) return res.status(400).json({ error: 'purchaseId required' });

    const purchase = await kv.get(`tb:mkt:purchase:${purchaseId}`);
    if (!purchase) return res.status(404).json({ error: 'Purchase not found' });
    if (purchase.status !== 'pending') return res.status(409).json({ error: 'Already processed' });

    const game = await kv.get(`tb:mkt:game:${purchase.gameId}`);
    if (!game) return res.status(404).json({ error: 'Game not found' });

    const sheetsData2 = await kv.hgetall('tb:mkt:sheets:hash') || {};
    const allSheets = Object.values(sheetsData2).map(v => { try { return typeof v === 'string' ? JSON.parse(v) : v; } catch(e) { return null; } }).filter(Boolean);
    const soldNums = new Set(game.soldSheetNums || []);
    const available = allSheets.filter(s => s.n >= game.sheetFrom && s.n <= game.sheetTo && !soldNums.has(s.n));
    if (available.length < purchase.quantity)
      return res.status(409).json({ error: `Only ${available.length} sheets left in this game` });

    // Honor player's requested sheet numbers if they specified any
    let assigned;
    if (purchase.requestedSheetNums && purchase.requestedSheetNums.length) {
      const reqSet = new Set(purchase.requestedSheetNums);
      const requested = available.filter(s => reqSet.has(s.n));
      const rest = available.filter(s => !reqSet.has(s.n));
      assigned = [...requested, ...rest].slice(0, purchase.quantity);
    } else {
      assigned = available.slice(0, purchase.quantity);
    }
    const sheetList = assigned.map(s => ({ n: s.n, filename: s.f, url: s.u }));

    const dlToken = genToken();
    await kv.set(`tb:mkt:dl:${dlToken}`, { sheets: sheetList, gameName: game.name, purchaseId }, { ex: 172800 });

    const newSold = [...(game.soldSheetNums || []), ...assigned.map(s => s.n)];
    game.soldSheetNums = newSold; game.soldCount = newSold.length;
    await kv.set(`tb:mkt:game:${purchase.gameId}`, game);

    const games = await kv.get('tb:mkt:games') || [];
    const gIdx = games.findIndex(g => g.id === purchase.gameId);
    if (gIdx >= 0) { games[gIdx].soldCount = game.soldCount; await kv.set('tb:mkt:games', games); }

    purchase.status = 'approved'; purchase.downloadToken = dlToken;
    purchase.approvedAt = Date.now();
    purchase.sheetNums = assigned.map(s => s.n);
    await kv.set(`tb:mkt:purchase:${purchaseId}`, purchase, { ex: 172800 });

    const plist = await kv.get('tb:mkt:purchases') || [];
    const pIdx = plist.findIndex(p => p.purchaseId === purchaseId);
    if (pIdx >= 0) { plist[pIdx] = purchase; await kv.set('tb:mkt:purchases', plist); }

    return res.json({ ok: true, downloadToken: dlToken, sheetsAssigned: assigned.length });
  }

  /* ── Admin: delete a game ── */
  if (action === 'delete-game') {
    const { password, gameId } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!gameId) return res.status(400).json({ error: 'gameId required' });

    await kv.del(`tb:mkt:game:${gameId}`);
    const games = await kv.get('tb:mkt:games') || [];
    const filtered = games.filter(g => g.id !== gameId);
    await kv.set('tb:mkt:games', filtered);
    return res.json({ ok: true });
  }

  return res.status(400).json({ error: 'Unknown action' });
};
