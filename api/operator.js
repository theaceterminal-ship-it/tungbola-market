const { Redis } = require('@upstash/redis');
const { put } = require('@vercel/blob');
const { secureHeaders, rateLimit } = require('./_security');
const crypto = require('crypto');
const kv = Redis.fromEnv();

function genId() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
}
function genToken() {
  return Date.now().toString(36).toUpperCase() + Math.random().toString(36).slice(2, 9).toUpperCase();
}

async function getOperator(apiKey) {
  if (!apiKey) return null;
  const ops = await kv.get('tb:ops') || [];
  const summary = ops.find(o => o.apiKey === String(apiKey));
  if (!summary) return null;
  return kv.get(`tb:op:${summary.id}`);
}

module.exports = async function(req, res) {
  secureHeaders(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).end();

  const body = req.body || {};
  const { action, apiKey } = body;

  /* ── Public: get called numbers for a live game ── */
  if (action === 'get-live-game') {
    if (await rateLimit(req, 'getlive', 300, 60))
      return res.status(429).json({ error: 'Too many requests' });
    const { gameId } = body;
    if (!gameId) return res.status(400).json({ error: 'gameId required' });
    const state = await kv.get(`tb:live:${gameId}`) || { calledNumbers: [], lastNumber: null };
    return res.json({ ok: true, calledNumbers: state.calledNumbers, lastNumber: state.lastNumber, lastCalledAt: state.lastCalledAt });
  }

  if (!apiKey) return res.status(401).json({ error: 'apiKey required' });
  if (await rateLimit(req, `opapi:${String(apiKey).slice(0, 8)}`, 200, 60))
    return res.status(429).json({ error: 'Too many requests' });

  const operator = await getOperator(apiKey);
  if (!operator) return res.status(401).json({ error: 'Invalid API key' });

  /* ── Get operator info + their games ── */
  if (action === 'get-info') {
    const games = await kv.get('tb:mkt:games') || [];
    const myGames = games.filter(g => g.operatorId === operator.id);
    return res.json({ ok: true, operator: { id: operator.id, name: operator.name, plan: operator.plan }, games: myGames });
  }

  /* ── Get purchases for operator's games ── */
  if (action === 'get-purchases') {
    const games = await kv.get('tb:mkt:games') || [];
    const myGameIds = new Set(games.filter(g => g.operatorId === operator.id).map(g => g.id));
    const purchases = await kv.get('tb:mkt:purchases') || [];
    return res.json({ ok: true, purchases: purchases.filter(p => myGameIds.has(p.gameId)) });
  }

  /* ── Create game → published to marketplace ── */
  if (action === 'create-game') {
    if (await rateLimit(req, `op-cg:${operator.id}`, 30, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { name, gameDate, gameDateRaw, pricePerSheet, description, prizes, thumbnail } = body;
    if (!name) return res.status(400).json({ error: 'Game name required' });

    const id = genId();
    const game = {
      id, operatorId: operator.id, operatorName: operator.name,
      name: String(name).trim().slice(0, 80),
      gameDate: gameDate ? String(gameDate).trim().slice(0, 40) : null,
      gameDateRaw: gameDateRaw ? String(gameDateRaw).trim().slice(0, 30) : null,
      pricePerSheet: Math.max(1, Number(pricePerSheet) || 5),
      description: String(description || '').trim().slice(0, 200),
      prizes: Array.isArray(prizes) ? prizes.slice(0, 12) : [],
      thumbnail: thumbnail ? String(thumbnail).slice(0, 500) : null,
      status: 'draft', sheetFrom: 0, sheetTo: 0, sheetCount: 0, soldCount: 0,
      createdAt: Date.now()
    };
    await kv.set(`tb:mkt:game:${id}`, game);
    const games = await kv.get('tb:mkt:games') || [];
    games.unshift(game);
    await kv.set('tb:mkt:games', games.slice(0, 200));
    return res.json({ ok: true, game });
  }

  /* ── Edit game ── */
  if (action === 'edit-game') {
    const { gameId, name, gameDate, gameDateRaw, pricePerSheet, description, prizes, thumbnail } = body;
    if (!gameId) return res.status(400).json({ error: 'gameId required' });
    const game = await kv.get(`tb:mkt:game:${gameId}`);
    if (!game) return res.status(404).json({ error: 'Game not found' });
    if (game.operatorId !== operator.id) return res.status(403).json({ error: 'Not your game' });

    if (name !== undefined) game.name = String(name).trim().slice(0, 80);
    if (gameDate !== undefined) game.gameDate = gameDate ? String(gameDate).trim().slice(0, 40) : null;
    if (gameDateRaw !== undefined) game.gameDateRaw = gameDateRaw ? String(gameDateRaw).trim().slice(0, 30) : null;
    if (pricePerSheet !== undefined) game.pricePerSheet = Math.max(1, Number(pricePerSheet) || 5);
    if (description !== undefined) game.description = String(description || '').trim().slice(0, 200);
    if (prizes !== undefined) game.prizes = Array.isArray(prizes) ? prizes.slice(0, 12) : [];
    if (thumbnail !== undefined) game.thumbnail = thumbnail ? String(thumbnail).slice(0, 500) : null;

    await kv.set(`tb:mkt:game:${gameId}`, game);
    const games = await kv.get('tb:mkt:games') || [];
    const idx = games.findIndex(g => g.id === gameId);
    if (idx !== -1) { games[idx] = game; await kv.set('tb:mkt:games', games); }
    return res.json({ ok: true, game });
  }

  /* ── Assign sheets to game ── */
  if (action === 'assign-sheets') {
    const { gameId, sheetFrom, sheetTo } = body;
    if (!gameId || !sheetFrom || !sheetTo)
      return res.status(400).json({ error: 'gameId, sheetFrom, sheetTo required' });
    const game = await kv.get(`tb:mkt:game:${gameId}`);
    if (!game) return res.status(404).json({ error: 'Game not found' });
    if (game.operatorId !== operator.id) return res.status(403).json({ error: 'Not your game' });

    const from = parseInt(sheetFrom), to = parseInt(sheetTo);
    if (from < 1 || to < from) return res.status(400).json({ error: 'Invalid range' });

    // Plan A: use operator's own sheet library; Plan B: use shared sheets
    const hashKey = operator.plan === 'own-sheets'
      ? `tb:op:${operator.id}:sheets`
      : 'tb:mkt:sheets:hash';
    const raw = await kv.hgetall(hashKey) || {};
    const allSheets = Object.values(raw)
      .map(v => { try { return typeof v === 'string' ? JSON.parse(v) : v; } catch(e) { return null; } })
      .filter(Boolean);
    const inRange = allSheets.filter(s => s.n >= from && s.n <= to);
    if (!inRange.length) return res.status(400).json({ error: `No sheets in range ${from}–${to}` });

    game.sheetFrom = from; game.sheetTo = to;
    game.sheetCount = inRange.length; game.soldCount = 0; game.soldSheetNums = [];
    await kv.set(`tb:mkt:game:${gameId}`, game);

    const games = await kv.get('tb:mkt:games') || [];
    const idx = games.findIndex(g => g.id === gameId);
    const compact = {
      id: game.id, operatorId: game.operatorId, operatorName: game.operatorName,
      name: game.name, gameDate: game.gameDate, pricePerSheet: game.pricePerSheet,
      description: game.description, prizes: game.prizes, thumbnail: game.thumbnail || null,
      status: game.status, sheetCount: game.sheetCount, soldCount: game.soldCount, createdAt: game.createdAt
    };
    if (idx >= 0) games[idx] = compact; else games.unshift(compact);
    await kv.set('tb:mkt:games', games.slice(0, 200));
    return res.json({ ok: true, game, sheetsFound: inRange.length });
  }

  /* ── Set game status (list / unlist / end) ── */
  if (action === 'set-status') {
    const { gameId, status } = body;
    if (!['listed', 'ended', 'draft'].includes(status))
      return res.status(400).json({ error: 'status must be listed, ended or draft' });
    const game = await kv.get(`tb:mkt:game:${gameId}`);
    if (!game) return res.status(404).json({ error: 'Game not found' });
    if (game.operatorId !== operator.id) return res.status(403).json({ error: 'Not your game' });
    if (status === 'listed' && !game.sheetCount)
      return res.status(400).json({ error: 'Assign sheets before listing' });

    game.status = status;
    await kv.set(`tb:mkt:game:${gameId}`, game);
    const games = await kv.get('tb:mkt:games') || [];
    const idx = games.findIndex(g => g.id === gameId);
    if (idx >= 0) { games[idx].status = status; await kv.set('tb:mkt:games', games); }
    return res.json({ ok: true });
  }

  /* ── Delete game ── */
  if (action === 'delete-game') {
    const { gameId } = body;
    const game = await kv.get(`tb:mkt:game:${gameId}`);
    if (!game) return res.status(404).json({ error: 'Game not found' });
    if (game.operatorId !== operator.id) return res.status(403).json({ error: 'Not your game' });

    await kv.del(`tb:mkt:game:${gameId}`);
    const games = await kv.get('tb:mkt:games') || [];
    await kv.set('tb:mkt:games', games.filter(g => g.id !== gameId));
    const purchases = await kv.get('tb:mkt:purchases') || [];
    const toDelete = purchases.filter(p => p.gameId === gameId);
    await kv.set('tb:mkt:purchases', purchases.filter(p => p.gameId !== gameId));
    await Promise.all(toDelete.map(p => kv.del(`tb:mkt:purchase:${p.purchaseId}`)));
    return res.json({ ok: true });
  }

  /* ── Approve purchase ── */
  if (action === 'approve-purchase') {
    const { purchaseId } = body;
    if (!purchaseId) return res.status(400).json({ error: 'purchaseId required' });
    const purchase = await kv.get(`tb:mkt:purchase:${purchaseId}`);
    if (!purchase) return res.status(404).json({ error: 'Purchase not found' });
    if (purchase.status !== 'pending') return res.status(409).json({ error: 'Already processed' });
    const game = await kv.get(`tb:mkt:game:${purchase.gameId}`);
    if (!game) return res.status(404).json({ error: 'Game not found' });
    if (game.operatorId !== operator.id) return res.status(403).json({ error: 'Not your game' });

    const hashKey = operator.plan === 'own-sheets'
      ? `tb:op:${operator.id}:sheets`
      : 'tb:mkt:sheets:hash';
    const raw = await kv.hgetall(hashKey) || {};
    const allSheets = Object.values(raw)
      .map(v => { try { return typeof v === 'string' ? JSON.parse(v) : v; } catch(e) { return null; } })
      .filter(Boolean);
    const soldNums = new Set(game.soldSheetNums || []);
    const available = allSheets.filter(s => s.n >= game.sheetFrom && s.n <= game.sheetTo && !soldNums.has(s.n));
    if (available.length < purchase.quantity)
      return res.status(409).json({ error: `Only ${available.length} sheets left` });

    let assigned;
    if (purchase.requestedSheetNums?.length) {
      const reqSet = new Set(purchase.requestedSheetNums);
      assigned = [...available.filter(s => reqSet.has(s.n)), ...available.filter(s => !reqSet.has(s.n))].slice(0, purchase.quantity);
    } else {
      assigned = available.slice(0, purchase.quantity);
    }

    const dlToken = genToken();
    await kv.set(`tb:mkt:dl:${dlToken}`, { sheets: assigned.map(s => ({ n: s.n, filename: s.f, url: s.u })), gameName: game.name, purchaseId }, { ex: 172800 });

    const newSold = [...(game.soldSheetNums || []), ...assigned.map(s => s.n)];
    game.soldSheetNums = newSold; game.soldCount = newSold.length;
    await kv.set(`tb:mkt:game:${purchase.gameId}`, game);
    const games = await kv.get('tb:mkt:games') || [];
    const gIdx = games.findIndex(g => g.id === purchase.gameId);
    if (gIdx >= 0) { games[gIdx].soldCount = game.soldCount; await kv.set('tb:mkt:games', games); }

    purchase.status = 'approved'; purchase.downloadToken = dlToken;
    purchase.approvedAt = Date.now(); purchase.sheetNums = assigned.map(s => s.n);
    await kv.set(`tb:mkt:purchase:${purchaseId}`, purchase, { ex: 172800 });
    const plist = await kv.get('tb:mkt:purchases') || [];
    const pIdx = plist.findIndex(p => p.purchaseId === purchaseId);
    if (pIdx >= 0) { plist[pIdx] = purchase; await kv.set('tb:mkt:purchases', plist); }

    return res.json({ ok: true, downloadToken: dlToken, sheetsAssigned: assigned.length });
  }

  /* ── Call a number → syncs to Tungbola players ── */
  if (action === 'call-number') {
    const { gameId, number } = body;
    if (!gameId) return res.status(400).json({ error: 'gameId required' });
    const num = parseInt(number);
    if (!num || num < 1 || num > 90) return res.status(400).json({ error: 'Number must be 1–90' });
    const game = await kv.get(`tb:mkt:game:${gameId}`);
    if (!game) return res.status(404).json({ error: 'Game not found' });
    if (game.operatorId !== operator.id) return res.status(403).json({ error: 'Not your game' });

    const liveKey = `tb:live:${gameId}`;
    const state = await kv.get(liveKey) || { calledNumbers: [], gameId };
    if (!state.calledNumbers.includes(num)) {
      state.calledNumbers.push(num);
      state.lastNumber = num;
      state.lastCalledAt = Date.now();
    }
    await kv.set(liveKey, state, { ex: 7200 });
    return res.json({ ok: true, calledNumbers: state.calledNumbers, lastNumber: num });
  }

  /* ── Reset live game state ── */
  if (action === 'reset-live') {
    const { gameId } = body;
    const game = await kv.get(`tb:mkt:game:${gameId}`);
    if (!game) return res.status(404).json({ error: 'Game not found' });
    if (game.operatorId !== operator.id) return res.status(403).json({ error: 'Not your game' });
    await kv.del(`tb:live:${gameId}`);
    return res.json({ ok: true });
  }

  /* ── Upload sheet to operator's own library (Plan A only) ── */
  if (action === 'upload-sheet') {
    if (operator.plan !== 'own-sheets')
      return res.status(403).json({ error: 'Own Sheets plan required' });
    const { sheetNum, data, filename } = body;
    if (!sheetNum || !data) return res.status(400).json({ error: 'sheetNum and data required' });
    const num = parseInt(sheetNum);
    if (!num || num < 1 || num > 9999) return res.status(400).json({ error: 'Invalid sheet number (1–9999)' });
    let buffer;
    try { buffer = Buffer.from(data, 'base64'); }
    catch(e) { return res.status(400).json({ error: 'Invalid base64' }); }
    if (buffer.length > 5 * 1024 * 1024) return res.status(400).json({ error: 'File too large (max 5MB)' });
    const ext = String(filename || 'sheet.pdf').split('.').pop().toLowerCase();
    const ct = ext === 'pdf' ? 'application/pdf' : 'image/jpeg';
    try {
      const blob = await put(`tungbola/op-${operator.id}/sheet-${num}.${ext}`, buffer, { access: 'public', contentType: ct });
      const rec = { n: num, f: filename || `sheet-${num}.${ext}`, u: blob.url };
      await kv.hset(`tb:op:${operator.id}:sheets`, { [`s${num}`]: JSON.stringify(rec) });
      return res.json({ ok: true, url: blob.url, n: num });
    } catch(e) {
      return res.status(500).json({ error: `Upload failed: ${e.message}` });
    }
  }

  /* ── Sheet library stats ── */
  if (action === 'sheet-library-stats') {
    const raw = await kv.hgetall(`tb:op:${operator.id}:sheets`) || {};
    return res.json({ ok: true, count: Object.keys(raw).length, plan: operator.plan });
  }

  return res.status(400).json({ error: 'Unknown action' });
};
