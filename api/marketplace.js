const { Redis } = require('@upstash/redis');
const { put } = require('@vercel/blob');
const { secureHeaders, rateLimit, checkPassword } = require('./_security');
const crypto = require('crypto');
const kv = Redis.fromEnv();

async function sendPushVapid(subscription) {
  if (!process.env.VAPID_PUBLIC_KEY || !process.env.VAPID_PRIVATE_KEY) return;
  try {
    const endpoint = new URL(subscription.endpoint);
    const subtle = (globalThis.crypto || crypto.webcrypto).subtle;
    const now = Math.floor(Date.now() / 1000);
    const h = Buffer.from(JSON.stringify({ typ: 'JWT', alg: 'ES256' })).toString('base64url');
    const c = Buffer.from(JSON.stringify({ aud: endpoint.origin, exp: now + 43200, sub: 'mailto:admin@tungbola.com' })).toString('base64url');
    const sigInput = Buffer.from(`${h}.${c}`);
    const pubBytes = Buffer.from(process.env.VAPID_PUBLIC_KEY, 'base64url');
    const x = pubBytes.slice(1, 33).toString('base64url');
    const y = pubBytes.slice(33, 65).toString('base64url');
    const privKey = await subtle.importKey('jwk',
      { kty: 'EC', crv: 'P-256', d: process.env.VAPID_PRIVATE_KEY, x, y, key_ops: ['sign'] },
      { name: 'ECDSA', namedCurve: 'P-256' }, false, ['sign']
    );
    const sig = Buffer.from(await subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, privKey, sigInput)).toString('base64url');
    const jwt = `${h}.${c}.${sig}`;
    const https = require('https');
    await new Promise((resolve, reject) => {
      const req = https.request({
        hostname: endpoint.hostname, port: endpoint.port || 443,
        path: endpoint.pathname + endpoint.search, method: 'POST',
        headers: { 'Authorization': `vapid t=${jwt},k=${process.env.VAPID_PUBLIC_KEY}`, 'TTL': '86400', 'Urgency': 'high', 'Content-Length': '0' }
      }, res => { res.resume(); resolve(res.statusCode); });
      req.on('error', reject);
      req.end();
    });
  } catch(e) { console.error('sendPushVapid failed:', e.message); }
}

function genApiKey() {
  return crypto.randomBytes(20).toString('hex');
}

function genId() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
}
function genToken() {
  return Date.now().toString(36).toUpperCase() + Math.random().toString(36).slice(2, 9).toUpperCase();
}

function hashPassword(pwd) {
  return crypto.createHmac('sha256', process.env.ADMIN_PASSWORD || 'tb-player').update(String(pwd)).digest('hex');
}

function normPhone(s) { return String(s || '').replace(/\D/g, ''); }

function calcAmount(game, qty) {
  if (Array.isArray(game.pricingTiers) && game.pricingTiers.length) {
    const tier = game.pricingTiers.find(t => t.qty === qty);
    if (tier && tier.price > 0) return tier.price;
  }
  return (game.pricePerSheet || 5) * qty;
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
    if (type === 'settings') {
      const settings = await kv.get('tb:mkt:settings') || {};
      const cfg = await kv.get('tb:config') || {};
      return res.json({ settings: { ...settings, upiId: settings.upiId || cfg.upiId || '' } });
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
    const { password, name, gameDate, pricePerSheet, description, prizes, thumbnail, gameDateRaw, pricingTiers, joinTime } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!name) return res.status(400).json({ error: 'Game name required' });

    const id = genId();
    const game = {
      id,
      name: String(name).trim().slice(0, 80),
      gameDate: gameDate ? String(gameDate).trim().slice(0, 40) : null,
      gameDateRaw: gameDateRaw ? String(gameDateRaw).trim().slice(0, 30) : null,
      joinTime: joinTime ? String(joinTime).trim().slice(0, 20) : null,
      pricePerSheet: Math.max(1, Number(pricePerSheet) || 5),
      pricingTiers: Array.isArray(pricingTiers) ? pricingTiers.slice(0, 10).map(t => ({ qty: Math.max(1, parseInt(t.qty)||1), price: Math.max(1, parseInt(t.price)||1) })) : [],
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
    const compact = { id: game.id, name: game.name, gameDate: game.gameDate, gameDateRaw: game.gameDateRaw || null,
      joinTime: game.joinTime || null,
      pricePerSheet: game.pricePerSheet, pricingTiers: game.pricingTiers || [],
      description: game.description, prizes: game.prizes,
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

  /* ── Player: register ── */
  if (action === 'player-register') {
    if (await rateLimit(req, 'playerreg', 5, 3600))
      return res.status(429).json({ error: 'Too many requests. Try again later.' });
    const { name, phone, password } = body;
    if (!name || !phone || !password)
      return res.status(400).json({ error: 'Name, phone and password are required' });
    if (String(password).length < 4)
      return res.status(400).json({ error: 'Password must be at least 4 characters' });

    const np = normPhone(phone);
    if (!np) return res.status(400).json({ error: 'Invalid phone number' });

    const existing = await kv.get(`tb:player:${np}`);
    if (existing) return res.status(409).json({ error: 'Phone already registered. Please sign in.' });

    const playerId = 'pl_' + genId();
    const player = {
      id: playerId,
      name: String(name).trim().slice(0, 50),
      phone: np,
      passwordHash: hashPassword(password),
      createdAt: Date.now(),
    };
    await kv.set(`tb:player:${np}`, player);

    const token = crypto.randomBytes(32).toString('hex');
    await kv.set(`tb:psession:${token}`, { playerId, phone: np, name: player.name }, { ex: 604800 });

    return res.json({ ok: true, sessionToken: token, player: { name: player.name, phone: np } });
  }

  /* ── Player: login ── */
  if (action === 'player-login') {
    if (await rateLimit(req, 'playerlogin', 20, 3600))
      return res.status(429).json({ error: 'Too many attempts. Try again later.' });
    const { phone, password } = body;
    if (!phone || !password) return res.status(400).json({ error: 'Phone and password required' });

    const np = normPhone(phone);
    const player = await kv.get(`tb:player:${np}`);
    if (!player || player.passwordHash !== hashPassword(password))
      return res.status(401).json({ error: 'Wrong phone number or password' });

    const token = crypto.randomBytes(32).toString('hex');
    await kv.set(`tb:psession:${token}`, { playerId: player.id, phone: np, name: player.name }, { ex: 604800 });

    return res.json({ ok: true, sessionToken: token, player: { name: player.name, phone: np } });
  }

  /* ── Player: verify session ── */
  if (action === 'player-verify-session') {
    if (await rateLimit(req, 'playerverify', 120, 60))
      return res.status(429).json({ error: 'Too many requests' });
    const { sessionToken } = body;
    if (!sessionToken) return res.status(400).json({ error: 'sessionToken required' });

    const session = await kv.get(`tb:psession:${sessionToken}`);
    if (!session) return res.status(401).json({ error: 'Session expired or invalid' });

    // Refresh TTL (sliding window)
    await kv.set(`tb:psession:${sessionToken}`, session, { ex: 604800 });
    return res.json({ ok: true, player: { name: session.name, phone: session.phone } });
  }

  /* ── Player: get own orders ── */
  if (action === 'my-orders') {
    if (await rateLimit(req, 'myorders', 60, 60))
      return res.status(429).json({ error: 'Too many requests' });
    const { sessionToken } = body;
    if (!sessionToken) return res.status(400).json({ error: 'sessionToken required' });
    const session = await kv.get(`tb:psession:${sessionToken}`);
    if (!session) return res.status(401).json({ error: 'Session expired' });
    const np = normPhone(session.phone);
    const all = await kv.get('tb:mkt:purchases') || [];
    const mine = all.filter(p => normPhone(p.phone) === np).slice(0, 20);
    const fresh = await Promise.all(mine.map(p => kv.get(`tb:mkt:purchase:${p.purchaseId}`).then(v => v || null).catch(() => null)));
    const orders = fresh.filter(Boolean).map(p => ({
      purchaseId: p.purchaseId, gameName: p.gameName, quantity: p.quantity,
      amount: p.amount, status: p.status, createdAt: p.createdAt, downloaded: !!p.downloaded,
      downloadToken: (p.status === 'approved' && !p.downloaded) ? p.downloadToken : null,
    }));
    return res.json({ orders });
  }

  /* ── Player: save push subscription ── */
  if (action === 'subscribe-push') {
    if (await rateLimit(req, 'subscribepush', 20, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { sessionToken, subscription } = body;
    if (!sessionToken || !subscription) return res.status(400).json({ error: 'sessionToken and subscription required' });
    const session = await kv.get(`tb:psession:${sessionToken}`);
    if (!session) return res.status(401).json({ error: 'Session expired' });
    const np = normPhone(session.phone);
    await kv.set(`tb:push:${np}`, subscription, { ex: 60 * 60 * 24 * 90 }); // 90 days
    return res.json({ ok: true });
  }

  /* ── Player: purchase sheets ── */
  if (action === 'purchase') {
    if (await rateLimit(req, 'mktbuy', 10, 3600))
      return res.status(429).json({ error: 'Too many requests. Try again later.' });
    const { sessionToken, playerName, phone, gameId, quantity, requestedSheetNums } = body;
    if (!gameId || !quantity)
      return res.status(400).json({ error: 'gameId and quantity required' });

    // Resolve player identity from session or fallback to explicit name/phone
    let resolvedName = playerName, resolvedPhone = phone;
    if (sessionToken) {
      const session = await kv.get(`tb:psession:${sessionToken}`);
      if (!session) return res.status(401).json({ error: 'Session expired. Please sign in again.' });
      resolvedName = session.name; resolvedPhone = session.phone;
    } else if (!playerName || !phone) {
      return res.status(400).json({ error: 'playerName and phone required when not signed in' });
    }

    const games = await kv.get('tb:mkt:games') || [];
    const gameMeta = games.find(g => g.id === gameId);
    if (!gameMeta || gameMeta.status !== 'listed')
      return res.status(404).json({ error: 'Game not available for purchase' });

    const qty = Math.max(1, Math.min(150, parseInt(quantity) || 1));
    const available = gameMeta.sheetCount - (gameMeta.soldCount || 0);
    if (available < qty)
      return res.status(409).json({ error: `Only ${available} sheet${available !== 1 ? 's' : ''} available` });

    const amount = calcAmount(gameMeta, qty);
    const purchaseId = genId();
    const reqNums = Array.isArray(requestedSheetNums) && requestedSheetNums.length
      ? requestedSheetNums.slice(0, 150).map(Number).filter(n => n >= gameMeta.sheetFrom && n <= gameMeta.sheetTo)
      : null;
    const purchase = {
      purchaseId, playerName: String(resolvedName).trim().slice(0, 50),
      phone: String(resolvedPhone).trim().slice(0, 20),
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
    if (purchase.status === 'approved') {
      if (purchase.downloaded)
        return res.json({ status: 'downloaded', quantity: purchase.quantity });
      return res.json({ status: 'approved', downloadToken: purchase.downloadToken, quantity: purchase.quantity });
    }
    return res.json({ status: purchase.status });
  }

  /* ── Admin: edit game ── */
  if (action === 'edit-game') {
    if (await rateLimit(req, 'editgame', 60, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, gameId, name, gameDate, gameDateRaw, pricePerSheet, description, prizes, thumbnail, pricingTiers, joinTime } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!gameId) return res.status(400).json({ error: 'gameId required' });
    const game = await kv.get(`tb:mkt:game:${gameId}`);
    if (!game) return res.status(404).json({ error: 'Game not found' });
    if (name !== undefined) game.name = String(name).trim().slice(0, 80);
    if (gameDate !== undefined) game.gameDate = gameDate ? String(gameDate).trim().slice(0, 40) : null;
    if (gameDateRaw !== undefined) game.gameDateRaw = gameDateRaw ? String(gameDateRaw).trim().slice(0, 30) : null;
    if (joinTime !== undefined) game.joinTime = joinTime ? String(joinTime).trim().slice(0, 20) : null;
    if (pricePerSheet !== undefined) game.pricePerSheet = Math.max(1, Number(pricePerSheet) || 5);
    if (pricingTiers !== undefined) game.pricingTiers = Array.isArray(pricingTiers) ? pricingTiers.slice(0, 10).map(t => ({ qty: Math.max(1, parseInt(t.qty)||1), price: Math.max(1, parseInt(t.price)||1) })) : [];
    if (description !== undefined) game.description = String(description || '').trim().slice(0, 200);
    if (prizes !== undefined) game.prizes = Array.isArray(prizes) ? prizes.slice(0, 12) : [];
    if (thumbnail !== undefined) game.thumbnail = thumbnail ? String(thumbnail).slice(0, 500) : null;
    await kv.set(`tb:mkt:game:${gameId}`, game);
    const games = await kv.get('tb:mkt:games') || [];
    const idx = games.findIndex(g => g.id === gameId);
    if (idx !== -1) { games[idx] = game; await kv.set('tb:mkt:games', games); }
    return res.json({ ok: true, game });
  }

  /* ── Admin: update settings ── */
  if (action === 'update-settings') {
    if (await rateLimit(req, 'updatesettings', 30, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, operatorName, whatsappNumber, supportText, upiId, customQrUrl } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    const settings = await kv.get('tb:mkt:settings') || {};
    if (operatorName !== undefined) settings.operatorName = String(operatorName).trim().slice(0, 80);
    if (whatsappNumber !== undefined) settings.whatsappNumber = String(whatsappNumber).trim().slice(0, 20);
    if (supportText !== undefined) settings.supportText = String(supportText).trim().slice(0, 200);
    if (customQrUrl !== undefined) settings.customQrUrl = customQrUrl ? String(customQrUrl).slice(0, 500) : null;
    if (upiId !== undefined) {
      settings.upiId = String(upiId).trim().slice(0, 100);
      const cfg = await kv.get('tb:config') || {};
      cfg.upiId = settings.upiId;
      await kv.set('tb:config', cfg);
    }
    await kv.set('tb:mkt:settings', settings);
    return res.json({ ok: true, settings });
  }

  /* ── Player: lookup purchase by phone + PIN ── */
  if (action === 'lookup-purchase') {
    if (await rateLimit(req, 'lookuppurchase', 10, 60))
      return res.status(429).json({ error: 'Too many requests. Try again in a minute.' });
    const { name, phone } = body;
    if (!name || !phone) return res.status(400).json({ error: 'Name and phone required' });
    const purchases = await kv.get('tb:mkt:purchases') || [];
    const normPhone = s => String(s || '').trim().replace(/\D/g, '');
    const normName = s => String(s || '').trim().toLowerCase();
    const match = purchases.find(p =>
      normPhone(p.phone) === normPhone(phone) &&
      normName(p.playerName) === normName(name)
    );
    if (!match) return res.status(404).json({ error: 'No order found for that name and phone number' });
    const fresh = await kv.get(`tb:mkt:purchase:${match.purchaseId}`);
    if (!fresh) return res.status(404).json({ error: 'Order has expired' });
    if (fresh.downloaded)
      return res.json({ purchaseId: fresh.purchaseId, status: 'downloaded', gameName: fresh.gameName, quantity: fresh.quantity, amount: fresh.amount, playerName: fresh.playerName });
    return res.json({
      purchaseId: fresh.purchaseId, status: fresh.status,
      gameName: fresh.gameName, quantity: fresh.quantity, amount: fresh.amount,
      playerName: fresh.playerName,
      downloadToken: fresh.status === 'approved' ? fresh.downloadToken : undefined
    });
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

  /* ── Player: consume (invalidate) download token after sheets are saved ── */
  if (action === 'consume-download') {
    if (await rateLimit(req, 'consumedl', 30, 300))
      return res.status(429).json({ error: 'Too many requests' });
    const { downloadToken } = body;
    if (!downloadToken) return res.status(400).json({ error: 'Token required' });
    const dl = await kv.get(`tb:mkt:dl:${downloadToken}`);
    if (dl) {
      await kv.del(`tb:mkt:dl:${downloadToken}`);
      if (dl.purchaseId) {
        const purchase = await kv.get(`tb:mkt:purchase:${dl.purchaseId}`);
        if (purchase) {
          purchase.downloaded = true;
          purchase.downloadedAt = Date.now();
          await kv.set(`tb:mkt:purchase:${dl.purchaseId}`, purchase, { ex: 21600 });
          const plist = await kv.get('tb:mkt:purchases') || [];
          const pIdx = plist.findIndex(p => p.purchaseId === dl.purchaseId);
          if (pIdx >= 0) { plist[pIdx].downloaded = true; await kv.set('tb:mkt:purchases', plist); }
        }
      }
    }
    return res.json({ ok: true });
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
    await kv.set(`tb:mkt:dl:${dlToken}`, { sheets: sheetList, gameName: game.name, purchaseId }, { ex: 21600 });

    const newSold = [...(game.soldSheetNums || []), ...assigned.map(s => s.n)];
    game.soldSheetNums = newSold; game.soldCount = newSold.length;
    await kv.set(`tb:mkt:game:${purchase.gameId}`, game);

    const games = await kv.get('tb:mkt:games') || [];
    const gIdx = games.findIndex(g => g.id === purchase.gameId);
    if (gIdx >= 0) { games[gIdx].soldCount = game.soldCount; await kv.set('tb:mkt:games', games); }

    purchase.status = 'approved'; purchase.downloadToken = dlToken;
    purchase.approvedAt = Date.now();
    purchase.sheetNums = assigned.map(s => s.n);
    await kv.set(`tb:mkt:purchase:${purchaseId}`, purchase, { ex: 21600 });

    const plist = await kv.get('tb:mkt:purchases') || [];
    const pIdx = plist.findIndex(p => p.purchaseId === purchaseId);
    if (pIdx >= 0) { plist[pIdx] = purchase; await kv.set('tb:mkt:purchases', plist); }

    // Send push notification to player
    try {
      const np = normPhone(purchase.phone);
      const pushSub = await kv.get(`tb:push:${np}`);
      if (pushSub) await sendPushVapid(pushSub);
    } catch(pushErr) { console.error('Push failed:', pushErr.message); }

    return res.json({ ok: true, downloadToken: dlToken, sheetsAssigned: assigned.length });
  }

  /* ── Admin: delete a game ── */
  if (action === 'delete-game') {
    const { password, gameId } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!gameId) return res.status(400).json({ error: 'gameId required' });

    // Remove game record and from games list
    await kv.del(`tb:mkt:game:${gameId}`);
    const games = await kv.get('tb:mkt:games') || [];
    await kv.set('tb:mkt:games', games.filter(g => g.id !== gameId));

    // Remove all purchases for this game (individual keys + list)
    const purchases = await kv.get('tb:mkt:purchases') || [];
    const toDelete = purchases.filter(p => p.gameId === gameId);
    const remaining = purchases.filter(p => p.gameId !== gameId);
    await kv.set('tb:mkt:purchases', remaining);
    await Promise.all(toDelete.map(p => kv.del(`tb:mkt:purchase:${p.purchaseId}`)));

    return res.json({ ok: true });
  }

  /* ── Public: get live game called numbers (polled by Tungbola) ── */
  if (action === 'get-live-game') {
    if (await rateLimit(req, 'getlive', 300, 60))
      return res.status(429).json({ error: 'Too many requests' });
    const { gameId } = body;
    if (!gameId) return res.status(400).json({ error: 'gameId required' });
    const state = await kv.get(`tb:live:${gameId}`) || { calledNumbers: [], lastNumber: null };
    return res.json({ ok: true, calledNumbers: state.calledNumbers, lastNumber: state.lastNumber, lastCalledAt: state.lastCalledAt });
  }

  /* ── Admin: create an operator account ── */
  if (action === 'create-operator') {
    const { password, name, email, phone, plan } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!name || !plan) return res.status(400).json({ error: 'name and plan required' });
    if (!['own-sheets', 'generate'].includes(plan))
      return res.status(400).json({ error: 'plan must be own-sheets or generate' });

    const id = 'op_' + genId();
    const apiKey = genApiKey();
    const operator = {
      id, name: String(name).trim().slice(0, 80),
      email: String(email || '').trim().slice(0, 100),
      phone: String(phone || '').trim().slice(0, 20),
      plan, apiKey, createdAt: Date.now(), active: true
    };
    await kv.set(`tb:op:${id}`, operator);
    const ops = await kv.get('tb:ops') || [];
    ops.unshift({ id, name: operator.name, plan, apiKey, createdAt: operator.createdAt });
    await kv.set('tb:ops', ops);
    return res.json({ ok: true, operator });
  }

  /* ── Admin: list operators ── */
  if (action === 'list-operators') {
    const { password } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    const ops = await kv.get('tb:ops') || [];
    return res.json({ ok: true, operators: ops });
  }

  /* ── Admin: delete operator ── */
  if (action === 'delete-operator') {
    const { password, operatorId } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!operatorId) return res.status(400).json({ error: 'operatorId required' });
    await kv.del(`tb:op:${operatorId}`);
    const ops = await kv.get('tb:ops') || [];
    await kv.set('tb:ops', ops.filter(o => o.id !== operatorId));
    return res.json({ ok: true });
  }

  return res.status(400).json({ error: 'Unknown action' });
};
