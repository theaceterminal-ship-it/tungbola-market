const { put } = require('@vercel/blob');
const { secureHeaders, rateLimit, checkPassword } = require('./_security');
const { db, gameFromRow, gameToRow, purchaseFromRow, purchaseToRow, operatorFromRow } = require('./_db');
const { sendPush } = require('./_push');
const { broadcastGame, notifyPlayerApproved, notifyPlayerRejected } = require('./telegram');
const crypto = require('crypto');

// ── Telegram notification ────────────────────────────────────
async function sendTelegramOrderNotification(purchase, game) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  if (!token) return;
  try {
    // Use operator chat if game has an operator, else admin chat
    let chatId = process.env.TELEGRAM_ADMIN_CHAT_ID;
    if (game.operatorId) {
      const { data: op } = await db().from('operators').select('telegram_chat_id').eq('id', game.operatorId).single();
      if (op?.telegram_chat_id) chatId = op.telegram_chat_id;
    }
    if (!chatId) return;

    const reqNums = purchase.requestedSheetNums?.length
      ? `\n📌 Requested: #${purchase.requestedSheetNums.join(', #')}`
      : '';
    const time = new Date(purchase.createdAt).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', timeZone: 'Asia/Kolkata' });
    const text = `🎟 *New Order*\n\n👤 ${purchase.playerName}\n📱 ${purchase.phone}\n🎮 ${purchase.gameName}\n📋 ${purchase.quantity} sheet${purchase.quantity !== 1 ? 's' : ''}\n💰 ₹${purchase.amount}\n⏰ ${time}${reqNums}\n\n_Check your UPI app for ₹${purchase.amount} before approving._`;

    const payload = {
      chat_id: chatId,
      text,
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [[
          { text: '✅ Approve', callback_data: `approve:${purchase.purchaseId}` },
          { text: '❌ Reject',  callback_data: `reject:${purchase.purchaseId}` }
        ]]
      }
    };
    const https = require('https');
    const body = JSON.stringify(payload);
    await new Promise((resolve, reject) => {
      const req = https.request({
        hostname: 'api.telegram.org', path: `/bot${token}/sendMessage`, method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
      }, res => { res.resume(); resolve(); });
      req.on('error', reject);
      req.write(body); req.end();
    });
  } catch(e) { console.error('sendTelegramOrderNotification failed:', e.message); }
}

// ── Helpers ──────────────────────────────────────────────────
function genId()    { return Date.now().toString(36) + Math.random().toString(36).slice(2, 6); }
function genToken() { return Date.now().toString(36).toUpperCase() + Math.random().toString(36).slice(2, 9).toUpperCase(); }
function normPhone(s) { return String(s || '').replace(/\D/g, ''); }

function hashPassword(pwd) {
  return crypto.createHmac('sha256', process.env.HMAC_SECRET || 'tb-cmp-key').update(String(pwd)).digest('hex');
}

function calcAmount(game, qty) {
  if (Array.isArray(game.pricingTiers) && game.pricingTiers.length) {
    const tier = game.pricingTiers.find(t => t.qty === qty);
    if (tier && tier.price > 0) return tier.price;
  }
  return (game.pricePerSheet || 5) * qty;
}

// ── Main handler ─────────────────────────────────────────────
module.exports = async function(req, res) {
  secureHeaders(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  /* GET: public game listing OR admin dashboard */
  if (req.method === 'GET') {
    if (await rateLimit(req, 'mktget', 120, 60))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, type } = req.query;

    if (!password) {
      const { data: gameRows } = await db().from('games').select('*').eq('status', 'listed').order('created_at', { ascending: false }).limit(200);
      // Batch-load operator UPI/support info for games that have an operator
      const opIds = [...new Set((gameRows || []).filter(g => g.operator_id).map(g => g.operator_id))];
      let opMap = {};
      if (opIds.length) {
        const { data: opRows } = await db().from('operators').select('id,upi_id,support_phone,display_name').in('id', opIds);
        for (const op of opRows || []) opMap[op.id] = op;
      }
      return res.json({ games: (gameRows || []).map(r => {
        const g = gameFromRow(r);
        if (r.operator_id && opMap[r.operator_id]) {
          const op = opMap[r.operator_id];
          g.operatorUpiId        = op.upi_id        || null;
          g.operatorSupportPhone = op.support_phone  || null;
          g.operatorDisplayName  = op.display_name   || null;
        }
        return g;
      }) });
    }

    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });

    if (type === 'purchases') {
      const { data } = await db().from('purchases').select('*').order('created_at', { ascending: false }).limit(500);
      return res.json({ purchases: (data || []).map(purchaseFromRow) });
    }
    if (type === 'settings') {
      const { data } = await db().from('config').select('value').eq('key', 'settings').single();
      return res.json({ settings: data?.value || {} });
    }
    if (type === 'platform-payments') {
      const { data } = await db().from('platform_payments')
        .select('*').order('created_at', { ascending: false }).limit(200);
      return res.json({ payments: data || [] });
    }
    const { data } = await db().from('games').select('*').order('created_at', { ascending: false }).limit(200);
    return res.json({ games: (data || []).map(gameFromRow) });
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
    try { buffer = Buffer.from(data, 'base64'); } catch(e) { return res.status(400).json({ error: 'Invalid base64' }); }
    if (buffer.length > 5 * 1024 * 1024) return res.status(400).json({ error: 'Image too large (max 5MB)' });
    const ext = String(filename || 'image.jpg').split('.').pop().toLowerCase();
    const ct = ext === 'png' ? 'image/png' : ext === 'gif' ? 'image/gif' : ext === 'webp' ? 'image/webp' : 'image/jpeg';
    try {
      const blob = await put(`tungbola/prize-${Date.now()}.${ext}`, buffer, { access: 'public', contentType: ct });
      return res.json({ ok: true, url: blob.url });
    } catch(e) { return res.status(500).json({ error: `Upload failed: ${e.message}` }); }
  }

  /* ── Admin: create a game ── */
  if (action === 'create-game') {
    if (await rateLimit(req, 'creategame', 30, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, name, gameDate, gameDateRaw, pricePerSheet, description, prizes, thumbnail, pricingTiers, joinTime, joinLink, joinDetails } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!name) return res.status(400).json({ error: 'Game name required' });

    const game = {
      id: genId(), name: String(name).trim().slice(0, 80),
      gameDate: gameDate ? String(gameDate).trim().slice(0, 40) : null,
      gameDateRaw: gameDateRaw ? String(gameDateRaw).trim().slice(0, 30) : null,
      joinTime: joinTime ? String(joinTime).trim().slice(0, 20) : null,
      joinLink: joinLink ? String(joinLink).trim().slice(0, 500) : null,
      joinDetails: joinDetails ? String(joinDetails).trim().slice(0, 500) : null,
      pricePerSheet: Math.max(1, Number(pricePerSheet) || 5),
      pricingTiers: Array.isArray(pricingTiers) ? pricingTiers.slice(0, 10).map(t => ({ qty: Math.max(1, parseInt(t.qty)||1), price: Math.max(1, parseInt(t.price)||1) })) : [],
      description: String(description || '').trim().slice(0, 200),
      prizes: Array.isArray(prizes) ? prizes.slice(0, 12) : [],
      thumbnail: thumbnail ? String(thumbnail).slice(0, 500) : null,
      status: 'draft', sheetFrom: 0, sheetTo: 0, sheetCount: 0, soldCount: 0,
      soldSheetNums: [], createdAt: Date.now()
    };
    await db().from('games').insert(gameToRow(game));
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

    const { data: gameRow } = await db().from('games').select('*').eq('id', gameId).single();
    if (!gameRow) return res.status(404).json({ error: 'Game not found' });
    if (gameRow.status === 'ended') return res.status(409).json({ error: 'Game has ended' });

    const { data: sheetRows } = await db().from('sheets').select('*').gte('n', from).lte('n', to);
    if (!sheetRows?.length) return res.status(400).json({ error: `No uploaded sheets in range ${from}–${to}` });

    await db().from('games').update({
      sheet_from: from, sheet_to: to, sheet_count: sheetRows.length,
      sold_count: 0, sold_sheet_nums: []
    }).eq('id', gameId);

    const game = gameFromRow({ ...gameRow, sheet_from: from, sheet_to: to, sheet_count: sheetRows.length, sold_count: 0, sold_sheet_nums: [] });
    return res.json({ ok: true, game, sheetsFound: sheetRows.length });
  }

  /* ── Admin: set game status ── */
  if (action === 'set-status') {
    const { password, gameId, status } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!['listed', 'ended', 'draft'].includes(status))
      return res.status(400).json({ error: 'status must be listed, ended or draft' });

    const { data: gameRow } = await db().from('games').select('*').eq('id', gameId).single();
    if (!gameRow) return res.status(404).json({ error: 'Game not found' });
    if (status === 'listed' && !gameRow.sheet_count)
      return res.status(400).json({ error: 'Assign sheets before listing the game' });

    await db().from('games').update({ status }).eq('id', gameId);

    if (status === 'listed' && gameRow.operator_id) {
      try {
        const { data: opRow } = await db().from('operators').select('player_channel_id').eq('id', gameRow.operator_id).single();
        if (opRow?.player_channel_id) await broadcastGame(opRow.player_channel_id, gameFromRow({ ...gameRow, status }));
      } catch(e) { console.error('Broadcast on admin set-status failed:', e.message); }
    }

    return res.json({ ok: true });
  }

  /* ── Admin: edit game ── */
  if (action === 'edit-game') {
    if (await rateLimit(req, 'editgame', 60, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, gameId, name, gameDate, gameDateRaw, pricePerSheet, description, prizes, thumbnail, pricingTiers, joinTime, joinLink, joinDetails } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!gameId) return res.status(400).json({ error: 'gameId required' });

    const { data: gameRow } = await db().from('games').select('*').eq('id', gameId).single();
    if (!gameRow) return res.status(404).json({ error: 'Game not found' });

    const updates = {};
    if (name !== undefined)          updates.name            = String(name).trim().slice(0, 80);
    if (gameDate !== undefined)      updates.game_date       = gameDate ? String(gameDate).trim().slice(0, 40) : null;
    if (gameDateRaw !== undefined)   updates.game_date_raw   = gameDateRaw ? String(gameDateRaw).trim().slice(0, 30) : null;
    if (joinTime !== undefined)      updates.join_time       = joinTime ? String(joinTime).trim().slice(0, 20) : null;
    if (joinLink !== undefined)      updates.join_link       = joinLink ? String(joinLink).trim().slice(0, 500) : null;
    if (joinDetails !== undefined)   updates.join_details    = joinDetails ? String(joinDetails).trim().slice(0, 500) : null;
    if (pricePerSheet !== undefined) updates.price_per_sheet = Math.max(1, Number(pricePerSheet) || 5);
    if (pricingTiers !== undefined)  updates.pricing_tiers   = Array.isArray(pricingTiers) ? pricingTiers.slice(0, 10).map(t => ({ qty: Math.max(1, parseInt(t.qty)||1), price: Math.max(1, parseInt(t.price)||1) })) : [];
    if (description !== undefined)   updates.description     = String(description || '').trim().slice(0, 200);
    if (prizes !== undefined)        updates.prizes          = Array.isArray(prizes) ? prizes.slice(0, 12) : [];
    if (thumbnail !== undefined)     updates.thumbnail       = thumbnail ? String(thumbnail).slice(0, 500) : null;

    const { data: updated } = await db().from('games').update(updates).eq('id', gameId).select().single();
    return res.json({ ok: true, game: gameFromRow(updated) });
  }

  /* ── Admin: update settings ── */
  if (action === 'update-settings') {
    if (await rateLimit(req, 'updatesettings', 30, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, operatorName, whatsappNumber, supportText, upiId, customQrUrl } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });

    const { data: cfg } = await db().from('config').select('value').eq('key', 'settings').single();
    const settings = cfg?.value || {};
    if (operatorName !== undefined)  settings.operatorName  = String(operatorName).trim().slice(0, 80);
    if (whatsappNumber !== undefined) settings.whatsappNumber = String(whatsappNumber).trim().slice(0, 20);
    if (supportText !== undefined)   settings.supportText   = String(supportText).trim().slice(0, 200);
    if (customQrUrl !== undefined)   settings.customQrUrl   = customQrUrl ? String(customQrUrl).slice(0, 500) : null;
    if (upiId !== undefined) {
      settings.upiId = String(upiId).trim().slice(0, 100);
      await db().from('config').upsert({ key: 'app_config', value: { upiId: settings.upiId } });
    }
    await db().from('config').upsert({ key: 'settings', value: settings });
    return res.json({ ok: true, settings });
  }

  /* ── Admin: approve purchase → assign sheets → generate download token ── */
  if (action === 'approve-purchase') {
    if (await rateLimit(req, 'approvepurchase', 60, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { password, purchaseId } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!purchaseId) return res.status(400).json({ error: 'purchaseId required' });

    const { data: pRow } = await db().from('purchases').select('*').eq('purchase_id', purchaseId).single();
    if (!pRow) return res.status(404).json({ error: 'Purchase not found' });
    if (pRow.status !== 'pending') return res.status(409).json({ error: 'Already processed' });

    const purchase = purchaseFromRow(pRow);
    const { data: gRow } = await db().from('games').select('*').eq('id', purchase.gameId).single();
    if (!gRow) return res.status(404).json({ error: 'Game not found' });
    const game = gameFromRow(gRow);

    // Use operator_sheets for Plan A operators, shared sheets for everything else
    let allSheetsQuery, opPlan;
    if (gRow.operator_id) {
      const { data: opRow } = await db().from('operators').select('plan').eq('id', gRow.operator_id).single();
      opPlan = opRow?.plan;
      allSheetsQuery = opRow?.plan === 'own-sheets'
        ? db().from('operator_sheets').select('*').eq('operator_id', gRow.operator_id).gte('n', game.sheetFrom).lte('n', game.sheetTo)
        : db().from('sheets').select('*').gte('n', game.sheetFrom).lte('n', game.sheetTo);
    } else {
      allSheetsQuery = db().from('sheets').select('*').gte('n', game.sheetFrom).lte('n', game.sheetTo);
    }
    const { data: allSheets } = await allSheetsQuery;
    if (opPlan === 'generate' && !allSheets?.length)
      return res.status(409).json({ error: 'Generate plan is not yet configured for this game. Contact the platform admin.' });

    const soldSet = new Set(game.soldSheetNums);
    const available = (allSheets || []).filter(s => !soldSet.has(s.n));

    if (available.length < purchase.quantity)
      return res.status(409).json({ error: `Only ${available.length} sheets left` });

    let assigned;
    if (purchase.requestedSheetNums?.length) {
      const reqSet = new Set(purchase.requestedSheetNums);
      assigned = [...available.filter(s => reqSet.has(s.n)), ...available.filter(s => !reqSet.has(s.n))].slice(0, purchase.quantity);
    } else {
      assigned = available.slice(0, purchase.quantity);
    }

    const sheetList = assigned.map(s => ({ n: s.n, filename: s.f, url: s.u }));
    const dlToken = genToken();
    const now = Date.now();
    const newSoldNums = [...game.soldSheetNums, ...assigned.map(s => s.n)];

    // Atomic: only update game if sold_count hasn't changed since we read it (prevents race on concurrent approvals)
    const { data: lockCheck } = await db().from('games')
      .update({ sold_sheet_nums: newSoldNums, sold_count: newSoldNums.length })
      .eq('id', game.id).eq('sold_count', game.soldCount).select('id');
    if (!lockCheck?.length)
      return res.status(409).json({ error: 'Another order was approved at the same time — please try again.' });

    await Promise.all([
      db().from('download_tokens').insert({ token: dlToken, sheets: sheetList, game_name: game.name, purchase_id: purchaseId }),
      db().from('purchases').update({ status: 'approved', download_token: dlToken, approved_at: now, sheet_nums: assigned.map(s => s.n) }).eq('purchase_id', purchaseId)
    ]);

    // Push + Telegram notifications to player
    try {
      const { data: pushRow } = await db().from('push_subscriptions').select('subscription').eq('phone', normPhone(purchase.phone)).single();
      if (pushRow?.subscription) await sendPush(pushRow.subscription);
    } catch(e) { console.error('Push failed:', e.message); }
    try { await notifyPlayerApproved(normPhone(purchase.phone), game.name, purchase.quantity, purchase.amount, dlToken, game.joinLink, game.joinDetails); } catch(e) {}

    return res.json({ ok: true, downloadToken: dlToken, sheetsAssigned: assigned.length });
  }

  /* ── Admin: reject purchase ── */
  if (action === 'reject-purchase') {
    const { password, purchaseId } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!purchaseId) return res.status(400).json({ error: 'purchaseId required' });

    const { data: pRow } = await db().from('purchases').select('status').eq('purchase_id', purchaseId).single();
    if (!pRow) return res.status(404).json({ error: 'Purchase not found' });
    if (pRow.status !== 'pending') return res.status(409).json({ error: 'Already processed' });

    const { data: fullPRow } = await db().from('purchases').select('phone, player_name, game_name, quantity, amount').eq('purchase_id', purchaseId).single();
    await db().from('purchases').update({ status: 'rejected' }).eq('purchase_id', purchaseId);
    if (fullPRow) {
      try { await notifyPlayerRejected(normPhone(fullPRow.phone), fullPRow.game_name, fullPRow.quantity, fullPRow.amount); } catch(e) {}
    }
    return res.json({ ok: true });
  }

  /* ── Admin: delete a game ── */
  if (action === 'delete-game') {
    const { password, gameId } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!gameId) return res.status(400).json({ error: 'gameId required' });

    await db().from('purchases').delete().eq('game_id', gameId);
    await db().from('games').delete().eq('id', gameId);
    return res.json({ ok: true });
  }

  /* ── Admin: create operator ── */
  if (action === 'create-operator') {
    const { password, name, email, phone: opPhone, plan } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!name || !plan) return res.status(400).json({ error: 'name and plan required' });
    if (!['own-sheets', 'generate'].includes(plan))
      return res.status(400).json({ error: 'plan must be own-sheets or generate' });

    const operator = {
      id: 'op_' + genId(), name: String(name).trim().slice(0, 80),
      email: String(email || '').trim().slice(0, 100), phone: String(opPhone || '').trim().slice(0, 20),
      plan, api_key: crypto.randomBytes(20).toString('hex'), active: true, created_at: Date.now()
    };
    await db().from('operators').insert(operator);
    return res.json({ ok: true, operator: operatorFromRow(operator) });
  }

  /* ── Admin: list operators ── */
  if (action === 'list-operators') {
    const { password } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    const { data } = await db().from('operators').select('*').order('created_at', { ascending: false });
    return res.json({ ok: true, operators: (data || []).map(operatorFromRow) });
  }

  /* ── Admin: delete operator ── */
  if (action === 'delete-operator') {
    const { password, operatorId } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!operatorId) return res.status(400).json({ error: 'operatorId required' });
    await db().from('operators').delete().eq('id', operatorId);
    return res.json({ ok: true });
  }

  /* ── Player: register ── */
  if (action === 'player-register') {
    if (await rateLimit(req, 'playerreg', 5, 3600))
      return res.status(429).json({ error: 'Too many requests. Try again later.' });
    const { name, phone, password } = body;
    if (!name || !phone || !password) return res.status(400).json({ error: 'Name, phone and password are required' });
    if (String(password).length < 4) return res.status(400).json({ error: 'Password must be at least 4 characters' });

    const np = normPhone(phone);
    if (!np || np.length < 6) return res.status(400).json({ error: 'Invalid phone number' });

    const { data: existing } = await db().from('players').select('phone').eq('phone', np).single();
    if (existing) return res.status(409).json({ error: 'Phone already registered. Please sign in.' });

    const player = {
      id: 'pl_' + genId(), name: String(name).trim().slice(0, 50),
      phone: np, password_hash: hashPassword(password), created_at: Date.now()
    };
    await db().from('players').insert(player);

    const token = crypto.randomBytes(32).toString('hex');
    const expiresAt = new Date(Date.now() + 604800000).toISOString();
    await db().from('sessions').insert({ token, player_id: player.id, phone: np, name: player.name, expires_at: expiresAt });

    return res.json({ ok: true, sessionToken: token, player: { name: player.name, phone: np } });
  }

  /* ── Player: login ── */
  if (action === 'player-login') {
    if (await rateLimit(req, 'playerlogin', 20, 3600))
      return res.status(429).json({ error: 'Too many attempts. Try again later.' });
    const { phone, password } = body;
    if (!phone || !password) return res.status(400).json({ error: 'Phone and password required' });

    const np = normPhone(phone);
    const { data: player } = await db().from('players').select('*').eq('phone', np).single();
    if (!player || player.password_hash !== hashPassword(password)) {
      if (player) {
        try {
          const { data: tgRow } = await db().from('player_telegram').select('id').eq('phone', np).maybeSingle();
          if (tgRow) return res.status(401).json({ error: 'This account was linked via Telegram. Use the bot to access your orders — /myorders' });
        } catch(e) {}
      }
      return res.status(401).json({ error: 'Wrong phone number or password' });
    }

    const token = crypto.randomBytes(32).toString('hex');
    const expiresAt = new Date(Date.now() + 604800000).toISOString();
    await db().from('sessions').insert({ token, player_id: player.id, phone: np, name: player.name, expires_at: expiresAt });

    return res.json({ ok: true, sessionToken: token, player: { name: player.name, phone: np } });
  }

  /* ── Player: verify session ── */
  if (action === 'player-verify-session') {
    if (await rateLimit(req, 'playerverify', 120, 60))
      return res.status(429).json({ error: 'Too many requests' });
    const { sessionToken } = body;
    if (!sessionToken) return res.status(400).json({ error: 'sessionToken required' });

    const { data: session } = await db().from('sessions').select('*').eq('token', sessionToken).gt('expires_at', new Date().toISOString()).single();
    if (!session) return res.status(401).json({ error: 'Session expired or invalid' });

    // Sliding window: refresh expires_at
    await db().from('sessions').update({ expires_at: new Date(Date.now() + 604800000).toISOString() }).eq('token', sessionToken);
    return res.json({ ok: true, player: { name: session.name, phone: session.phone } });
  }

  /* ── Player: get own orders ── */
  if (action === 'my-orders') {
    if (await rateLimit(req, 'myorders', 60, 60))
      return res.status(429).json({ error: 'Too many requests' });
    const { sessionToken } = body;
    if (!sessionToken) return res.status(400).json({ error: 'sessionToken required' });

    const { data: session } = await db().from('sessions').select('phone').eq('token', sessionToken).gt('expires_at', new Date().toISOString()).single();
    if (!session) return res.status(401).json({ error: 'Session expired' });

    const { data: rows } = await db().from('purchases').select('*').eq('phone', normPhone(session.phone)).order('created_at', { ascending: false }).limit(20);
    const orders = (rows || []).map(p => {
      const purchase = purchaseFromRow(p);
      return {
        purchaseId: purchase.purchaseId, gameName: purchase.gameName, quantity: purchase.quantity,
        amount: purchase.amount, status: purchase.status, createdAt: purchase.createdAt,
        downloaded: purchase.downloaded,
        downloadToken: (purchase.status === 'approved' && !purchase.downloaded) ? purchase.downloadToken : null
      };
    });
    return res.json({ orders });
  }

  /* ── Player: save push subscription ── */
  if (action === 'subscribe-push') {
    if (await rateLimit(req, 'subscribepush', 20, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { sessionToken, subscription } = body;
    if (!sessionToken || !subscription) return res.status(400).json({ error: 'sessionToken and subscription required' });

    const { data: session } = await db().from('sessions').select('phone').eq('token', sessionToken).gt('expires_at', new Date().toISOString()).single();
    if (!session) return res.status(401).json({ error: 'Session expired' });

    await db().from('push_subscriptions').upsert({ phone: normPhone(session.phone), subscription, updated_at: new Date().toISOString() });
    return res.json({ ok: true });
  }

  /* ── Player: purchase sheets ── */
  if (action === 'purchase') {
    if (await rateLimit(req, 'mktbuy', 10, 3600))
      return res.status(429).json({ error: 'Too many requests. Try again later.' });
    const { sessionToken, playerName, phone, gameId, quantity, requestedSheetNums } = body;
    if (!gameId || !quantity) return res.status(400).json({ error: 'gameId and quantity required' });

    let resolvedName = playerName, resolvedPhone = phone;
    if (sessionToken) {
      const { data: session } = await db().from('sessions').select('name,phone').eq('token', sessionToken).gt('expires_at', new Date().toISOString()).single();
      if (!session) return res.status(401).json({ error: 'Session expired. Please sign in again.' });
      resolvedName = session.name; resolvedPhone = session.phone;
    } else if (!playerName || !phone) {
      return res.status(400).json({ error: 'playerName and phone required when not signed in' });
    }

    const { data: gRow } = await db().from('games').select('*').eq('id', gameId).eq('status', 'listed').single();
    if (!gRow) return res.status(404).json({ error: 'Game not available for purchase' });
    const game = gameFromRow(gRow);

    const qty = Math.max(1, Math.min(150, parseInt(quantity) || 1));
    const available = game.sheetCount - (game.soldCount || 0);
    if (available < qty) return res.status(409).json({ error: `Only ${available} sheet${available !== 1 ? 's' : ''} available` });

    const amount = calcAmount(game, qty);
    const purchaseId = genId();
    const reqNums = Array.isArray(requestedSheetNums) && requestedSheetNums.length
      ? requestedSheetNums.slice(0, 150).map(Number).filter(n => n >= game.sheetFrom && n <= game.sheetTo)
      : null;

    const purchase = {
      purchaseId, playerName: String(resolvedName).trim().slice(0, 50),
      phone: String(resolvedPhone).trim().slice(0, 20),
      gameId, gameName: game.name, quantity: qty, amount,
      requestedSheetNums: reqNums, status: 'pending', createdAt: Date.now()
    };
    await db().from('purchases').insert(purchaseToRow(purchase));

    // Telegram notification to operator / admin
    sendTelegramOrderNotification(purchase, game).catch(() => {});

    return res.json({ ok: true, purchaseId, amount, playerName: purchase.playerName, gameName: game.name });
  }

  /* ── Player: get available sheet numbers ── */
  if (action === 'available-sheets') {
    if (await rateLimit(req, 'availsheets', 120, 60))
      return res.status(429).json({ error: 'Too many requests' });
    const { gameId } = body;
    if (!gameId) return res.status(400).json({ error: 'gameId required' });

    const { data: gRow } = await db().from('games').select('*').eq('id', gameId).eq('status', 'listed').single();
    if (!gRow) return res.status(404).json({ error: 'Game not found' });
    const game = gameFromRow(gRow);

    let sheetQuery;
    if (gRow.operator_id) {
      const { data: opRow } = await db().from('operators').select('plan').eq('id', gRow.operator_id).single();
      sheetQuery = opRow?.plan === 'own-sheets'
        ? db().from('operator_sheets').select('n').eq('operator_id', gRow.operator_id).gte('n', game.sheetFrom).lte('n', game.sheetTo)
        : db().from('sheets').select('n').gte('n', game.sheetFrom).lte('n', game.sheetTo);
    } else {
      sheetQuery = db().from('sheets').select('n').gte('n', game.sheetFrom).lte('n', game.sheetTo);
    }
    const { data: sheetRows } = await sheetQuery;

    const soldSet = new Set(game.soldSheetNums);
    const allNums = (sheetRows || []).map(s => s.n).sort((a, b) => a - b);
    return res.json({
      available: allNums.filter(n => !soldSet.has(n)),
      sold: allNums.filter(n => soldSet.has(n)),
      total: allNums.length
    });
  }

  /* ── Player: poll purchase status ── */
  if (action === 'check-purchase') {
    if (await rateLimit(req, 'checkpurchase', 120, 60))
      return res.status(429).json({ error: 'Too many requests' });
    const { purchaseId } = body;
    if (!purchaseId) return res.status(400).json({ error: 'purchaseId required' });

    const { data: pRow } = await db().from('purchases').select('*').eq('purchase_id', purchaseId).single();
    if (!pRow) return res.status(404).json({ error: 'Purchase not found or expired' });
    const p = purchaseFromRow(pRow);
    if (p.status === 'approved') {
      if (p.downloaded) return res.json({ status: 'downloaded', quantity: p.quantity });
      return res.json({ status: 'approved', downloadToken: p.downloadToken, quantity: p.quantity });
    }
    return res.json({ status: p.status });
  }

  /* ── Player: lookup purchase by name+phone ── */
  if (action === 'lookup-purchase') {
    if (await rateLimit(req, 'lookuppurchase', 10, 60))
      return res.status(429).json({ error: 'Too many requests. Try again in a minute.' });
    const { name, phone } = body;
    if (!name || !phone) return res.status(400).json({ error: 'Name and phone required' });

    const np = normPhone(phone);
    const { data: rows } = await db().from('purchases').select('*').eq('phone', np).order('created_at', { ascending: false }).limit(10);
    const match = (rows || []).find(p => p.player_name.trim().toLowerCase() === String(name).trim().toLowerCase());
    if (!match) return res.status(404).json({ error: 'No order found for that name and phone number' });

    const p = purchaseFromRow(match);
    return res.json({
      purchaseId: p.purchaseId, status: p.status, gameName: p.gameName,
      quantity: p.quantity, amount: p.amount, playerName: p.playerName,
      downloadToken: p.status === 'approved' ? p.downloadToken : undefined
    });
  }

  /* ── Player: get downloads using token ── */
  if (action === 'get-downloads') {
    if (await rateLimit(req, 'getdl', 60, 300))
      return res.status(429).json({ error: 'Too many requests' });
    const { downloadToken } = body;
    if (!downloadToken) return res.status(400).json({ error: 'Token required' });

    const { data: dl } = await db().from('download_tokens').select('*').eq('token', downloadToken).eq('consumed', false).gt('expires_at', new Date().toISOString()).single();
    if (!dl) return res.status(404).json({ error: 'Download link expired or invalid. Contact admin.' });
    return res.json({ ok: true, sheets: dl.sheets, gameName: dl.game_name });
  }

  /* ── Player: consume (invalidate) download token ── */
  if (action === 'consume-download') {
    if (await rateLimit(req, 'consumedl', 30, 300))
      return res.status(429).json({ error: 'Too many requests' });
    const { downloadToken } = body;
    if (!downloadToken) return res.status(400).json({ error: 'Token required' });

    const { data: dl } = await db().from('download_tokens').select('purchase_id').eq('token', downloadToken).single();
    if (dl) {
      const now = Date.now();
      await db().from('download_tokens').update({ consumed: true }).eq('token', downloadToken);
      if (dl.purchase_id) {
        await db().from('purchases').update({ downloaded: true, downloaded_at: now, status: 'downloaded' }).eq('purchase_id', dl.purchase_id);
      }
    }
    return res.json({ ok: true });
  }

  /* ── Player: resend expired download link ── */
  if (action === 'resend-download') {
    if (await rateLimit(req, 'resenddl', 5, 3600))
      return res.status(429).json({ error: 'Too many requests. Try again later.' });
    const { sessionToken, purchaseId } = body;
    if (!sessionToken || !purchaseId) return res.status(400).json({ error: 'sessionToken and purchaseId required' });

    const { data: session } = await db().from('sessions').select('phone').eq('token', sessionToken).gt('expires_at', new Date().toISOString()).single();
    if (!session) return res.status(401).json({ error: 'Session expired' });

    const { data: pRow } = await db().from('purchases').select('*').eq('purchase_id', purchaseId).eq('phone', normPhone(session.phone)).single();
    if (!pRow) return res.status(404).json({ error: 'Order not found' });
    if (!['approved', 'downloaded'].includes(pRow.status)) return res.status(409).json({ error: 'Order not yet approved' });

    // Reuse sheets list from the most recent download token for this purchase
    const { data: tokenRows } = await db().from('download_tokens')
      .select('sheets, game_name').eq('purchase_id', purchaseId)
      .order('expires_at', { ascending: false }).limit(1);
    if (!tokenRows?.length) return res.status(404).json({ error: 'Sheet data not found. Please contact support.' });

    const dlToken = genToken();
    await db().from('download_tokens').insert({ token: dlToken, sheets: tokenRows[0].sheets, game_name: tokenRows[0].game_name, purchase_id: purchaseId });
    await db().from('purchases').update({ download_token: dlToken, downloaded: false, downloaded_at: null, status: 'approved' }).eq('purchase_id', purchaseId);

    // Re-notify player via Telegram
    try { await notifyPlayerApproved(normPhone(session.phone), tokenRows[0].game_name, pRow.quantity, pRow.amount, dlToken); } catch(e) {}

    return res.json({ ok: true, downloadToken: dlToken });
  }

  /* ── Admin: verify platform payment → auto-list game ── */
  if (action === 'verify-platform-payment') {
    const { password, paymentId } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!paymentId) return res.status(400).json({ error: 'paymentId required' });

    const { data: payRow } = await db().from('platform_payments').select('*').eq('id', paymentId).single();
    if (!payRow) return res.status(404).json({ error: 'Payment not found' });
    if (payRow.status !== 'pending') return res.status(409).json({ error: `Payment already ${payRow.status}` });

    const now = Date.now();
    await Promise.all([
      db().from('platform_payments').update({ status: 'verified', verified_at: now }).eq('id', paymentId),
      db().from('games').update({ status: 'listed' }).eq('id', payRow.game_id)
    ]);

    // Notify operator + broadcast to player channel
    const token = process.env.TELEGRAM_BOT_TOKEN;
    if (token) {
      const { data: opRow } = await db().from('operators')
        .select('telegram_chat_id, telegram_id, player_channel_id').eq('id', payRow.operator_id).single();
      const chatId = opRow?.telegram_chat_id || opRow?.telegram_id;
      if (chatId) {
        const host = process.env.APP_HOST || 'tungbola-market.vercel.app';
        const txt  = `✅ *Payment Verified!*\n\n🎮 *${payRow.game_name}* is now live on TungbolaMarket!\n\n📋 ${payRow.sheet_count} sheets · ₹${payRow.amount}\n\nPlayers can start booking now! 🎉`;
        const pl   = JSON.stringify({
          chat_id: chatId, text: txt, parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [[{ text: '🎮 View Game', url: `https://${host}/g/${payRow.game_id}` }]] }
        });
        const r = require('https').request({
          hostname: 'api.telegram.org', path: `/bot${token}/sendMessage`, method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(pl) }
        }, x => x.resume());
        r.on('error', () => {}); r.write(pl); r.end();
      }
      // Broadcast game announcement to player channel
      if (opRow?.player_channel_id) {
        try {
          const { data: gRow } = await db().from('games').select('*').eq('id', payRow.game_id).single();
          if (gRow) await broadcastGame(opRow.player_channel_id, gameFromRow(gRow));
        } catch(e) { console.error('Broadcast on web verify failed:', e.message); }
      }
    }

    return res.json({ ok: true });
  }

  /* ── Admin: reject platform payment ── */
  if (action === 'reject-platform-payment') {
    const { password, paymentId } = body;
    if (!checkPassword(password, process.env.ADMIN_PASSWORD))
      return res.status(401).json({ error: 'Wrong password' });
    if (!paymentId) return res.status(400).json({ error: 'paymentId required' });

    const { data: payRow } = await db().from('platform_payments').select('status,operator_id,game_name,operator_name').eq('id', paymentId).single();
    if (!payRow) return res.status(404).json({ error: 'Payment not found' });
    if (payRow.status !== 'pending') return res.status(409).json({ error: `Payment already ${payRow.status}` });

    await db().from('platform_payments').update({ status: 'rejected' }).eq('id', paymentId);

    // Notify operator
    const token = process.env.TELEGRAM_BOT_TOKEN;
    if (token) {
      const { data: opRow } = await db().from('operators')
        .select('telegram_chat_id, telegram_id').eq('id', payRow.operator_id).single();
      const chatId = opRow?.telegram_chat_id || opRow?.telegram_id;
      if (chatId) {
        const pl = JSON.stringify({
          chat_id: chatId,
          text: `❌ *Payment Rejected*\n\nYour listing payment for *${payRow.game_name}* was rejected.\n\nPlease re-submit with the correct UTR number.`,
          parse_mode: 'Markdown'
        });
        const r = require('https').request({
          hostname: 'api.telegram.org', path: `/bot${token}/sendMessage`, method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(pl) }
        }, x => x.resume());
        r.on('error', () => {}); r.write(pl); r.end();
      }
    }

    return res.json({ ok: true });
  }

  /* ── Public: get live game called numbers ── */
  if (action === 'get-live-game') {
    if (await rateLimit(req, 'getlive', 300, 60))
      return res.status(429).json({ error: 'Too many requests' });
    const { gameId } = body;
    if (!gameId) return res.status(400).json({ error: 'gameId required' });

    const { data } = await db().from('live_games').select('*').eq('game_id', gameId).single();
    return res.json({
      ok: true,
      calledNumbers: data?.called_numbers || [],
      lastNumber: data?.last_number || null,
      lastCalledAt: data?.last_called_at || null
    });
  }

  return res.status(400).json({ error: 'Unknown action' });
};
