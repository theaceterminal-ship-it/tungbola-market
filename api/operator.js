const { put } = require('@vercel/blob');
const { handleUpload } = require('@vercel/blob/client');
const { secureHeaders, rateLimit } = require('./_security');
const { db, gameFromRow, gameToRow, purchaseFromRow, purchaseToRow, operatorFromRow, sheetFromRow } = require('./_db');
const { sendPush } = require('./_push');
const { notifyPlayerApproved, notifyPlayerRejected, broadcastGame } = require('./telegram');
const crypto = require('crypto');

function genId()    { return Date.now().toString(36) + Math.random().toString(36).slice(2, 6); }
function genToken() { return Date.now().toString(36).toUpperCase() + Math.random().toString(36).slice(2, 9).toUpperCase(); }

function formatZoomId(id) {
  const s = String(id).replace(/\D/g, '');
  if (s.length <= 9)  return s.replace(/(\d{3})(\d{3})(\d{3})/, '$1 $2 $3');
  if (s.length === 10) return s.replace(/(\d{3})(\d{3})(\d{4})/, '$1 $2 $3');
  return s.replace(/(\d{3})(\d{4})(\d{4})/, '$1 $2 $3'); // 11 digits
}

async function getZoomAccessToken(operator) {
  if (!operator.zoomAccessToken) throw new Error('Zoom not connected');
  if (operator.zoomTokenExpiry && Date.now() < operator.zoomTokenExpiry - 300000) {
    return operator.zoomAccessToken;
  }
  if (!operator.zoomRefreshToken) throw new Error('Zoom session expired — please reconnect');
  const r = await fetch('https://zoom.us/oauth/token', {
    method: 'POST',
    headers: {
      'Authorization': 'Basic ' + Buffer.from(`${process.env.ZOOM_CLIENT_ID}:${process.env.ZOOM_CLIENT_SECRET}`).toString('base64'),
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token: operator.zoomRefreshToken }).toString(),
  });
  if (!r.ok) throw new Error('Zoom token refresh failed — please reconnect');
  const tokens = await r.json();
  await db().from('operators').update({
    zoom_access_token:  tokens.access_token,
    zoom_refresh_token: tokens.refresh_token,
    zoom_token_expiry:  Date.now() + (tokens.expires_in * 1000),
  }).eq('id', operator.id);
  return tokens.access_token;
}

async function getOperator(apiKey) {
  if (!apiKey) return null;
  const { data } = await db().from('operators').select('*').eq('api_key', String(apiKey)).eq('active', true).single();
  return data ? operatorFromRow(data) : null;
}

module.exports = async function(req, res) {
  secureHeaders(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).end();

  const body = req.body || {};
  const { action, apiKey } = body;

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

  /* ── Vercel Blob client-upload handler (Plan A) ── */
  if (body.type === 'blob.generate-client-token' || body.type === 'blob.upload-completed') {
    if (body.type === 'blob.generate-client-token') {
      if (await rateLimit(req, 'op-upload', 5000, 3600))
        return res.status(429).json({ error: 'Upload rate limit reached' });
    }
    const headers = { get: name => req.headers[name.toLowerCase()] || null };
    try {
      const response = await handleUpload({
        body, request: { headers },
        onBeforeGenerateToken: async (pathname, clientPayload) => {
          let payload = {};
          try { payload = JSON.parse(clientPayload || '{}'); } catch {}
          const op = await getOperator(payload.apiKey);
          if (!op) throw new Error('Invalid API key');
          if (op.plan !== 'own-sheets') throw new Error('Plan A only');
          const num = parseInt(payload.sheetNum);
          if (!num || num < 1 || num > 9999) throw new Error('Invalid sheet number (1–9999)');
          return {
            allowedContentTypes: ['application/pdf', 'image/jpeg', 'image/png'],
            maximumSizeInBytes: 10 * 1024 * 1024,
            tokenPayload: JSON.stringify({ operatorId: op.id })
          };
        },
        onUploadCompleted: async () => {}
      });
      return res.json(response);
    } catch(e) { return res.status(400).json({ error: e.message || 'Upload failed' }); }
  }

  if (!apiKey) return res.status(401).json({ error: 'apiKey required' });
  if (await rateLimit(req, `opapi:${String(apiKey).slice(0, 8)}`, 200, 60))
    return res.status(429).json({ error: 'Too many requests' });

  const operator = await getOperator(apiKey);
  if (!operator) return res.status(401).json({ error: 'Invalid API key' });

  /* ── Get operator info + their games ── */
  if (action === 'get-info') {
    const { data: gameRows } = await db().from('games').select('*').eq('operator_id', operator.id).order('created_at', { ascending: false });
    return res.json({
      ok: true,
      operator: {
        id: operator.id, name: operator.name, plan: operator.plan,
        telegramChatId: operator.telegramChatId || null,
        playerChannelId: operator.playerChannelId || null,
        displayName: operator.displayName || null,
        supportPhone: operator.supportPhone || null,
        upiId: operator.upiId || null,
        zoomConnected: operator.zoomConnected,
      },
      games: (gameRows || []).map(r => gameFromRow(r))
    });
  }

  /* ── Update operator profile ── */
  if (action === 'update-profile') {
    const { displayName, supportPhone, upiId } = body;
    const updates = {};
    if (displayName  !== undefined) updates.display_name  = displayName  ? String(displayName).trim().slice(0, 80)  : null;
    if (supportPhone !== undefined) updates.support_phone = supportPhone ? String(supportPhone).trim().slice(0, 20)  : null;
    if (upiId        !== undefined) updates.upi_id        = upiId        ? String(upiId).trim().slice(0, 100) : null;
    if (Object.keys(updates).length) {
      await db().from('operators').update(updates).eq('id', operator.id);
    }
    return res.json({ ok: true });
  }

  /* ── Zoom: return OAuth URL ── */
  if (action === 'zoom-auth-url') {
    const clientId = process.env.ZOOM_CLIENT_ID;
    const redirectUri = process.env.ZOOM_REDIRECT_URI;
    if (!clientId || !redirectUri) return res.status(503).json({ error: 'Zoom integration not configured on this platform' });
    const url = `https://zoom.us/oauth/authorize?response_type=code&client_id=${encodeURIComponent(clientId)}&redirect_uri=${encodeURIComponent(redirectUri)}&state=${encodeURIComponent(operator.apiKey)}`;
    return res.json({ ok: true, url });
  }

  /* ── Zoom: create a meeting ── */
  if (action === 'zoom-create-meeting') {
    const { topic, startTime, duration = 60 } = body;
    if (!topic || !startTime) return res.status(400).json({ error: 'topic and startTime required' });
    let accessToken;
    try { accessToken = await getZoomAccessToken(operator); }
    catch (e) { return res.status(400).json({ error: e.message }); }
    const meetingRes = await fetch('https://api.zoom.us/v2/users/me/meetings', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        topic,
        type: 2,
        start_time: new Date(startTime).toISOString(),
        duration,
        timezone: 'Asia/Kolkata',
        settings: { join_before_host: true, waiting_room: false },
      }),
    });
    if (!meetingRes.ok) {
      const err = await meetingRes.json().catch(() => ({}));
      return res.status(400).json({ error: err.message || 'Failed to create Zoom meeting' });
    }
    const meeting = await meetingRes.json();
    const meetingId = formatZoomId(meeting.id);
    const joinDetails = `Meeting ID: ${meetingId}\nPasscode: ${meeting.password}`;
    return res.json({ ok: true, joinUrl: meeting.join_url, meetingId, passcode: meeting.password, joinDetails });
  }

  /* ── Zoom: disconnect ── */
  if (action === 'zoom-disconnect') {
    await db().from('operators').update({
      zoom_access_token: null, zoom_refresh_token: null, zoom_token_expiry: null,
    }).eq('id', operator.id);
    return res.json({ ok: true });
  }

  /* ── Update operator's Telegram settings ── */
  if (action === 'update-telegram') {
    const { telegramChatId, playerChannelId } = body;
    const updates = {};
    if (telegramChatId !== undefined) updates.telegram_chat_id = telegramChatId ? String(telegramChatId).trim() : null;
    if (playerChannelId !== undefined) updates.player_channel_id = playerChannelId ? String(playerChannelId).trim() : null;
    if (Object.keys(updates).length) await db().from('operators').update(updates).eq('id', operator.id);
    return res.json({ ok: true });
  }

  /* ── Get purchases for operator's games ── */
  if (action === 'get-purchases') {
    const { data: gameRows } = await db().from('games').select('id').eq('operator_id', operator.id);
    const myGameIds = (gameRows || []).map(g => g.id);
    if (!myGameIds.length) return res.json({ ok: true, purchases: [] });
    const { data: rows } = await db().from('purchases').select('*').in('game_id', myGameIds).order('created_at', { ascending: false }).limit(500);
    return res.json({ ok: true, purchases: (rows || []).map(purchaseFromRow) });
  }

  /* ── Create game ── */
  if (action === 'create-game') {
    if (await rateLimit(req, `op-cg:${operator.id}`, 30, 3600))
      return res.status(429).json({ error: 'Too many requests' });
    const { name, gameDate, gameDateRaw, pricePerSheet, description, prizes, thumbnail, pricingTiers, joinTime, joinLink, joinDetails } = body;
    if (!name) return res.status(400).json({ error: 'Game name required' });

    const game = {
      id: genId(), operatorId: operator.id, operatorName: operator.name,
      name: String(name).trim().slice(0, 80),
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

  /* ── Edit game ── */
  if (action === 'edit-game') {
    const { gameId, name, gameDate, gameDateRaw, pricePerSheet, description, prizes, thumbnail, pricingTiers, joinTime, joinLink, joinDetails } = body;
    if (!gameId) return res.status(400).json({ error: 'gameId required' });

    const { data: gRow } = await db().from('games').select('operator_id').eq('id', gameId).single();
    if (!gRow) return res.status(404).json({ error: 'Game not found' });
    if (gRow.operator_id !== operator.id) return res.status(403).json({ error: 'Not your game' });

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

  /* ── Assign sheets to game ── */
  if (action === 'assign-sheets') {
    const { gameId, sheetFrom, sheetTo } = body;
    if (!gameId || !sheetFrom || !sheetTo)
      return res.status(400).json({ error: 'gameId, sheetFrom, sheetTo required' });

    const { data: gRow } = await db().from('games').select('*').eq('id', gameId).single();
    if (!gRow) return res.status(404).json({ error: 'Game not found' });
    if (gRow.operator_id !== operator.id) return res.status(403).json({ error: 'Not your game' });

    const from = parseInt(sheetFrom), to = parseInt(sheetTo);
    if (from < 1 || to < from) return res.status(400).json({ error: 'Invalid range' });

    const sheetTable = operator.plan === 'own-sheets' ? 'operator_sheets' : 'sheets';
    const query = operator.plan === 'own-sheets'
      ? db().from('operator_sheets').select('n').eq('operator_id', operator.id).gte('n', from).lte('n', to)
      : db().from('sheets').select('n').gte('n', from).lte('n', to);
    const { data: sheetRows } = await query;
    if (!sheetRows?.length) return res.status(400).json({ error: `No sheets in range ${from}–${to}` });

    await db().from('games').update({
      sheet_from: from, sheet_to: to, sheet_count: sheetRows.length,
      sold_count: 0, sold_sheet_nums: []
    }).eq('id', gameId);

    const game = gameFromRow({ ...gRow, sheet_from: from, sheet_to: to, sheet_count: sheetRows.length, sold_count: 0, sold_sheet_nums: [] });
    return res.json({ ok: true, game, sheetsFound: sheetRows.length });
  }

  /* ── Set game status ── */
  if (action === 'set-status') {
    const { gameId, status } = body;
    if (!['listed', 'ended', 'draft'].includes(status))
      return res.status(400).json({ error: 'status must be listed, ended or draft' });

    const { data: gRow } = await db().from('games').select('operator_id,sheet_count,status').eq('id', gameId).single();
    if (!gRow) return res.status(404).json({ error: 'Game not found' });
    if (gRow.operator_id !== operator.id) return res.status(403).json({ error: 'Not your game' });
    if (status === 'listed' && !operator.upiId)
      return res.status(400).json({ error: 'Add your UPI ID in your operator profile before publishing a game.', code: 'NO_UPI' });
    if (status === 'listed' && !gRow.sheet_count)
      return res.status(400).json({ error: 'Assign sheets before listing' });

    const wasListed = gRow.status !== 'listed';
    const { data: updatedGame } = await db().from('games').update({ status }).eq('id', gameId).select().single();

    if (status === 'listed' && wasListed && updatedGame) {
      try {
        const { data: opRow } = await db().from('operators').select('player_channel_id').eq('id', operator.id).single();
        if (opRow?.player_channel_id) await broadcastGame(opRow.player_channel_id, gameFromRow(updatedGame));
      } catch(e) { console.error('Broadcast on set-status failed:', e.message); }
    }

    return res.json({ ok: true });
  }

  /* ── Delete game ── */
  if (action === 'delete-game') {
    const { gameId } = body;
    const { data: gRow } = await db().from('games').select('operator_id').eq('id', gameId).single();
    if (!gRow) return res.status(404).json({ error: 'Game not found' });
    if (gRow.operator_id !== operator.id) return res.status(403).json({ error: 'Not your game' });

    await db().from('purchases').delete().eq('game_id', gameId);
    await db().from('games').delete().eq('id', gameId);
    return res.json({ ok: true });
  }

  /* ── Upload thumbnail ── */
  if (action === 'upload-thumbnail') {
    const { data, filename } = body;
    if (!data) return res.status(400).json({ error: 'data required' });
    let buffer;
    try { buffer = Buffer.from(data, 'base64'); } catch(e) { return res.status(400).json({ error: 'Invalid base64' }); }
    if (buffer.length > 5 * 1024 * 1024) return res.status(400).json({ error: 'File too large (max 5MB)' });
    const ext = String(filename || 'thumb.jpg').split('.').pop().toLowerCase();
    const ct = ['png','gif','webp'].includes(ext) ? `image/${ext}` : 'image/jpeg';
    try {
      const blob = await put(`tungbola/op-${operator.id}/thumb-${Date.now()}.${ext}`, buffer, { access: 'public', contentType: ct });
      return res.json({ ok: true, url: blob.url });
    } catch(e) { return res.status(500).json({ error: `Upload failed: ${e.message}` }); }
  }

  /* ── Approve purchase ── */
  if (action === 'approve-purchase') {
    const { purchaseId } = body;
    if (!purchaseId) return res.status(400).json({ error: 'purchaseId required' });

    const { data: pRow } = await db().from('purchases').select('*').eq('purchase_id', purchaseId).single();
    if (!pRow) return res.status(404).json({ error: 'Purchase not found' });
    if (pRow.status !== 'pending') return res.status(409).json({ error: 'Already processed' });

    const { data: gRow } = await db().from('games').select('*').eq('id', pRow.game_id).single();
    if (!gRow) return res.status(404).json({ error: 'Game not found' });
    if (gRow.operator_id !== operator.id) return res.status(403).json({ error: 'Not your game' });

    const game = gameFromRow(gRow);
    const purchase = purchaseFromRow(pRow);

    const sheetQuery = operator.plan === 'own-sheets'
      ? db().from('operator_sheets').select('*').eq('operator_id', operator.id).gte('n', game.sheetFrom).lte('n', game.sheetTo)
      : db().from('sheets').select('*').gte('n', game.sheetFrom).lte('n', game.sheetTo);
    const { data: allSheets } = await sheetQuery;

    if (operator.plan === 'generate' && !allSheets?.length)
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
    const np = String(purchase.phone).replace(/\D/g, '');
    try {
      const { data: pushRow } = await db().from('push_subscriptions').select('subscription').eq('phone', np).single();
      if (pushRow?.subscription) await sendPush(pushRow.subscription);
    } catch(e) { console.error('Push failed:', e.message); }
    try {
      await notifyPlayerApproved(np, game.name, purchase.quantity, purchase.amount, dlToken, game.joinLink, game.joinDetails);
    } catch(e) { console.error('Telegram notify failed:', e.message); }

    return res.json({ ok: true, downloadToken: dlToken, sheetsAssigned: assigned.length });
  }

  /* ── Reject purchase ── */
  if (action === 'reject-purchase') {
    const { purchaseId } = body;
    if (!purchaseId) return res.status(400).json({ error: 'purchaseId required' });

    const { data: pRow } = await db().from('purchases').select('*').eq('purchase_id', purchaseId).single();
    if (!pRow) return res.status(404).json({ error: 'Purchase not found' });
    if (pRow.status !== 'pending') return res.status(409).json({ error: 'Already processed' });

    const { data: gRow } = await db().from('games').select('operator_id').eq('id', pRow.game_id).single();
    if (gRow?.operator_id !== operator.id) return res.status(403).json({ error: 'Not your game' });

    await db().from('purchases').update({ status: 'rejected' }).eq('purchase_id', purchaseId);

    try {
      const np = String(pRow.phone || '').replace(/\D/g, '');
      await notifyPlayerRejected(np, pRow.game_name, pRow.quantity, pRow.amount);
    } catch(e) { console.error('Telegram notify failed:', e.message); }

    return res.json({ ok: true });
  }

  /* ── Call a number (live game) ── */
  if (action === 'call-number') {
    const { gameId, number } = body;
    if (!gameId) return res.status(400).json({ error: 'gameId required' });
    const num = parseInt(number);
    if (!num || num < 1 || num > 90) return res.status(400).json({ error: 'Number must be 1–90' });

    const { data: gRow } = await db().from('games').select('operator_id').eq('id', gameId).single();
    if (!gRow) return res.status(404).json({ error: 'Game not found' });
    if (gRow.operator_id !== operator.id) return res.status(403).json({ error: 'Not your game' });

    const { data: liveRow } = await db().from('live_games').select('*').eq('game_id', gameId).single();
    const calledNumbers = liveRow?.called_numbers || [];
    if (!calledNumbers.includes(num)) calledNumbers.push(num);

    const newState = {
      game_id: gameId, called_numbers: calledNumbers, last_number: num,
      last_called_at: Date.now(), expires_at: new Date(Date.now() + 7200000).toISOString()
    };
    await db().from('live_games').upsert(newState);
    return res.json({ ok: true, calledNumbers, lastNumber: num });
  }

  /* ── Reset live game ── */
  if (action === 'reset-live') {
    const { gameId } = body;
    const { data: gRow } = await db().from('games').select('operator_id').eq('id', gameId).single();
    if (!gRow) return res.status(404).json({ error: 'Game not found' });
    if (gRow.operator_id !== operator.id) return res.status(403).json({ error: 'Not your game' });
    await db().from('live_games').delete().eq('game_id', gameId);
    return res.json({ ok: true });
  }

  /* ── Upload sheet to operator library (Plan A) ── */
  if (action === 'upload-sheet') {
    if (operator.plan !== 'own-sheets') return res.status(403).json({ error: 'Own Sheets plan required' });
    const { sheetNum, data, filename } = body;
    if (!sheetNum || !data) return res.status(400).json({ error: 'sheetNum and data required' });
    const num = parseInt(sheetNum);
    if (!num || num < 1 || num > 9999) return res.status(400).json({ error: 'Invalid sheet number (1–9999)' });
    let buffer;
    try { buffer = Buffer.from(data, 'base64'); } catch(e) { return res.status(400).json({ error: 'Invalid base64' }); }
    if (buffer.length > 5 * 1024 * 1024) return res.status(400).json({ error: 'File too large (max 5MB)' });
    const ext = String(filename || 'sheet.pdf').split('.').pop().toLowerCase();
    const ct = ext === 'pdf' ? 'application/pdf' : 'image/jpeg';
    try {
      const blob = await put(`tungbola/op-${operator.id}/sheet-${num}.${ext}`, buffer, { access: 'public', contentType: ct });
      await db().from('operator_sheets').upsert({ operator_id: operator.id, n: num, f: filename || `sheet-${num}.${ext}`, u: blob.url, s: buffer.length, ts: Date.now() });
      return res.json({ ok: true, url: blob.url, n: num });
    } catch(e) { return res.status(500).json({ error: `Upload failed: ${e.message}` }); }
  }

  /* ── Register sheet after client upload (Plan A) ── */
  if (action === 'register-sheet') {
    if (operator.plan !== 'own-sheets') return res.status(403).json({ error: 'Plan A only' });
    const { sheetNum, filename, url } = body;
    if (!url || !sheetNum || !filename) return res.status(400).json({ error: 'sheetNum, filename, url required' });
    const num = parseInt(sheetNum);
    if (!num || num < 1 || num > 9999) return res.status(400).json({ error: 'Invalid sheet number (1–9999)' });
    await db().from('operator_sheets').upsert({ operator_id: operator.id, n: num, f: String(filename).trim().slice(0, 80), u: url, ts: Date.now() });
    return res.json({ ok: true, sheet: { n: num, f: filename, u: url } });
  }

  /* ── Sheet library stats ── */
  if (action === 'sheet-library-stats') {
    const { count } = await db().from('operator_sheets').select('*', { count: 'exact', head: true }).eq('operator_id', operator.id);
    return res.json({ ok: true, count: count || 0, plan: operator.plan });
  }

  /* ── Get specific sheet from library ── */
  if (action === 'get-sheet') {
    const { sheetNum } = body;
    const num = parseInt(sheetNum);
    if (!num || num < 1) return res.status(400).json({ error: 'sheetNum required' });
    const { data: sheetRow } = await db().from('operator_sheets').select('*').eq('operator_id', operator.id).eq('n', num).single();
    if (!sheetRow) return res.status(404).json({ error: 'Sheet not found' });
    return res.json({ ok: true, sheet: sheetFromRow(sheetRow) });
  }

  return res.status(400).json({ error: 'Unknown action' });
};
