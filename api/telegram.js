const { secureHeaders } = require('./_security');
const { db, gameFromRow, purchaseFromRow, sheetFromRow } = require('./_db');
const { sendPush } = require('./_push');
const crypto = require('crypto');
const https  = require('https');

// ── Send a message via Telegram Bot API ─────────────────────
async function tgSend(method, payload) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  if (!token) return;
  const body = JSON.stringify(payload);
  return new Promise((resolve) => {
    const req = https.request({
      hostname: 'api.telegram.org',
      path: `/bot${token}/${method}`,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
    }, res => { res.resume(); resolve(); });
    req.on('error', () => resolve());
    req.write(body); req.end();
  });
}

// ── Answer a callback query (required by Telegram) ──────────
async function answerCallback(callbackQueryId, text) {
  await tgSend('answerCallbackQuery', { callback_query_id: callbackQueryId, text, show_alert: false });
}

// ── Edit existing message after action ──────────────────────
async function editMessage(chatId, messageId, text) {
  await tgSend('editMessageText', { chat_id: chatId, message_id: messageId, text, parse_mode: 'Markdown' });
}

// ── Handle approve callback ──────────────────────────────────
async function handleApprove(purchaseId, chatId, messageId, callbackQueryId) {
  const { data: pRow } = await db().from('purchases').select('*').eq('purchase_id', purchaseId).single();
  if (!pRow) {
    await answerCallback(callbackQueryId, 'Order not found.');
    return;
  }
  if (pRow.status !== 'pending') {
    await answerCallback(callbackQueryId, `Already ${pRow.status}.`);
    await editMessage(chatId, messageId, `✅ *Order #${purchaseId.slice(-6)}*\nStatus: already ${pRow.status}`);
    return;
  }

  const purchase = purchaseFromRow(pRow);
  const { data: gRow } = await db().from('games').select('*').eq('id', purchase.gameId).single();
  if (!gRow) { await answerCallback(callbackQueryId, 'Game not found.'); return; }
  const game = gameFromRow(gRow);

  // Determine which sheet library to use (operator plan A vs shared)
  let sheetQuery;
  if (gRow.operator_id) {
    const { data: opRow } = await db().from('operators').select('plan').eq('id', gRow.operator_id).single();
    sheetQuery = opRow?.plan === 'own-sheets'
      ? db().from('operator_sheets').select('*').eq('operator_id', gRow.operator_id).gte('n', game.sheetFrom).lte('n', game.sheetTo)
      : db().from('sheets').select('*').gte('n', game.sheetFrom).lte('n', game.sheetTo);
  } else {
    sheetQuery = db().from('sheets').select('*').gte('n', game.sheetFrom).lte('n', game.sheetTo);
  }

  const { data: allSheets } = await sheetQuery;
  const soldSet = new Set(game.soldSheetNums);
  const available = (allSheets || []).filter(s => !soldSet.has(s.n));

  if (available.length < purchase.quantity) {
    await answerCallback(callbackQueryId, `Not enough sheets! Only ${available.length} left.`);
    return;
  }

  let assigned;
  if (purchase.requestedSheetNums?.length) {
    const reqSet = new Set(purchase.requestedSheetNums);
    assigned = [...available.filter(s => reqSet.has(s.n)), ...available.filter(s => !reqSet.has(s.n))].slice(0, purchase.quantity);
  } else {
    assigned = available.slice(0, purchase.quantity);
  }

  const sheetList = assigned.map(s => ({ n: s.n, filename: s.f, url: s.u }));
  const dlToken = Date.now().toString(36).toUpperCase() + Math.random().toString(36).slice(2, 9).toUpperCase();
  const now = Date.now();
  const newSoldNums = [...game.soldSheetNums, ...assigned.map(s => s.n)];

  await Promise.all([
    db().from('download_tokens').insert({ token: dlToken, sheets: sheetList, game_name: game.name, purchase_id: purchaseId }),
    db().from('games').update({ sold_sheet_nums: newSoldNums, sold_count: newSoldNums.length }).eq('id', game.id),
    db().from('purchases').update({ status: 'approved', download_token: dlToken, approved_at: now, sheet_nums: assigned.map(s => s.n) }).eq('purchase_id', purchaseId)
  ]);

  // Push notification to player
  try {
    const np = String(purchase.phone).replace(/\D/g, '');
    const { data: pushRow } = await db().from('push_subscriptions').select('subscription').eq('phone', np).single();
    if (pushRow?.subscription) await sendPush(pushRow.subscription);
  } catch(e) {}

  await answerCallback(callbackQueryId, '✅ Approved! Player notified.');
  await editMessage(chatId, messageId,
    `✅ *Approved* — #${purchaseId.slice(-6)}\n\n👤 ${purchase.playerName}\n🎮 ${purchase.gameName}\n📋 ${purchase.quantity} sheets assigned\n💰 ₹${purchase.amount}`
  );
}

// ── Handle reject callback ───────────────────────────────────
async function handleReject(purchaseId, chatId, messageId, callbackQueryId) {
  const { data: pRow } = await db().from('purchases').select('status,player_name,game_name,quantity,amount').eq('purchase_id', purchaseId).single();
  if (!pRow) { await answerCallback(callbackQueryId, 'Order not found.'); return; }
  if (pRow.status !== 'pending') {
    await answerCallback(callbackQueryId, `Already ${pRow.status}.`);
    return;
  }
  await db().from('purchases').update({ status: 'rejected' }).eq('purchase_id', purchaseId);
  await answerCallback(callbackQueryId, '❌ Order rejected.');
  await editMessage(chatId, messageId,
    `❌ *Rejected* — #${purchaseId.slice(-6)}\n\n👤 ${pRow.player_name}\n🎮 ${pRow.game_name}\n📋 ${pRow.quantity} sheets\n💰 ₹${pRow.amount}`
  );
}

// ── Main webhook handler ─────────────────────────────────────
module.exports = async function(req, res) {
  secureHeaders(res);

  // Telegram always sends POST
  if (req.method !== 'POST') return res.status(405).end();

  // Validate secret token if configured
  const secret = process.env.TELEGRAM_WEBHOOK_SECRET;
  if (secret) {
    const incoming = req.headers['x-telegram-bot-api-secret-token'];
    if (!incoming || !crypto.timingSafeEqual(Buffer.from(incoming), Buffer.from(secret)))
      return res.status(401).end();
  }

  // Always respond 200 immediately (Telegram retries if it gets anything else)
  res.status(200).json({ ok: true });

  try {
    const update = req.body || {};

    // Handle inline button presses
    if (update.callback_query) {
      const cb = update.callback_query;
      const [cbAction, purchaseId] = (cb.data || '').split(':');
      const chatId = cb.message?.chat?.id;
      const messageId = cb.message?.message_id;
      const callbackQueryId = cb.id;

      if (!purchaseId || !chatId || !messageId) return;

      if (cbAction === 'approve') {
        await handleApprove(purchaseId, chatId, messageId, callbackQueryId);
      } else if (cbAction === 'reject') {
        await handleReject(purchaseId, chatId, messageId, callbackQueryId);
      }
    }
  } catch(e) {
    console.error('Telegram webhook error:', e.message);
  }
};
