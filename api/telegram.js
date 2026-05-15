const { secureHeaders } = require('./_security');
const { db, gameFromRow, purchaseFromRow } = require('./_db');
const { sendPush } = require('./_push');
const crypto = require('crypto');
const https  = require('https');

// ─────────────────────────────────────────────────────────────
// Telegram API helpers
// ─────────────────────────────────────────────────────────────

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

async function tgSendRead(method, payload) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  if (!token) return null;
  const body = JSON.stringify(payload);
  return new Promise((resolve) => {
    const req = https.request({
      hostname: 'api.telegram.org',
      path: `/bot${token}/${method}`,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
    }, res => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => { try { resolve(JSON.parse(d)); } catch { resolve(null); } });
    });
    req.on('error', () => resolve(null));
    req.write(body); req.end();
  });
}

async function tgGet(path) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  if (!token) return null;
  return new Promise((resolve) => {
    const req = https.request({
      hostname: 'api.telegram.org',
      path: `/bot${token}/${path}`,
      method: 'GET'
    }, res => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => { try { resolve(JSON.parse(d)); } catch { resolve(null); } });
    });
    req.on('error', () => resolve(null));
    req.end();
  });
}

async function tgReply(chatId, text, extra = {}) {
  await tgSend('sendMessage', { chat_id: chatId, text, parse_mode: 'Markdown', ...extra });
}

async function answerCallback(callbackQueryId, text) {
  await tgSend('answerCallbackQuery', { callback_query_id: callbackQueryId, text, show_alert: false });
}

async function editMessage(chatId, messageId, text, extra = {}) {
  await tgSend('editMessageText', { chat_id: chatId, message_id: messageId, text, parse_mode: 'Markdown', ...extra });
}

// ─────────────────────────────────────────────────────────────
// Operator helpers
// ─────────────────────────────────────────────────────────────

async function getOpByTgId(telegramId) {
  const { data } = await db().from('operators').select('*').eq('telegram_id', String(telegramId)).single();
  return data || null;
}

// ─────────────────────────────────────────────────────────────
// Player Telegram helpers
// ─────────────────────────────────────────────────────────────

async function getPlayerPhone(telegramId) {
  const { data } = await db().from('player_telegram').select('phone').eq('telegram_id', String(telegramId)).single();
  return data?.phone || null;
}

async function notifyPlayerApproved(phone, gameName, quantity, amount, dlToken) {
  try {
    const { data } = await db().from('player_telegram').select('telegram_id').eq('phone', String(phone)).single();
    if (!data?.telegram_id) return;
    const host = process.env.APP_HOST || 'tungbola-market.vercel.app';
    await tgSend('sendMessage', {
      chat_id: data.telegram_id,
      text:
        `✅ *Order Approved!*\n\n` +
        `🎮 ${gameName}\n` +
        `📋 ${quantity} sheets · ₹${amount}\n\n` +
        `Tap below to download your sheets.\n_Link expires in 6 hours._`,
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [[{ text: '📥 Download Sheets', url: `https://${host}/?dl=${dlToken}` }]]
      }
    });
  } catch (e) {}
}

async function notifyPlayerRejected(phone, gameName, quantity, amount) {
  try {
    const { data } = await db().from('player_telegram').select('telegram_id').eq('phone', String(phone)).single();
    if (!data?.telegram_id) return;
    await tgSend('sendMessage', {
      chat_id: data.telegram_id,
      text:
        `❌ *Order Declined*\n\n` +
        `🎮 ${gameName}\n` +
        `📋 ${quantity} sheets · ₹${amount}\n\n` +
        `Contact the operator for details.`,
      parse_mode: 'Markdown'
    });
  } catch (e) {}
}

// ─────────────────────────────────────────────────────────────
// Wizard session helpers
// ─────────────────────────────────────────────────────────────

async function getSession(telegramId) {
  const { data } = await db().from('bot_sessions').select('*').eq('telegram_id', String(telegramId)).single();
  if (!data) return null;
  // Expire sessions idle for more than 24 hours
  if (Date.now() - new Date(data.updated_at).getTime() > 86400000) {
    await db().from('bot_sessions').delete().eq('telegram_id', String(telegramId));
    return null;
  }
  return data;
}

async function setSession(telegramId, step, data) {
  await db().from('bot_sessions').upsert(
    { telegram_id: String(telegramId), step, data: data || {}, updated_at: new Date().toISOString() },
    { onConflict: 'telegram_id' }
  );
}

async function clearSession(telegramId) {
  await db().from('bot_sessions').delete().eq('telegram_id', String(telegramId));
}

// ─────────────────────────────────────────────────────────────
// Prize types + UI helpers
// ─────────────────────────────────────────────────────────────

const PRIZE_TYPES = [
  'Full House', 'Second Full House', 'Third Full House',
  'Upper Line', 'Middle Line', 'Bottom Line',
  'Ticket Corners', 'Sheet Corner',
  'Early 5', 'Early 6', 'Early 7', 'Jackpot'
];

const PRIZE_EMOJI = {
  'Full House': '🏆', 'Second Full House': '🥈', 'Third Full House': '🥉',
  'Upper Line': '⬆️', 'Middle Line': '➡️', 'Bottom Line': '⬇️',
  'Ticket Corners': '🎫', 'Sheet Corner': '📄',
  'Early 5': '⚡', 'Early 6': '⚡', 'Early 7': '⚡', 'Jackpot': '💎'
};

function dateKeyboard() {
  const row = [];
  const lbl = ['Today', 'Tomorrow'];
  for (let i = 0; i < 3; i++) {
    const d = new Date();
    d.setDate(d.getDate() + i);
    const val = d.toLocaleDateString('en-IN', { day: 'numeric', month: 'short' });
    row.push({ text: i < 2 ? lbl[i] : val, callback_data: `w_date:${val}` });
  }
  return { inline_keyboard: [row, [{ text: 'Skip', callback_data: 'w_skip' }]] };
}

const TIME_KB = {
  inline_keyboard: [
    [
      { text: '6 PM',  callback_data: 'w_time:6:00 PM'  },
      { text: '7 PM',  callback_data: 'w_time:7:00 PM'  },
      { text: '8 PM',  callback_data: 'w_time:8:00 PM'  },
      { text: '9 PM',  callback_data: 'w_time:9:00 PM'  },
      { text: '10 PM', callback_data: 'w_time:10:00 PM' }
    ],
    [{ text: 'Skip', callback_data: 'w_skip' }]
  ]
};

const SKIP_KB = { inline_keyboard: [[{ text: 'Skip', callback_data: 'w_skip' }]] };
const NOW_KB  = { inline_keyboard: [[{ text: '🚀 List immediately', callback_data: 'w_sched:now' }]] };

// ─────────────────────────────────────────────────────────────
// Operator commands
// ─────────────────────────────────────────────────────────────

async function handleOperatorHelp(chatId) {
  await tgReply(chatId,
    `*Tungbola Operator Bot*\n\n` +
    `/newgame — Create a game _(listing fee applies)_\n` +
    `/stats — Live sales stats\n` +
    `/setchannel @ch — Set your player channel\n` +
    `/link APIKEY — Link your account\n` +
    `/cancel — Cancel current action`
  );
}

async function handleLink(chatId, telegramId, args) {
  const apiKey = (args || '').trim();
  if (!apiKey) {
    await tgReply(chatId, `Usage: \`/link YOUR_API_KEY\`\nFind it in your operator dashboard.`);
    return;
  }
  const { data: opRow } = await db().from('operators').select('*').eq('api_key', apiKey).single();
  if (!opRow) { await tgReply(chatId, '❌ API key not found.'); return; }
  const updates = { telegram_id: String(telegramId) };
  if (!opRow.telegram_chat_id) updates.telegram_chat_id = String(chatId);
  await db().from('operators').update(updates).eq('id', opRow.id);
  await tgReply(chatId, `✅ *Linked — ${opRow.name}*\n\n/newgame · /stats · /setchannel`);
}

async function handleSetChannel(chatId, telegramId, args) {
  const op = await getOpByTgId(telegramId);
  if (!op) { await tgReply(chatId, '❌ Not linked. Use `/link YOUR_API_KEY` first.'); return; }
  const channelId = (args || '').trim();
  if (!channelId) {
    await tgReply(chatId,
      `*Set player channel*\n\n1. Add bot as Admin to your channel\n2. Run: \`/setchannel @your_channel\``
    );
    return;
  }
  const result = await tgSendRead('sendMessage', {
    chat_id: channelId,
    text: '✅ Channel connected to Tungbola Market bot.'
  });
  if (!result?.ok) {
    await tgReply(chatId, `❌ Can't post to \`${channelId}\`. Make the bot an Admin first.`);
    return;
  }
  await db().from('operators').update({ player_channel_id: channelId }).eq('id', op.id);
  await tgReply(chatId, `✅ Player channel set to \`${channelId}\`.`);
}

async function handleStats(chatId, telegramId) {
  const op = await getOpByTgId(telegramId);
  if (!op) { await tgReply(chatId, '❌ Not linked. Use `/link YOUR_API_KEY` first.'); return; }

  const { data: gRows } = await db().from('games')
    .select('id, name, sheet_count, sheet_from, sheet_to, sold_count')
    .eq('operator_id', op.id)
    .eq('status', 'listed');

  if (!gRows?.length) {
    await tgReply(chatId, `📊 *${op.name}*\n\nNo active games.\n/newgame to create one.`);
    return;
  }

  const gameIds = gRows.map(g => g.id);
  let totalRem = 0;
  const lines = gRows.map(g => {
    const total = g.sheet_count || (g.sheet_to - g.sheet_from + 1);
    const rem = total - (g.sold_count || 0);
    totalRem += rem;
    return `  • ${g.name}: *${rem}* left`;
  }).join('\n');

  const { count: pending } = await db().from('purchases')
    .select('*', { count: 'exact', head: true })
    .in('game_id', gameIds).eq('status', 'pending');

  const todayMs = new Date().setHours(0, 0, 0, 0);
  const { data: sales } = await db().from('purchases')
    .select('quantity, amount')
    .in('game_id', gameIds)
    .in('status', ['approved', 'downloaded'])
    .gte('created_at', todayMs);

  const sold = (sales || []).reduce((s, p) => s + (p.quantity || 0), 0);
  const rev  = (sales || []).reduce((s, p) => s + (p.amount  || 0), 0);

  await tgReply(chatId,
    `📊 *${op.name}*\n\n` +
    `📋 *${totalRem} sheets remaining*\n${lines}\n\n` +
    `⏳ *${pending || 0} pending approvals*\n\n` +
    `📈 *Today — ${sold} sold · ₹${rev.toLocaleString('en-IN')}*`
  );
}

async function handleCancel(chatId, telegramId) {
  const had = await getSession(telegramId);
  await clearSession(telegramId);
  await tgReply(chatId, had ? '✅ Cancelled.' : 'Nothing to cancel.');
}

// ─────────────────────────────────────────────────────────────
// Player commands
// ─────────────────────────────────────────────────────────────

async function handlePlayerStart(chatId, telegramId) {
  const existing = await getPlayerPhone(telegramId);
  if (existing) {
    await tgReply(chatId,
      `👋 *Welcome back!*\n\nDownload links arrive here automatically when orders are approved.\n\n/games — Browse & buy sheets\n/myorders — Your orders`
    );
    return;
  }
  await clearSession(telegramId);
  await setSession(telegramId, 'p_phone', {});
  await tgReply(chatId,
    `👋 *Welcome to Tungbola!*\n\nSend your *registered phone number* to link your account.\nYou'll receive order approvals and download links here automatically.`
  );
}

async function handleMyOrders(chatId, telegramId, args) {
  let phone = await getPlayerPhone(telegramId);

  if (!phone) {
    if (args) {
      phone = args.replace(/\D/g, '');
    } else {
      await clearSession(telegramId);
      await setSession(telegramId, 'p_phone', {});
      await tgReply(chatId, `Send your registered phone number to link your account.`);
      return;
    }
  }

  const { data: orders } = await db().from('purchases')
    .select('purchase_id, game_name, quantity, amount, status, download_token')
    .eq('phone', phone)
    .order('created_at', { ascending: false })
    .limit(6);

  if (!orders?.length) {
    await tgReply(chatId, `No orders found.`);
    return;
  }

  const host = process.env.APP_HOST || 'tungbola-market.vercel.app';
  const keyboard = [];
  let msg = `📦 *Your Orders*\n\n`;

  for (const o of orders) {
    const e = o.status === 'approved' ? '✅' : o.status === 'downloaded' ? '📥' : o.status === 'pending' ? '⏳' : '❌';
    msg += `${e} *${o.game_name}* — ${o.quantity} sheets · ₹${o.amount}\n`;
    if (o.status === 'approved' && o.download_token) {
      keyboard.push([{ text: `📥 Download — ${o.game_name}`, url: `https://${host}/?dl=${o.download_token}` }]);
    } else if (o.status === 'downloaded') {
      keyboard.push([{ text: `🔄 New link — ${o.game_name}`, callback_data: `p_resend:${o.purchase_id}` }]);
    }
  }

  await tgReply(chatId, msg, keyboard.length ? { reply_markup: { inline_keyboard: keyboard } } : {});
}

// ─────────────────────────────────────────────────────────────
// Player — Browse & Buy via Telegram
// ─────────────────────────────────────────────────────────────

async function handleGames(chatId) {
  const { data: gameRows } = await db().from('games')
    .select('id, name, game_date, join_time, price_per_sheet, pricing_tiers, sheet_count, sold_count')
    .eq('status', 'listed')
    .order('created_at', { ascending: false })
    .limit(8);

  if (!gameRows?.length) {
    await tgReply(chatId, '📋 No games available right now. Check back soon!');
    return;
  }

  let msg = '🎮 *Available Games*\n\n';
  const keyboard = [];

  for (const g of gameRows) {
    const rem = Math.max(0, (g.sheet_count || 0) - (g.sold_count || 0));
    if (rem <= 0) continue;
    const price = Array.isArray(g.pricing_tiers) && g.pricing_tiers.length
      ? `₹${g.price_per_sheet}+` : `₹${g.price_per_sheet}/sheet`;
    msg += `🎯 *${g.name}*`;
    if (g.game_date) msg += ` — ${g.game_date}`;
    msg += `\n   ${price} · ${rem} sheets left\n\n`;
    keyboard.push([{ text: `🎟 Buy — ${g.name}`, callback_data: `p_buy:${g.id}` }]);
  }

  if (!keyboard.length) {
    await tgReply(chatId, '😔 All games are currently sold out. Check back soon!');
    return;
  }

  await tgReply(chatId, msg, { reply_markup: { inline_keyboard: keyboard } });
}

async function handleBuyGame(chatId, telegramId, gameId) {
  if (!gameId) { await handleGames(chatId); return; }

  const phone = await getPlayerPhone(telegramId);
  if (!phone) {
    await clearSession(telegramId);
    await setSession(telegramId, 'p_phone', { pendingGameId: gameId });
    await tgReply(chatId,
      `📱 *Send your phone number to get started*\n\nYour sheets will be delivered here automatically once your order is approved.\n\n_/cancel to start over_`
    );
    return;
  }

  const { data: gRow } = await db().from('games')
    .select('*').eq('id', gameId).eq('status', 'listed').single();
  if (!gRow) { await tgReply(chatId, '❌ Game not found or no longer available.'); return; }

  let operatorUpiId = null;
  if (gRow.operator_id) {
    const { data: opRow } = await db().from('operators').select('upi_id').eq('id', gRow.operator_id).single();
    operatorUpiId = opRow?.upi_id || null;
  }
  if (!operatorUpiId) {
    await tgReply(chatId, '❌ This game is not accepting payments yet. Try the website or contact the organiser.');
    return;
  }

  const rem = Math.max(0, (gRow.sheet_count || 0) - (gRow.sold_count || 0));
  if (rem <= 0) { await tgReply(chatId, '😔 *Sold out!* All sheets taken.\n\n/games to see other games.'); return; }

  const maxQty = Math.min(rem, 10);
  const priceLines = Array.isArray(gRow.pricing_tiers) && gRow.pricing_tiers.length
    ? gRow.pricing_tiers.map(t => `${t.qty} sheets → ₹${t.price}`).join('\n') + `\n1 sheet → ₹${gRow.price_per_sheet}`
    : `₹${gRow.price_per_sheet} per sheet`;

  await setSession(telegramId, 'p_buy_qty', {
    gameId: gRow.id, gameName: gRow.name,
    pricePerSheet: gRow.price_per_sheet, pricingTiers: gRow.pricing_tiers || [],
    operatorUpiId, sheetCount: gRow.sheet_count, soldCount: gRow.sold_count
  });

  await tgReply(chatId,
    `🎯 *${gRow.name}*\n` +
    (gRow.game_date ? `📅 ${gRow.game_date}${gRow.join_time ? ` · ⏰ ${gRow.join_time}` : ''}\n` : '') +
    `\n💰 *Pricing:*\n${priceLines}\n\n` +
    `📋 ${rem} sheets remaining\n\n` +
    `*How many sheets?* _(1–${maxQty})_\n\n_/cancel to start over_`,
    { reply_markup: { inline_keyboard: [[{ text: '❌ Cancel', callback_data: 'p_cancel' }]] } }
  );
}

// ─────────────────────────────────────────────────────────────
// /newgame wizard
// ─────────────────────────────────────────────────────────────

async function handleNewGame(chatId, telegramId) {
  const op = await getOpByTgId(telegramId);
  if (!op) { await tgReply(chatId, '❌ Not linked. Use `/link YOUR_API_KEY` first.'); return; }
  if (!op.upi_id) {
    await tgReply(chatId,
      `❌ *UPI not set up*\n\nPlayers pay you via UPI — you must add your UPI ID before listing games.\n\n` +
      `Open the *Operator Web App → Profile → UPI ID*, then try again.`
    );
    return;
  }
  await setSession(telegramId, 'w_name', { operatorId: op.id, operatorName: op.name });
  await tgReply(chatId, `🎯 *New Game* — ${op.name}\n\n*Game name?*`);
}

async function processWizard(chatId, telegramId, text, photoFileId, tgUser = null) {
  const session = await getSession(telegramId);
  if (!session) return false;

  const { step, data } = session;

  // ── Player phone linking ──────────────────────────────────────
  if (step === 'p_phone') {
    const phone = (text || '').replace(/\D/g, '');
    if (phone.length < 10) { await tgReply(chatId, '❌ Enter a valid 10-digit phone number.'); return true; }
    let { data: player } = await db().from('players').select('phone, name').eq('phone', phone).single();
    if (!player) {
      // New player — register inline from Telegram profile (no web signup required)
      const firstName = (tgUser?.first_name || '').trim();
      const lastName  = (tgUser?.last_name  || '').trim();
      const tgName    = (firstName + (lastName ? ' ' + lastName : '')) || tgUser?.username || `Player${phone.slice(-4)}`;
      const playerId  = 'tg_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
      const fakeHash  = crypto.randomBytes(16).toString('hex');
      await db().from('players').insert({ phone, id: playerId, name: tgName, password_hash: fakeHash });
      player = { name: tgName };
    }
    await db().from('player_telegram').upsert(
      { phone, telegram_id: String(telegramId) },
      { onConflict: 'phone' }
    );
    await clearSession(telegramId);
    await tgReply(chatId,
      `✅ *Welcome, ${player.name}!*\n\nYour account is linked — download links arrive here automatically when orders are approved.\n\n/games — Browse & buy sheets\n/myorders — Your orders`
    );
    if (data.pendingGameId) await handleBuyGame(chatId, telegramId, data.pendingGameId);
    return true;
  }

  // ── 1: name ───────────────────────────────────────────────────
  if (step === 'w_name') {
    const val = (text || '').trim();
    if (val.length < 2) { await tgReply(chatId, '❌ Name too short.'); return true; }
    await setSession(telegramId, 'w_date', { ...data, name: val });
    await tgReply(chatId, `📅 *Game date?*`, { reply_markup: dateKeyboard() });
    return true;
  }

  // ── 2: game date ──────────────────────────────────────────────
  if (step === 'w_date') {
    const val = (text || '').trim();
    await setSession(telegramId, 'w_jointime', { ...data, gameDate: val.toLowerCase() === 'skip' ? null : val });
    await tgReply(chatId, `⏰ *Join time?*`, { reply_markup: TIME_KB });
    return true;
  }

  // ── 3: join time ──────────────────────────────────────────────
  if (step === 'w_jointime') {
    const val = (text || '').trim();
    await setSession(telegramId, 'w_sheets', { ...data, joinTime: val.toLowerCase() === 'skip' ? null : val });
    await tgReply(chatId, `📋 *Sheet range?*\n_e.g. 1\\-500 or 301\\-600_`);
    return true;
  }

  // ── 4: sheet range ────────────────────────────────────────────
  if (step === 'w_sheets') {
    const m = (text || '').match(/^(\d+)\s*[-–]\s*(\d+)$/);
    if (!m) { await tgReply(chatId, '❌ Format: `1-500`. Try again.'); return true; }
    const from = parseInt(m[1]), to = parseInt(m[2]);
    if (from >= to)             { await tgReply(chatId, '❌ From must be less than to.'); return true; }
    if (to - from + 1 > 10000) { await tgReply(chatId, '❌ Max 10,000 sheets per game.'); return true; }
    await setSession(telegramId, 'w_price', { ...data, sheetFrom: from, sheetTo: to, sheetCount: to - from + 1 });
    await tgReply(chatId, `💰 *Price per sheet?*\n_e.g. 50_`);
    return true;
  }

  // ── 5: price ──────────────────────────────────────────────────
  if (step === 'w_price') {
    const price = parseInt((text || '').replace(/[₹,\s]/g, ''));
    if (!price || price < 1 || price > 100000) { await tgReply(chatId, '❌ Enter a number (e.g. `50`).'); return true; }
    await setSession(telegramId, 'w_tiers', { ...data, pricePerSheet: price });
    await tgReply(chatId, `📦 *Bulk deals?*\n_e.g. 5 200, 10 350_`, { reply_markup: SKIP_KB });
    return true;
  }

  // ── 6: tiers ──────────────────────────────────────────────────
  if (step === 'w_tiers') {
    let tiers = [];
    const val = (text || '').trim();
    if (val.toLowerCase() !== 'skip' && val !== '') {
      for (const part of val.split(',').map(s => s.trim()).filter(Boolean)) {
        const pm = part.match(/^(\d+)\s+(\d+)$/);
        if (!pm) { await tgReply(chatId, '❌ Format: `5 200, 10 350` or tap Skip.'); return true; }
        tiers.push({ qty: parseInt(pm[1]), price: parseInt(pm[2]) });
      }
    }
    await setSession(telegramId, 'w_prize_0', { ...data, pricingTiers: tiers, prizes: [] });
    await askPrize(chatId, 0);
    return true;
  }

  // ── 7.x: prizes ───────────────────────────────────────────────
  if (step.startsWith('w_prize_')) {
    const idx = parseInt(step.slice(8), 10);
    const val = (text || '').trim();
    const newData = { ...data };
    if (val.toLowerCase() !== 'skip') {
      const amount = parseInt(val.replace(/[₹,\s]/g, ''));
      if (!amount || amount < 1) { await tgReply(chatId, '❌ Enter an amount or tap Skip.'); return true; }
      newData.prizes = [...(data.prizes || []), { name: PRIZE_TYPES[idx], kind: 'cash', amount }];
    }
    const nextIdx = idx + 1;
    if (nextIdx < PRIZE_TYPES.length) {
      await setSession(telegramId, `w_prize_${nextIdx}`, newData);
      await askPrize(chatId, nextIdx);
    } else {
      await setSession(telegramId, 'w_description', newData);
      await tgReply(chatId, `📝 *Description?*`, { reply_markup: SKIP_KB });
    }
    return true;
  }

  // ── 8: description ────────────────────────────────────────────
  if (step === 'w_description') {
    const val = (text || '').trim();
    await setSession(telegramId, 'w_thumbnail', { ...data, description: val.toLowerCase() === 'skip' ? '' : val });
    await tgReply(chatId, `🖼 *Thumbnail?*\nSend a photo or paste a URL.`, { reply_markup: SKIP_KB });
    return true;
  }

  // ── 9: thumbnail ──────────────────────────────────────────────
  if (step === 'w_thumbnail') {
    let thumbnail = null;
    if (photoFileId) {
      const file = await tgGet(`getFile?file_id=${encodeURIComponent(photoFileId)}`);
      if (file?.result?.file_path)
        thumbnail = `https://api.telegram.org/file/bot${process.env.TELEGRAM_BOT_TOKEN}/${file.result.file_path}`;
    } else {
      const val = (text || '').trim();
      if (val && val.toLowerCase() !== 'skip' && (val.startsWith('http://') || val.startsWith('https://')))
        thumbnail = val;
    }
    await setSession(telegramId, 'w_schedule', { ...data, thumbnail });
    await tgReply(chatId, `⏱ *When to list?*\n_Goes live after your listing fee is verified._\nOr schedule: \`15 May 7:00 PM\``, { reply_markup: NOW_KB });
    return true;
  }

  // ── 10: schedule ──────────────────────────────────────────────
  if (step === 'w_schedule') {
    await applyScheduleInput(chatId, telegramId, (text || '').trim(), data);
    return true;
  }

  // ── Confirm ───────────────────────────────────────────────────
  if (step === 'w_confirm') {
    const val = (text || '').trim().toLowerCase();
    if      (val === 'yes') { await finishGame(chatId, telegramId, data); }
    else if (val === 'no')  { await clearSession(telegramId); await tgReply(chatId, '❌ Cancelled. /newgame to restart.'); }
    else                    { await tgReply(chatId, 'Tap a button above or reply *yes* / *no*.'); }
    return true;
  }

  // ── w_utr: operator submits listing-fee UTR ──────────────────
  if (step === 'w_utr') {
    const utrClean = (text || '').trim().toUpperCase().replace(/\s/g, '');
    if (utrClean.length < 6) { await tgReply(chatId, '❌ UTR must be at least 6 characters. Try again.'); return true; }

    const { gameId, amount, sheetCount, operatorId, operatorName, name } = data;
    const payId = 'pay_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);

    const { error } = await db().from('platform_payments').insert({
      id: payId, game_id: gameId, operator_id: operatorId,
      game_name: name, operator_name: operatorName,
      sheet_count: sheetCount, amount, utr: utrClean,
      status: 'pending', created_at: Date.now()
    });
    if (error) { await tgReply(chatId, `❌ Failed to submit: ${error.message}`); return true; }

    await clearSession(telegramId);

    const adminChat = process.env.TELEGRAM_ADMIN_CHAT_ID;
    if (adminChat) {
      await tgSend('sendMessage', {
        chat_id: adminChat,
        text: `💰 *New Listing Payment*\n\nOperator: *${operatorName}*\nGame: *${name}*\n📋 ${sheetCount} sheets · ₹${amount}\nUTR: \`${utrClean}\``,
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: '✅ Verify & List', callback_data: `verifypay:${payId}` }]] }
      });
    }

    await tgReply(chatId,
      `✅ *Payment submitted!*\n\nUTR: \`${utrClean}\`\n\nGame goes live once verified — you'll be notified here. 🎉\n\n/stats to check status.`
    );
    return true;
  }

  // ── p_buy_qty: player picks quantity ─────────────────────────
  if (step === 'p_buy_qty') {
    const qty = parseInt((text || '').trim());
    const rem = Math.max(0, (data.sheetCount || 0) - (data.soldCount || 0));
    const maxQty = Math.min(rem, 10);
    if (!qty || qty < 1) { await tgReply(chatId, `❌ Enter a number between 1 and ${maxQty}.`); return true; }
    if (qty > maxQty)    { await tgReply(chatId, `❌ Max ${maxQty} sheets per order.`); return true; }

    let amount = data.pricePerSheet * qty;
    if (Array.isArray(data.pricingTiers) && data.pricingTiers.length) {
      const tier = data.pricingTiers.find(t => t.qty === qty);
      if (tier) amount = tier.price;
    }

    await showPickScreen(chatId, telegramId, { ...data, quantity: qty, amount });
    return true;
  }

  // ── p_buy_pick: player picks lucky numbers or skips ──────────
  if (step === 'p_buy_pick') {
    let pickedNums = null;
    const val = (text || '').trim().toLowerCase();

    if (val !== 'skip') {
      const nums = (text || '').match(/\d+/g)?.map(Number).filter(n => n > 0) || [];
      if (!nums.length) { await tgReply(chatId, '❌ Type sheet number(s) or tap *Random*.'); return true; }
      if (nums.length > data.quantity) {
        await tgReply(chatId, `❌ You ordered *${data.quantity}* sheet${data.quantity > 1 ? 's' : ''} — pick at most ${data.quantity} number${data.quantity > 1 ? 's' : ''}.`);
        return true;
      }
      // Validate availability
      const { data: gRow } = await db().from('games')
        .select('sheet_from, sheet_to, sold_sheet_nums').eq('id', data.gameId).single();
      const soldSet = new Set((gRow?.sold_sheet_nums || []).map(Number));
      const bad = nums.filter(n => n < gRow.sheet_from || n > gRow.sheet_to || soldSet.has(n));
      if (bad.length) {
        await tgReply(chatId, `❌ Sheet${bad.length > 1 ? 's' : ''} *#${bad.join(', #')}* ${bad.length > 1 ? 'are' : 'is'} not available. Check the list and try again.`);
        return true;
      }
      pickedNums = nums;
    }

    await setSession(telegramId, 'p_buy_confirm', { ...data, pickedNums });
    await showBuyConfirmation(chatId, { ...data, pickedNums });
    return true;
  }

  // ── p_buy_utr: player submits payment transaction ID ─────────
  if (step === 'p_buy_utr') {
    const utr = (text || '').trim().replace(/\s/g, '');
    if (utr.length < 6) {
      await tgReply(chatId, '❌ That doesn\'t look right — enter the transaction/reference ID from your UPI app.\n\n_/cancel to start over_');
      return true;
    }
    await confirmBuyOrder(chatId, null, telegramId, { ...data, utr });
    return true;
  }

  // Catch-all: session exists but step not handled (stale/corrupt state)
  if (session) {
    await tgReply(chatId, '❓ Something went wrong. Type /cancel to start over.');
    return true;
  }

  return false;
}

async function askPrize(chatId, idx) {
  const e = PRIZE_EMOJI[PRIZE_TYPES[idx]] || '🎯';
  await tgReply(chatId,
    `${e} *${PRIZE_TYPES[idx]}* ₹?  _(${idx + 1}/${PRIZE_TYPES.length})_`,
    { reply_markup: SKIP_KB }
  );
}

async function applyScheduleInput(chatId, telegramId, val, data) {
  if (!val) { await tgReply(chatId, '❌ Type "now" or a date like `15 May 7:00 PM`.'); return; }
  let scheduledFor = null, publishNow = false;
  if (val.toLowerCase() === 'now') {
    publishNow = true;
  } else {
    const d = new Date(val);
    if (isNaN(d.getTime()))        { await tgReply(chatId, '❌ Can\'t parse that. Try `15 May 7:00 PM`.'); return; }
    if (d.getTime() <= Date.now()) { await tgReply(chatId, '❌ That time is in the past.'); return; }
    scheduledFor = d.getTime();
  }
  const newData = { ...data, scheduledFor, publishNow };
  await setSession(telegramId, 'w_confirm', newData);
  await showSummary(chatId, newData);
}

async function showSummary(chatId, data) {
  const priceStr = data.pricingTiers?.length
    ? data.pricingTiers.map(t => `${t.qty}×₹${t.price}`).join(' · ') + ` + ₹${data.pricePerSheet}`
    : `₹${data.pricePerSheet}`;

  const prizeLines = (data.prizes || [])
    .map(p => `${PRIZE_EMOJI[p.name] || '🎯'} ${p.name}: ₹${Number(p.amount).toLocaleString('en-IN')}`)
    .join('\n') || '  —';

  const when = data.publishNow ? '🚀 Immediately after payment verified' : `📅 ${new Date(data.scheduledFor).toLocaleString('en-IN')} (after payment)`;

  const lines = [
    `✅ *Confirm Game*\n`,
    `🎯 *${data.name}*`,
    data.gameDate ? `📅 ${data.gameDate}${data.joinTime ? ` · ⏰ ${data.joinTime}` : ''}` : null,
    `📋 Sheets ${data.sheetFrom}–${data.sheetTo} (${data.sheetCount})  💰 ${priceStr}`,
    data.description ? `📝 ${data.description}` : null,
    `\n*Prizes:*\n${prizeLines}`,
    `\n*Listing:* ${when}`
  ].filter(Boolean).join('\n');

  await tgReply(chatId, lines, {
    reply_markup: {
      inline_keyboard: [[
        { text: '✅ Create Game', callback_data: 'w_confirm:yes' },
        { text: '❌ Cancel',     callback_data: 'w_confirm:no'  }
      ]]
    }
  });
}

async function finishGame(chatId, telegramId, data) {
  const gameId = 'g_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);

  const { error } = await db().from('games').insert({
    id: gameId,
    operator_id: data.operatorId,
    operator_name: data.operatorName,
    name: data.name,
    game_date: data.gameDate || null,
    game_date_raw: data.gameDate || null,
    join_time: data.joinTime || null,
    price_per_sheet: data.pricePerSheet,
    pricing_tiers: data.pricingTiers || [],
    description: data.description || '',
    prizes: data.prizes || [],
    thumbnail: data.thumbnail || null,
    status: 'draft',
    sheet_from: data.sheetFrom,
    sheet_to: data.sheetTo,
    sheet_count: data.sheetCount,
    sold_count: 0,
    sold_sheet_nums: [],
    scheduled_for: data.scheduledFor || null,
    created_at: Date.now()
  });

  if (error) { await tgReply(chatId, `❌ Failed: ${error.message}`); return; }

  const amount = Math.round(data.sheetCount * 1.99 * 100) / 100;
  const { data: cfgRow } = await db().from('config').select('value').eq('key', 'settings').single();
  const adminUpiId = cfgRow?.value?.upiId || '';

  await setSession(telegramId, 'w_utr', { ...data, gameId, amount });

  await tgReply(chatId,
    `✅ *Game Saved: ${data.name}*\n\n` +
    `━━━━━━━━━━━━\n` +
    `💰 *Listing Fee*\n` +
    `${data.sheetCount} sheets × ₹1.99 = *₹${amount}*\n\n` +
    `Pay to UPI:\n\`${adminUpiId || 'Contact admin for UPI ID'}\`\n` +
    `━━━━━━━━━━━━\n\n` +
    `After payment reply with your *UTR / transaction ID* to activate the game.`
  );
}

// ─────────────────────────────────────────────────────────────
// Player buy — screens + order creation
// ─────────────────────────────────────────────────────────────

async function showPickScreen(chatId, telegramId, data) {
  const { data: gRow } = await db().from('games')
    .select('sheet_from, sheet_to, sold_sheet_nums').eq('id', data.gameId).single();
  const soldSet = new Set((gRow?.sold_sheet_nums || []).map(Number));
  const from = gRow?.sheet_from || 1, to = gRow?.sheet_to || 1;
  const available = [];
  for (let n = from; n <= to; n++) if (!soldSet.has(n)) available.push(n);

  const shown = available.slice(0, 120);
  const rows = [];
  for (let i = 0; i < shown.length; i += 15)
    rows.push(shown.slice(i, i + 15).map(n => String(n).padStart(3, ' ')).join(' '));
  const numBlock = rows.join('\n');
  const extraNote = available.length > 120 ? `\n_...and ${available.length - 120} more in range ${from}–${to}_` : '';

  const qty = data.quantity;
  await setSession(telegramId, 'p_buy_pick', data);
  await tgReply(chatId,
    `📋 *Available sheets (${available.length} remaining):*\n\`\`\`\n${numBlock}\`\`\`${extraNote}\n\n` +
    `Type your lucky number${qty > 1 ? `s (up to ${qty}, e.g. \`7 23\`)` : ` (e.g. \`7\`)`} from the list.\n` +
    `Or tap below for random assignment.\n\n_/cancel to start over_`,
    { reply_markup: { inline_keyboard: [[{ text: '🎲 Random — surprise me!', callback_data: 'w_skip' }]] } }
  );
}

// Screen 1 — order review (edit before paying)
async function showBuyConfirmation(chatId, data) {
  const pickStr = data.pickedNums?.length
    ? `🎫 Your picks: *#${data.pickedNums.join(', #')}*`
    : `🎲 *Random assignment*`;

  await tgReply(chatId,
    `✅ *Review Your Order*\n\n` +
    `🎯 ${data.gameName}\n` +
    `📋 ${data.quantity} sheet${data.quantity > 1 ? 's' : ''} · *₹${data.amount}*\n` +
    `${pickStr}\n\n` +
    `Everything look right?\n\n_/cancel to start over_`,
    {
      reply_markup: {
        inline_keyboard: [
          [{ text: '✅ Looks good — Pay now', callback_data: 'p_confirm' }],
          [
            { text: '✏️ Change picks', callback_data: 'p_edit_picks' },
            { text: '❌ Cancel',        callback_data: 'p_cancel'     }
          ]
        ]
      }
    }
  );
}

// Screen 2 — payment (shown after player confirms order details)
async function showPaymentScreen(chatId, msgId, telegramId, data) {
  const upiLink =
    `upi://pay?pa=${encodeURIComponent(data.operatorUpiId)}` +
    `&pn=${encodeURIComponent(data.operatorName || 'Operator')}` +
    `&am=${data.amount}` +
    `&tn=${encodeURIComponent(data.gameName + ' Sheets')}` +
    `&cu=INR`;

  await setSession(telegramId, 'p_buy_pay', data);

  await editMessage(chatId, msgId,
    `💰 *Pay ₹${data.amount}*\n\n` +
    `Tap the button below — your UPI app opens with the amount pre-filled.\n\n` +
    `Once you've paid, tap *I've Paid* to submit your order to the organiser.`,
    {
      reply_markup: {
        inline_keyboard: [
          [{ text: `💳 Pay ₹${data.amount} via UPI`, url: upiLink }],
          [
            { text: "✅ I've Paid — Submit Order", callback_data: 'p_paid' },
            { text: '❌ Cancel',                   callback_data: 'p_cancel' }
          ]
        ]
      }
    }
  );
}

async function sendTelegramOrderNotification(purchase, game) {
  const { data: opRow } = await db().from('operators')
    .select('telegram_chat_id, telegram_id').eq('id', game.operatorId).single();
  const opChatId = opRow?.telegram_chat_id || opRow?.telegram_id;
  if (!opChatId) return;

  const pickNote = purchase.requestedSheetNums?.length
    ? `🎫 Requested: #${purchase.requestedSheetNums.join(', #')}`
    : '🎲 Random assignment';
  const utrNote = purchase.utr ? `\n💳 UTR: \`${purchase.utr}\`` : '';

  await tgSend('sendMessage', {
    chat_id: opChatId,
    text:
      `🛒 *New Order!*\n\n` +
      `👤 ${purchase.playerName} · 📞 ${purchase.phone}\n` +
      `🎮 ${purchase.gameName}\n` +
      `📋 ${purchase.quantity} sheet${purchase.quantity > 1 ? 's' : ''} · ₹${purchase.amount}\n` +
      `${pickNote}${utrNote}\n\n` +
      `🆔 Order: \`${purchase.purchaseId}\``,
    parse_mode: 'Markdown',
    reply_markup: {
      inline_keyboard: [[
        { text: '✅ Approve', callback_data: `approve:${purchase.purchaseId}` },
        { text: '❌ Reject',  callback_data: `reject:${purchase.purchaseId}` }
      ]]
    }
  });
}

async function confirmBuyOrder(chatId, msgId, telegramId, data) {
  const phone = await getPlayerPhone(telegramId);
  if (!phone) {
    await clearSession(telegramId);
    await tgReply(chatId, '❌ Session expired. Use /games to start over.');
    return;
  }

  const { data: playerRow } = await db().from('players').select('name').eq('phone', phone).single();
  if (!playerRow) { await clearSession(telegramId); await tgReply(chatId, '❌ Account not found. Use /start to re-link.'); return; }

  const { data: gRow } = await db().from('games')
    .select('sold_count, sheet_count, operator_id, operator_name')
    .eq('id', data.gameId).eq('status', 'listed').single();
  if (!gRow) { await clearSession(telegramId); await tgReply(chatId, '❌ Game no longer available.'); return; }
  if (Math.max(0, gRow.sheet_count - gRow.sold_count) < data.quantity) {
    await clearSession(telegramId);
    await tgReply(chatId, '❌ Not enough sheets left. Use /games to check availability.');
    return;
  }

  const purchaseId = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  const { error } = await db().from('purchases').insert({
    purchase_id: purchaseId, player_name: playerRow.name, phone,
    game_id: data.gameId, game_name: data.gameName,
    quantity: data.quantity, amount: data.amount,
    requested_sheet_nums: data.pickedNums || null, status: 'pending',
    download_token: null, sheet_nums: null, created_at: Date.now(),
    utr: data.utr || null
  });
  if (error) { await tgReply(chatId, `❌ Order failed: ${error.message}`); return; }

  await clearSession(telegramId);

  const successText =
    `✅ *Order Submitted!*\n\n` +
    `🎮 ${data.gameName}\n` +
    `📋 ${data.quantity} sheet${data.quantity > 1 ? 's' : ''} · ₹${data.amount}\n` +
    `${data.pickedNums?.length ? `🎫 Requested: #${data.pickedNums.join(', #')}` : '🎲 Random assignment'}\n` +
    (data.utr ? `💳 UTR: \`${data.utr}\`\n` : '') +
    `\n⏳ Awaiting approval. Your sheets will arrive here once approved.\n\n/myorders to track.`;

  if (msgId) {
    await editMessage(chatId, msgId, successText);
  } else {
    await tgReply(chatId, successText);
  }

  try {
    await sendTelegramOrderNotification(
      { purchaseId, playerName: playerRow.name, phone, gameName: data.gameName,
        quantity: data.quantity, amount: data.amount,
        requestedSheetNums: data.pickedNums || null, createdAt: Date.now(),
        utr: data.utr || null },
      { id: data.gameId, operatorId: gRow.operator_id, operatorName: gRow.operator_name }
    );
  } catch(e) {}
}

// ─────────────────────────────────────────────────────────────
// Channel broadcast — shared with api/cron.js
// ─────────────────────────────────────────────────────────────

async function broadcastGame(channelId, game) {
  const host       = process.env.APP_HOST || 'tungbola-market.vercel.app';
  const botUsername = process.env.TELEGRAM_BOT_USERNAME;
  const url        = `https://${host}/g/${game.id}`;

  const priceStr = Array.isArray(game.pricingTiers) && game.pricingTiers.length
    ? game.pricingTiers.map(t => `${t.qty}×₹${t.price}`).join(' · ') + ` · Single ₹${game.pricePerSheet}`
    : `₹${game.pricePerSheet} per sheet`;

  const prizeLines = (game.prizes || []).slice(0, 5)
    .map(p => `${PRIZE_EMOJI[p.name] || '•'} ${p.name}: ₹${Number(p.amount).toLocaleString('en-IN')}`)
    .join('\n');

  let msg = `🎯 *${game.name}*`;
  if (game.gameDate) msg += `\n📅 ${game.gameDate}`;
  if (game.joinTime) msg += ` · ⏰ ${game.joinTime}`;
  msg += `\n\n💰 ${priceStr}`;
  if (prizeLines) msg += `\n\n🏆 *Prizes:*\n${prizeLines}`;
  msg += `\n\n📋 ${game.sheetCount} sheets available`;

  const keyboard = [[{ text: '🌐 Book on Website', url }]];
  if (botUsername) keyboard.push([{ text: '🍀 Buy your lucky sheets', url: `https://t.me/${botUsername}?start=buy_${game.id}` }]);
  const replyMarkup = { inline_keyboard: keyboard };

  if (game.thumbnail) {
    await tgSend('sendPhoto', {
      chat_id: channelId, photo: game.thumbnail,
      caption: msg, parse_mode: 'Markdown', reply_markup: replyMarkup
    });
  } else {
    await tgSend('sendMessage', {
      chat_id: channelId, text: msg, parse_mode: 'Markdown', reply_markup: replyMarkup
    });
  }
}

// ─────────────────────────────────────────────────────────────
// Approve / Reject
// ─────────────────────────────────────────────────────────────

async function handleVerifyPay(paymentId, chatId, messageId, callbackQueryId, senderTgId) {
  // Only admin can verify (check sender against TELEGRAM_ADMIN_CHAT_ID)
  const adminChatId = String(process.env.TELEGRAM_ADMIN_CHAT_ID || '');
  if (adminChatId && String(chatId) !== adminChatId) {
    await answerCallback(callbackQueryId, 'Not authorized.'); return;
  }

  const { data: payRow } = await db().from('platform_payments').select('*').eq('id', paymentId).single();
  if (!payRow) { await answerCallback(callbackQueryId, 'Payment not found.'); return; }
  if (payRow.status !== 'pending') {
    await answerCallback(callbackQueryId, `Already ${payRow.status}.`);
    await editMessage(chatId, messageId, `✅ *Listing Payment*\nGame: ${payRow.game_name}\nStatus: already ${payRow.status}`);
    return;
  }

  const now = Date.now();
  await Promise.all([
    db().from('platform_payments').update({ status: 'verified', verified_at: now }).eq('id', paymentId),
    db().from('games').update({ status: 'listed' }).eq('id', payRow.game_id)
  ]);

  await answerCallback(callbackQueryId, '✅ Payment verified — game is now live!');
  await editMessage(chatId, messageId,
    `✅ *Payment Verified*\n\nOperator: ${payRow.operator_name}\nGame: *${payRow.game_name}*\n📋 ${payRow.sheet_count} sheets · ₹${payRow.amount}\nUTR: \`${payRow.utr}\`\n\n_Game is now live on marketplace._`
  );

  // Notify operator
  const { data: opRow } = await db().from('operators')
    .select('telegram_chat_id, telegram_id, player_channel_id').eq('id', payRow.operator_id).single();
  const opChatId = opRow?.telegram_chat_id || opRow?.telegram_id;
  if (opChatId) {
    const host = process.env.APP_HOST || 'tungbola-market.vercel.app';
    await tgSend('sendMessage', {
      chat_id: opChatId,
      text: `✅ *Payment Verified!*\n\n🎮 *${payRow.game_name}* is now live on TungbolaMarket!\n\n📋 ${payRow.sheet_count} sheets · ₹${payRow.amount}\n\nPlayers can start booking now! 🎉`,
      parse_mode: 'Markdown',
      reply_markup: { inline_keyboard: [[{ text: '🎮 View Game', url: `https://${host}/g/${payRow.game_id}` }]] }
    });
  }
  // Broadcast to player channel with thumbnail
  try {
    const { data: gRow } = await db().from('games').select('*').eq('id', payRow.game_id).single();
    if (opRow?.player_channel_id && gRow) await broadcastGame(opRow.player_channel_id, gameFromRow(gRow));
  } catch(e) { console.error('Broadcast on verify failed:', e.message); }
}

async function handleApprove(purchaseId, chatId, messageId, callbackQueryId) {
  const { data: pRow } = await db().from('purchases').select('*').eq('purchase_id', purchaseId).single();
  if (!pRow) { await answerCallback(callbackQueryId, 'Order not found.'); return; }
  if (pRow.status !== 'pending') {
    await answerCallback(callbackQueryId, `Already ${pRow.status}.`);
    await editMessage(chatId, messageId, `✅ *Order #${purchaseId.slice(-6)}*\nStatus: already ${pRow.status}`);
    return;
  }

  const purchase = purchaseFromRow(pRow);
  const { data: gRow } = await db().from('games').select('*').eq('id', purchase.gameId).single();
  if (!gRow) { await answerCallback(callbackQueryId, 'Game not found.'); return; }

  // Verify the caller owns this game
  if (gRow.operator_id) {
    const { data: opRow } = await db().from('operators').select('telegram_chat_id').eq('id', gRow.operator_id).single();
    if (opRow?.telegram_chat_id && String(opRow.telegram_chat_id) !== String(chatId)) {
      await answerCallback(callbackQueryId, '⛔ Not your order.'); return;
    }
  } else {
    const adminChat = process.env.TELEGRAM_ADMIN_CHAT_ID;
    if (adminChat && String(adminChat) !== String(chatId)) {
      await answerCallback(callbackQueryId, '⛔ Not authorized.'); return;
    }
  }
  const game = gameFromRow(gRow);

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
  const soldSet   = new Set(game.soldSheetNums);
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

  const sheetList   = assigned.map(s => ({ n: s.n, filename: s.f, url: s.u }));
  const dlToken     = Date.now().toString(36).toUpperCase() + Math.random().toString(36).slice(2, 9).toUpperCase();
  const now         = Date.now();
  const newSoldNums = [...game.soldSheetNums, ...assigned.map(s => s.n)];

  // Atomic: only update game if sold_count hasn't changed since we read it (prevents race on concurrent approvals)
  const { data: lockCheck } = await db().from('games')
    .update({ sold_sheet_nums: newSoldNums, sold_count: newSoldNums.length })
    .eq('id', game.id).eq('sold_count', game.soldCount).select('id');
  if (!lockCheck?.length) {
    await answerCallback(callbackQueryId, '⚠️ Conflict — another order was just approved. Please try again.');
    return;
  }

  await Promise.all([
    db().from('download_tokens').insert({ token: dlToken, sheets: sheetList, game_name: game.name, purchase_id: purchaseId }),
    db().from('purchases').update({ status: 'approved', download_token: dlToken, approved_at: now, sheet_nums: assigned.map(s => s.n) }).eq('purchase_id', purchaseId)
  ]);

  // Push notification (web)
  try {
    const np = String(purchase.phone).replace(/\D/g, '');
    const { data: pushRow } = await db().from('push_subscriptions').select('subscription').eq('phone', np).single();
    if (pushRow?.subscription) await sendPush(pushRow.subscription);
  } catch (e) {}

  // Telegram notification (if player linked)
  await notifyPlayerApproved(
    String(purchase.phone).replace(/\D/g, ''),
    purchase.gameName, purchase.quantity, purchase.amount, dlToken
  );

  await answerCallback(callbackQueryId, '✅ Approved!');
  await editMessage(chatId, messageId,
    `✅ *Approved* — #${purchaseId.slice(-6)}\n\n👤 ${purchase.playerName}\n🎮 ${purchase.gameName}\n📋 ${purchase.quantity} sheets · ₹${purchase.amount}`
  );
}

async function handleReject(purchaseId, chatId, messageId, callbackQueryId) {
  const { data: pRow } = await db().from('purchases')
    .select('status, phone, player_name, game_name, quantity, amount, game_id')
    .eq('purchase_id', purchaseId).single();
  if (!pRow) { await answerCallback(callbackQueryId, 'Order not found.'); return; }
  if (pRow.status !== 'pending') { await answerCallback(callbackQueryId, `Already ${pRow.status}.`); return; }

  // Verify the caller owns this game
  const { data: gRow } = await db().from('games').select('operator_id').eq('id', pRow.game_id).single();
  if (gRow?.operator_id) {
    const { data: opRow } = await db().from('operators').select('telegram_chat_id').eq('id', gRow.operator_id).single();
    if (opRow?.telegram_chat_id && String(opRow.telegram_chat_id) !== String(chatId)) {
      await answerCallback(callbackQueryId, '⛔ Not your order.'); return;
    }
  } else {
    const adminChat = process.env.TELEGRAM_ADMIN_CHAT_ID;
    if (adminChat && String(adminChat) !== String(chatId)) {
      await answerCallback(callbackQueryId, '⛔ Not authorized.'); return;
    }
  }

  await db().from('purchases').update({ status: 'rejected' }).eq('purchase_id', purchaseId);

  // Telegram notification (if player linked)
  await notifyPlayerRejected(
    String(pRow.phone || '').replace(/\D/g, ''),
    pRow.game_name, pRow.quantity, pRow.amount
  );

  await answerCallback(callbackQueryId, '❌ Rejected.');
  await editMessage(chatId, messageId,
    `❌ *Rejected* — #${purchaseId.slice(-6)}\n\n👤 ${pRow.player_name}\n🎮 ${pRow.game_name}\n📋 ${pRow.quantity} sheets · ₹${pRow.amount}`
  );
}

// ─────────────────────────────────────────────────────────────
// Main webhook handler
// ─────────────────────────────────────────────────────────────

module.exports = async function(req, res) {
  secureHeaders(res);
  if (req.method !== 'POST') return res.status(405).end();

  const secret = process.env.TELEGRAM_WEBHOOK_SECRET;
  if (secret) {
    const incoming = req.headers['x-telegram-bot-api-secret-token'];
    if (!incoming || !crypto.timingSafeEqual(Buffer.from(incoming), Buffer.from(secret)))
      return res.status(401).end();
  }

  try {
    const update = req.body || {};

    // ── Callback queries ──────────────────────────────────────
    if (update.callback_query) {
      const cb     = update.callback_query;
      const cbData = cb.data || '';
      const chatId = cb.message?.chat?.id;
      const msgId  = cb.message?.message_id;
      const tgId   = cb.from?.id;
      const cbqId  = cb.id;
      if (!chatId) return res.status(200).json({ ok: true });

      if (cbData.startsWith('verifypay:')) {
        await handleVerifyPay(cbData.slice(10), chatId, msgId, cbqId, String(tgId || ''));
      } else if (cbData.startsWith('approve:')) {
        await handleApprove(cbData.slice(8), chatId, msgId, cbqId);
      } else if (cbData.startsWith('reject:')) {
        await handleReject(cbData.slice(7), chatId, msgId, cbqId);
      } else if (cbData.startsWith('w_date:')) {
        await answerCallback(cbqId, '');
        await processWizard(chatId, tgId, cbData.slice(7), null);
      } else if (cbData.startsWith('w_time:')) {
        await answerCallback(cbqId, '');
        await processWizard(chatId, tgId, cbData.slice(7), null);
      } else if (cbData === 'w_skip') {
        await answerCallback(cbqId, '');
        await processWizard(chatId, tgId, 'skip', null);
      } else if (cbData.startsWith('w_sched:')) {
        const session = await getSession(tgId);
        if (session?.step === 'w_schedule') {
          await answerCallback(cbqId, '');
          await applyScheduleInput(chatId, tgId, cbData.slice(8), session.data);
        } else {
          await answerCallback(cbqId, 'Session expired — use /newgame.');
        }
      } else if (cbData.startsWith('w_confirm:')) {
        const session = await getSession(tgId);
        await answerCallback(cbqId, '');
        if (!session) {
          await tgReply(chatId, 'Session expired — use /newgame.');
        } else if (cbData === 'w_confirm:yes') {
          await finishGame(chatId, tgId, session.data);
        } else {
          await clearSession(tgId);
          await tgReply(chatId, '❌ Cancelled. /newgame to restart.');
        }
      } else if (cbData === 'p_confirm') {
        const session = await getSession(tgId);
        if (!session || session.step !== 'p_buy_confirm') {
          await answerCallback(cbqId, 'Session expired — use /games to start over.');
        } else {
          await answerCallback(cbqId, '');
          await showPaymentScreen(chatId, msgId, tgId, session.data);
        }
      } else if (cbData === 'p_paid') {
        const session = await getSession(tgId);
        if (!session || session.step !== 'p_buy_pay') {
          await answerCallback(cbqId, 'Session expired — use /games to start over.');
        } else {
          await answerCallback(cbqId, '');
          await setSession(tgId, 'p_buy_utr', session.data);
          await tgReply(chatId,
            `🧾 *Enter your transaction ID (UTR)*\n\n` +
            `Open your UPI app — look for the *transaction/reference ID* \\(usually 12 digits\\)\\.\n\n` +
            `Example: \`123456789012\`\n\n_/cancel to start over_`
          );
        }
      } else if (cbData === 'p_edit_picks') {
        const session = await getSession(tgId);
        if (!session || session.step !== 'p_buy_confirm') {
          await answerCallback(cbqId, 'Session expired — use /games to start over.');
        } else {
          await answerCallback(cbqId, '');
          await showPickScreen(chatId, tgId, session.data);
        }
      } else if (cbData.startsWith('p_buy:')) {
        await answerCallback(cbqId, '');
        await handleBuyGame(chatId, tgId, cbData.slice(6));
      } else if (cbData === 'p_cancel') {
        await clearSession(tgId);
        await answerCallback(cbqId, 'Cancelled.');
        await editMessage(chatId, msgId, '❌ Order cancelled. /games to browse again.');
      } else if (cbData.startsWith('p_resend:')) {
        const purchaseId = cbData.slice(9);
        await answerCallback(cbqId, '⏳ Generating new link...');
        const phone = await getPlayerPhone(tgId);
        if (!phone) { await tgReply(chatId, '❌ Account not linked. Use /start.'); }
        else {
          const { data: pRow } = await db().from('purchases')
            .select('status, game_name, quantity, amount').eq('purchase_id', purchaseId).eq('phone', phone).single();
          if (!pRow || !['approved', 'downloaded'].includes(pRow.status)) {
            await tgReply(chatId, '❌ Order not found or not yet approved.');
          } else {
            const { data: tokenRows } = await db().from('download_tokens')
              .select('sheets, game_name').eq('purchase_id', purchaseId)
              .order('expires_at', { ascending: false }).limit(1);
            if (!tokenRows?.length) {
              await tgReply(chatId, '❌ Sheet data not found. Please contact the organiser.');
            } else {
              const newToken = Date.now().toString(36).toUpperCase() + Math.random().toString(36).slice(2, 9).toUpperCase();
              await db().from('download_tokens').insert({ token: newToken, sheets: tokenRows[0].sheets, game_name: tokenRows[0].game_name, purchase_id: purchaseId });
              await db().from('purchases').update({ download_token: newToken, downloaded: false, downloaded_at: null, status: 'approved' }).eq('purchase_id', purchaseId);
              const host = process.env.APP_HOST || 'tungbola-market.vercel.app';
              await tgSend('sendMessage', {
                chat_id: chatId,
                text: `✅ *New download link ready!*\n\n🎮 ${pRow.game_name}\n\n_Link expires in 6 hours._`,
                parse_mode: 'Markdown',
                reply_markup: { inline_keyboard: [[{ text: '📥 Download Sheets', url: `https://${host}/?dl=${newToken}` }]] }
              });
            }
          }
        }
      } else {
        await answerCallback(cbqId, '');
      }

      return res.status(200).json({ ok: true });
    }

    // ── Messages ──────────────────────────────────────────────
    if (update.message) {
      const msg    = update.message;
      const chatId = msg.chat?.id;
      const tgId   = msg.from?.id;
      if (!chatId || !tgId) return res.status(200).json({ ok: true });
      if (msg.chat?.type !== 'private') return res.status(200).json({ ok: true });

      const text        = msg.text || '';
      const photoFileId = msg.photo?.length ? msg.photo[msg.photo.length - 1].file_id : null;

      const cmdMatch = text.match(/^\/(\w+)(?:@\S+)?\s*([\s\S]*)?$/);
      if (cmdMatch) {
        const cmd  = cmdMatch[1].toLowerCase();
        const args = (cmdMatch[2] || '').trim();

        if (cmd === 'start') {
          const op = await getOpByTgId(tgId);
          if (op) {
            await handleOperatorHelp(chatId);
          } else if (args && args.startsWith('buy_')) {
            await handleBuyGame(chatId, tgId, args.slice(4));
          } else {
            await handlePlayerStart(chatId, tgId);
          }
        } else if (cmd === 'help') {
          const op = await getOpByTgId(tgId);
          if (op) await handleOperatorHelp(chatId);
          else    await tgReply(chatId, `/games — browse & buy sheets\n/myorders — your orders`);
        } else if (cmd === 'myorders') {
          await handleMyOrders(chatId, tgId, args);
        } else if (cmd === 'link') {
          await handleLink(chatId, tgId, args);
        } else if (cmd === 'newgame') {
          await handleNewGame(chatId, tgId);
        } else if (cmd === 'stats') {
          await handleStats(chatId, tgId);
        } else if (cmd === 'setchannel') {
          await handleSetChannel(chatId, tgId, args);
        } else if (cmd === 'cancel') {
          await handleCancel(chatId, tgId);
        } else if (cmd === 'games' || cmd === 'shop') {
          await handleGames(chatId);
        } else if (cmd === 'buy') {
          await handleBuyGame(chatId, tgId, args.split(' ')[0]);
        }
        return res.status(200).json({ ok: true });
      }

      const consumed = await processWizard(chatId, tgId, text, photoFileId, msg.from);
      if (!consumed && text) await tgReply(chatId, 'Use /help to see commands.');
    }
  } catch (e) {
    console.error('Telegram webhook error:', e.message);
  }

  return res.status(200).json({ ok: true });
};

module.exports.broadcastGame        = broadcastGame;
module.exports.tgSend               = tgSend;
module.exports.notifyPlayerApproved = notifyPlayerApproved;
module.exports.notifyPlayerRejected = notifyPlayerRejected;
