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
// Operator / session helpers
// ─────────────────────────────────────────────────────────────

async function getOpByTgId(telegramId) {
  const { data } = await db().from('operators').select('*').eq('telegram_id', String(telegramId)).single();
  return data || null;
}

async function getSession(telegramId) {
  const { data } = await db().from('bot_sessions').select('*').eq('telegram_id', String(telegramId)).single();
  return data || null;
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
// Prize types (all standard tambola prizes)
// ─────────────────────────────────────────────────────────────

const PRIZE_TYPES = [
  'Full House', 'Second Full House', 'Third Full House',
  'Upper Line', 'Middle Line', 'Bottom Line',
  'Ticket Corners', 'Sheet Corner',
  'Early 5', 'Early 6', 'Early 7', 'Jackpot'
];

// ─────────────────────────────────────────────────────────────
// Commands
// ─────────────────────────────────────────────────────────────

async function handleHelp(chatId) {
  await tgReply(chatId,
    `*Tungbola Operator Bot* 🎯\n\n` +
    `/link APIKEY — Link your operator account\n` +
    `/newgame — Create & schedule a new game\n` +
    `/stats — Sheets remaining, pending orders, today's revenue\n` +
    `/setchannel @channel — Set your player broadcast channel\n` +
    `/cancel — Cancel current operation\n` +
    `/help — Show this message`
  );
}

async function handleLink(chatId, telegramId, args) {
  const apiKey = (args || '').trim();
  if (!apiKey) {
    await tgReply(chatId,
      `*Link your operator account*\n\nUsage: \`/link YOUR_API_KEY\`\n\nFind your API key in the operator dashboard.`
    );
    return;
  }
  const { data: opRow } = await db().from('operators').select('*').eq('api_key', apiKey).single();
  if (!opRow) {
    await tgReply(chatId, '❌ API key not found. Check your operator dashboard.');
    return;
  }
  // Set telegram_id for bot auth; also seed telegram_chat_id for notifications if not already set
  const updates = { telegram_id: String(telegramId) };
  if (!opRow.telegram_chat_id) updates.telegram_chat_id = String(chatId);
  await db().from('operators').update(updates).eq('id', opRow.id);
  await tgReply(chatId,
    `✅ *Linked! Welcome, ${opRow.name}.*\n\n` +
    `Your Telegram account is connected to your operator profile.\n\n` +
    `/newgame — Create a game\n` +
    `/stats — View live stats\n` +
    `/setchannel @channel — Set your player channel\n` +
    `/help — All commands`
  );
}

async function handleSetChannel(chatId, telegramId, args) {
  const op = await getOpByTgId(telegramId);
  if (!op) { await tgReply(chatId, '❌ Not linked. Use `/link YOUR_API_KEY` first.'); return; }

  const channelId = (args || '').trim();
  if (!channelId) {
    await tgReply(chatId,
      `*Set your player broadcast channel*\n\n` +
      `1. Add this bot as an *Admin* to your channel\n   (needs "Post Messages" permission)\n` +
      `2. Run: \`/setchannel @your_channel_username\`\n\n` +
      `New games will be posted to that channel automatically when published.`
    );
    return;
  }

  const result = await tgSendRead('sendMessage', {
    chat_id: channelId,
    text: '✅ Channel connected to Tungbola Market bot. New games will be broadcast here.'
  });
  if (!result?.ok) {
    await tgReply(chatId,
      `❌ Couldn't post to \`${channelId}\`.\n\n` +
      `Make sure:\n• Bot is added as Admin to the channel\n• You have "Post Messages" permission\n• Channel username is correct (e.g., @mychannel)`
    );
    return;
  }
  await db().from('operators').update({ player_channel_id: channelId }).eq('id', op.id);
  await tgReply(chatId, `✅ Player channel set to \`${channelId}\`. New games will be broadcast there automatically.`);
}

async function handleStats(chatId, telegramId) {
  const op = await getOpByTgId(telegramId);
  if (!op) { await tgReply(chatId, '❌ Not linked. Use `/link YOUR_API_KEY` first.'); return; }

  const { data: gRows } = await db().from('games')
    .select('id, name, sheet_count, sheet_from, sheet_to, sold_count')
    .eq('operator_id', op.id)
    .eq('status', 'listed');

  if (!gRows?.length) {
    await tgReply(chatId, `📊 *Stats — ${op.name}*\n\nNo active games right now.\n\n/newgame to create one.`);
    return;
  }

  const gameIds = gRows.map(g => g.id);

  let totalRemaining = 0;
  const gameLines = gRows.map(g => {
    const total = g.sheet_count || (g.sheet_to - g.sheet_from + 1);
    const rem = total - (g.sold_count || 0);
    totalRemaining += rem;
    return `  • ${g.name}: *${rem}* / ${total} left`;
  }).join('\n');

  const { count: pendingCount } = await db().from('purchases')
    .select('*', { count: 'exact', head: true })
    .in('game_id', gameIds)
    .eq('status', 'pending');

  const todayMs = new Date().setHours(0, 0, 0, 0);
  const { data: todaySales } = await db().from('purchases')
    .select('quantity, amount')
    .in('game_id', gameIds)
    .in('status', ['approved', 'downloaded'])
    .gte('created_at', todayMs);

  const todaySold = (todaySales || []).reduce((s, p) => s + (p.quantity || 0), 0);
  const todayRevenue = (todaySales || []).reduce((s, p) => s + (p.amount || 0), 0);

  let msg = `📊 *Stats — ${op.name}*\n\n`;
  msg += `📋 *Remaining Sheets: ${totalRemaining}*\n${gameLines}\n\n`;
  msg += `⏳ *Pending Approvals: ${pendingCount || 0}*\n\n`;
  msg += `📈 *Today: ${todaySold} sold · ₹${todayRevenue.toLocaleString('en-IN')} revenue*`;
  await tgReply(chatId, msg);
}

async function handleCancel(chatId, telegramId) {
  const session = await getSession(telegramId);
  await clearSession(telegramId);
  if (session) {
    await tgReply(chatId, '✅ Cancelled.\n\n/newgame /stats /help');
  } else {
    await tgReply(chatId, 'Nothing active to cancel.\n\n/help for commands.');
  }
}

// ─────────────────────────────────────────────────────────────
// /newgame wizard
// ─────────────────────────────────────────────────────────────

async function handleNewGame(chatId, telegramId) {
  const op = await getOpByTgId(telegramId);
  if (!op) { await tgReply(chatId, '❌ Not linked. Use `/link YOUR_API_KEY` first.'); return; }
  await setSession(telegramId, 'w_name', { operatorId: op.id, operatorName: op.name });
  await tgReply(chatId,
    `🎯 *New Game Setup* (${op.name})\n\nI'll collect all game details step by step.\nType /cancel anytime to abort.\n\n` +
    `*Step 1 of 10 — Game Name*\nWhat's the name of this game?`
  );
}

// Returns true if the message was consumed by the wizard
async function processWizard(chatId, telegramId, text, photoFileId) {
  const session = await getSession(telegramId);
  if (!session) return false;

  const { step, data } = session;

  // ── Step 1: name ─────────────────────────────────────────────
  if (step === 'w_name') {
    const val = (text || '').trim();
    if (val.length < 2) { await tgReply(chatId, '❌ Enter a valid game name (at least 2 characters).'); return true; }
    await setSession(telegramId, 'w_date', { ...data, name: val });
    await tgReply(chatId,
      `✅ *${val}*\n\n*Step 2 of 10 — Game Date & Time*\nWhen is the game? (e.g., \`15 May 8:30 PM\`)\nType "skip" to leave blank.`
    );
    return true;
  }

  // ── Step 2: game date ────────────────────────────────────────
  if (step === 'w_date') {
    const val = (text || '').trim();
    const gameDate = val.toLowerCase() === 'skip' ? null : val;
    await setSession(telegramId, 'w_jointime', { ...data, gameDate });
    await tgReply(chatId,
      `✅ Date: *${gameDate || 'Not set'}*\n\n*Step 3 of 10 — Join Time*\nFrom what time can players start booking? (e.g., \`7:00 PM\`)\nType "skip" to leave blank.`
    );
    return true;
  }

  // ── Step 3: join time ────────────────────────────────────────
  if (step === 'w_jointime') {
    const val = (text || '').trim();
    const joinTime = val.toLowerCase() === 'skip' ? null : val;
    await setSession(telegramId, 'w_sheets', { ...data, joinTime });
    await tgReply(chatId,
      `✅ Join time: *${joinTime || 'Not set'}*\n\n*Step 4 of 10 — Sheet Range*\nEnter the sheet range for this game.\nFormat: \`from-to\` (e.g., \`1-500\` or \`301-600\`)`
    );
    return true;
  }

  // ── Step 4: sheet range ──────────────────────────────────────
  if (step === 'w_sheets') {
    const m = (text || '').match(/^(\d+)\s*[-–]\s*(\d+)$/);
    if (!m) { await tgReply(chatId, '❌ Format: `1-500` (from dash to). Try again.'); return true; }
    const from = parseInt(m[1]), to = parseInt(m[2]);
    if (from >= to) { await tgReply(chatId, '❌ "From" must be less than "to". Try again.'); return true; }
    if (to - from + 1 > 10000) { await tgReply(chatId, '❌ Maximum 10,000 sheets per game.'); return true; }
    const count = to - from + 1;
    await setSession(telegramId, 'w_price', { ...data, sheetFrom: from, sheetTo: to, sheetCount: count });
    await tgReply(chatId,
      `✅ Sheets: *${from}–${to}* (${count} total)\n\n*Step 5 of 10 — Price Per Sheet*\nWhat's the price for a single sheet? (number only, e.g., \`50\`)`
    );
    return true;
  }

  // ── Step 5: price per sheet ──────────────────────────────────
  if (step === 'w_price') {
    const price = parseInt((text || '').replace(/[₹,\s]/g, ''));
    if (!price || price < 1 || price > 100000) { await tgReply(chatId, '❌ Enter a valid price in ₹ (e.g., `50`).'); return true; }
    await setSession(telegramId, 'w_tiers', { ...data, pricePerSheet: price });
    await tgReply(chatId,
      `✅ Price: *₹${price}*\n\n*Step 6 of 10 — Bulk Pricing (Optional)*\n` +
      `Add tiered pricing? Format: \`qty price, qty price\`\n` +
      `Example: \`5 200, 10 350\` = 5 sheets ₹200 · 10 sheets ₹350\n\n` +
      `Type "skip" for no bulk pricing.`
    );
    return true;
  }

  // ── Step 6: pricing tiers ────────────────────────────────────
  if (step === 'w_tiers') {
    let tiers = [];
    const val = (text || '').trim();
    if (val.toLowerCase() !== 'skip' && val !== '') {
      const parts = val.split(',').map(s => s.trim()).filter(Boolean);
      for (const part of parts) {
        const pm = part.match(/^(\d+)\s+(\d+)$/);
        if (!pm) {
          await tgReply(chatId, '❌ Invalid format. Use: `5 200, 10 350` (qty price pairs) or "skip".');
          return true;
        }
        tiers.push({ qty: parseInt(pm[1]), price: parseInt(pm[2]) });
      }
    }
    const tierStr = tiers.length ? tiers.map(t => `${t.qty}×₹${t.price}`).join(', ') : 'None';
    await setSession(telegramId, 'w_prize_0', { ...data, pricingTiers: tiers, prizes: [] });
    await tgReply(chatId,
      `✅ Tiers: *${tierStr}*\n\n*Step 7 of 10 — Prizes*\nI'll ask about each prize type. Enter a cash amount or "skip" to exclude it.\n\n` +
      `*Prize 1 of ${PRIZE_TYPES.length} — ${PRIZE_TYPES[0]}*\nAmount? (e.g., \`5000\`)`
    );
    return true;
  }

  // ── Step 7.x: prizes (one per PRIZE_TYPES entry) ─────────────
  if (step.startsWith('w_prize_')) {
    const idx = parseInt(step.slice(8), 10);
    const val = (text || '').trim();
    const newData = { ...data };

    if (val.toLowerCase() !== 'skip') {
      const amount = parseInt(val.replace(/[₹,\s]/g, ''));
      if (!amount || amount < 1) {
        await tgReply(chatId, '❌ Enter a valid amount (number) or "skip".');
        return true;
      }
      newData.prizes = [...(data.prizes || []), { name: PRIZE_TYPES[idx], kind: 'cash', amount }];
    }

    const nextIdx = idx + 1;
    if (nextIdx < PRIZE_TYPES.length) {
      await setSession(telegramId, `w_prize_${nextIdx}`, newData);
      const display = val.toLowerCase() === 'skip'
        ? 'Skipped'
        : `₹${parseInt(val.replace(/[₹,\s]/g, '')).toLocaleString('en-IN')}`;
      await tgReply(chatId,
        `✅ ${PRIZE_TYPES[idx]}: *${display}*\n\n` +
        `*Prize ${nextIdx + 1} of ${PRIZE_TYPES.length} — ${PRIZE_TYPES[nextIdx]}*\nAmount? or "skip"`
      );
    } else {
      await setSession(telegramId, 'w_description', newData);
      await tgReply(chatId,
        `✅ All ${PRIZE_TYPES.length} prize types collected (${newData.prizes?.length || 0} with amounts).\n\n` +
        `*Step 8 of 10 — Description (Optional)*\nEnter a short game description or type "skip".`
      );
    }
    return true;
  }

  // ── Step 8: description ──────────────────────────────────────
  if (step === 'w_description') {
    const val = (text || '').trim();
    const description = val.toLowerCase() === 'skip' ? '' : val;
    await setSession(telegramId, 'w_thumbnail', { ...data, description });
    await tgReply(chatId,
      `✅ Description: *${description || 'None'}*\n\n` +
      `*Step 9 of 10 — Thumbnail (Optional)*\nSend a photo or paste an image URL for the game card.\nType "skip" for no thumbnail.`
    );
    return true;
  }

  // ── Step 9: thumbnail ────────────────────────────────────────
  if (step === 'w_thumbnail') {
    let thumbnail = null;
    if (photoFileId) {
      const file = await tgGet(`getFile?file_id=${encodeURIComponent(photoFileId)}`);
      if (file?.result?.file_path) {
        thumbnail = `https://api.telegram.org/file/bot${process.env.TELEGRAM_BOT_TOKEN}/${file.result.file_path}`;
      }
    } else {
      const val = (text || '').trim();
      if (val && val.toLowerCase() !== 'skip' && (val.startsWith('http://') || val.startsWith('https://'))) {
        thumbnail = val;
      }
    }
    await setSession(telegramId, 'w_schedule', { ...data, thumbnail });
    await tgReply(chatId,
      `✅ Thumbnail: *${thumbnail ? 'Set' : 'None'}*\n\n` +
      `*Step 10 of 10 — When to Publish?*\n` +
      `When should this game go live for players?\n` +
      `• Type \`now\` to publish immediately\n` +
      `• Or enter a date/time: \`15 May 7:00 PM\``,
      { reply_markup: { inline_keyboard: [[{ text: '🚀 Publish Now', callback_data: 'w_sched:now' }]] } }
    );
    return true;
  }

  // ── Step 10: schedule ────────────────────────────────────────
  if (step === 'w_schedule') {
    await applyScheduleInput(chatId, telegramId, (text || '').trim(), data);
    return true;
  }

  // ── Confirm ──────────────────────────────────────────────────
  if (step === 'w_confirm') {
    const val = (text || '').trim().toLowerCase();
    if (val === 'yes') {
      await finishGame(chatId, telegramId, data);
    } else if (val === 'no') {
      await clearSession(telegramId);
      await tgReply(chatId, '❌ Cancelled.\n\n/newgame to start over.');
    } else {
      await tgReply(chatId, 'Reply *yes* to create the game or *no* to cancel.');
    }
    return true;
  }

  return false;
}

async function applyScheduleInput(chatId, telegramId, val, data) {
  let scheduledFor = null;
  let publishNow = false;

  if (!val) {
    await tgReply(chatId, '❌ Please type "now" or a future date/time (e.g., `15 May 7:00 PM`).');
    return;
  }
  if (val.toLowerCase() === 'now') {
    publishNow = true;
  } else {
    const d = new Date(val);
    if (isNaN(d.getTime())) {
      await tgReply(chatId, '❌ Couldn\'t parse that date. Try "now" or `15 May 7:00 PM`.');
      return;
    }
    if (d.getTime() <= Date.now()) {
      await tgReply(chatId, '❌ That time is in the past. Enter a future date/time.');
      return;
    }
    scheduledFor = d.getTime();
  }

  const newData = { ...data, scheduledFor, publishNow };
  await setSession(telegramId, 'w_confirm', newData);
  await showSummary(chatId, newData);
}

async function showSummary(chatId, data) {
  const priceStr = data.pricingTiers?.length
    ? data.pricingTiers.map(t => `${t.qty}×₹${t.price}`).join(' | ') + ` · Single ₹${data.pricePerSheet}`
    : `₹${data.pricePerSheet} per sheet`;

  const prizeStr = (data.prizes || []).length
    ? (data.prizes || []).map(p => `  • ${p.name}: ₹${Number(p.amount).toLocaleString('en-IN')}`).join('\n')
    : '  (none set)';

  const publishStr = data.publishNow
    ? '🚀 Immediately'
    : `📅 ${new Date(data.scheduledFor).toLocaleString('en-IN')}`;

  const msg =
    `📋 *Confirm Game Details*\n\n` +
    `*Name:* ${data.name}\n` +
    `*Date:* ${data.gameDate || '—'}\n` +
    `*Join Time:* ${data.joinTime || '—'}\n` +
    `*Sheets:* ${data.sheetFrom}–${data.sheetTo} (${data.sheetCount} total)\n` +
    `*Price:* ${priceStr}\n` +
    `*Description:* ${data.description || '—'}\n` +
    `*Thumbnail:* ${data.thumbnail ? '✅ Set' : 'None'}\n` +
    `*Prizes (${(data.prizes || []).length}):*\n${prizeStr}\n` +
    `*Publish:* ${publishStr}\n\n` +
    `Reply *yes* to create or *no* to cancel.`;

  await tgReply(chatId, msg, {
    reply_markup: {
      inline_keyboard: [[
        { text: '✅ Create Game', callback_data: 'w_confirm:yes' },
        { text: '❌ Cancel', callback_data: 'w_confirm:no' }
      ]]
    }
  });
}

async function finishGame(chatId, telegramId, data) {
  const gameId = 'g_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
  const status = data.publishNow ? 'listed' : 'draft';

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
    status,
    sheet_from: data.sheetFrom,
    sheet_to: data.sheetTo,
    sheet_count: data.sheetCount,
    sold_count: 0,
    sold_sheet_nums: [],
    scheduled_for: data.scheduledFor || null,
    created_at: Date.now()
  });

  if (error) {
    await tgReply(chatId, `❌ Failed to save game: ${error.message}`);
    return;
  }

  await clearSession(telegramId);

  if (data.publishNow) {
    const { data: opRow } = await db().from('operators').select('player_channel_id').eq('id', data.operatorId).single();
    if (opRow?.player_channel_id) {
      await broadcastGame(opRow.player_channel_id, { id: gameId, ...data });
    }
    await tgReply(chatId,
      `🎉 *Game Published!*\n\n` +
      `*${data.name}* is live — players can book now.\n` +
      `Game ID: \`${gameId}\`\n\n` +
      `/stats to monitor sales.`
    );
  } else {
    await tgReply(chatId,
      `✅ *Game Scheduled!*\n\n` +
      `*${data.name}* will go live at:\n` +
      `*${new Date(data.scheduledFor).toLocaleString('en-IN')}*\n` +
      `Game ID: \`${gameId}\`\n\n` +
      `The bot will publish and post to your player channel automatically.`
    );
  }
}

// ─────────────────────────────────────────────────────────────
// Channel broadcast — used by finishGame and api/cron.js
// ─────────────────────────────────────────────────────────────

async function broadcastGame(channelId, game) {
  const host = process.env.APP_HOST || 'tungbola-market.vercel.app';
  const url   = `https://${host}/g/${game.id}`;

  const priceStr = Array.isArray(game.pricingTiers) && game.pricingTiers.length
    ? game.pricingTiers.map(t => `${t.qty}×₹${t.price}`).join(' | ') + ` · Single ₹${game.pricePerSheet}`
    : `₹${game.pricePerSheet} per sheet`;

  const prizeLines = (game.prizes || []).slice(0, 5)
    .map(p => `• ${p.name}: ₹${Number(p.amount).toLocaleString('en-IN')}`)
    .join('\n');

  let msg = `🎯 *${game.name}*`;
  if (game.gameDate)  msg += `\n📅 ${game.gameDate}`;
  if (game.joinTime)  msg += ` · ⏰ Book by ${game.joinTime}`;
  msg += `\n\n💰 ${priceStr}`;
  if (prizeLines)     msg += `\n\n🏆 *Prizes:*\n${prizeLines}`;
  msg += `\n\n📋 ${game.sheetCount} sheets available`;

  await tgSend('sendMessage', {
    chat_id: channelId,
    text: msg,
    parse_mode: 'Markdown',
    reply_markup: { inline_keyboard: [[{ text: '🎟 Book Sheets Now', url }]] }
  });
}

// ─────────────────────────────────────────────────────────────
// Approve / Reject (order management)
// ─────────────────────────────────────────────────────────────

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
  const dlToken   = Date.now().toString(36).toUpperCase() + Math.random().toString(36).slice(2, 9).toUpperCase();
  const now       = Date.now();
  const newSoldNums = [...game.soldSheetNums, ...assigned.map(s => s.n)];

  await Promise.all([
    db().from('download_tokens').insert({ token: dlToken, sheets: sheetList, game_name: game.name, purchase_id: purchaseId }),
    db().from('games').update({ sold_sheet_nums: newSoldNums, sold_count: newSoldNums.length }).eq('id', game.id),
    db().from('purchases').update({ status: 'approved', download_token: dlToken, approved_at: now, sheet_nums: assigned.map(s => s.n) }).eq('purchase_id', purchaseId)
  ]);

  try {
    const np = String(purchase.phone).replace(/\D/g, '');
    const { data: pushRow } = await db().from('push_subscriptions').select('subscription').eq('phone', np).single();
    if (pushRow?.subscription) await sendPush(pushRow.subscription);
  } catch (e) {}

  await answerCallback(callbackQueryId, '✅ Approved! Player notified.');
  await editMessage(chatId, messageId,
    `✅ *Approved* — #${purchaseId.slice(-6)}\n\n👤 ${purchase.playerName}\n🎮 ${purchase.gameName}\n📋 ${purchase.quantity} sheets assigned\n💰 ₹${purchase.amount}`
  );
}

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

    // ── Callback query (button presses) ──────────────────────
    if (update.callback_query) {
      const cb    = update.callback_query;
      const cbData = cb.data || '';
      const chatId = cb.message?.chat?.id;
      const messageId = cb.message?.message_id;
      const telegramId = cb.from?.id;
      const callbackQueryId = cb.id;

      if (!chatId || !messageId) return res.status(200).json({ ok: true });

      if (cbData.startsWith('approve:')) {
        await handleApprove(cbData.slice(8), chatId, messageId, callbackQueryId);
      } else if (cbData.startsWith('reject:')) {
        await handleReject(cbData.slice(7), chatId, messageId, callbackQueryId);
      } else if (cbData.startsWith('w_sched:')) {
        const session = await getSession(telegramId);
        if (session?.step === 'w_schedule') {
          await answerCallback(callbackQueryId, '');
          await applyScheduleInput(chatId, telegramId, cbData.slice(8), session.data);
        } else {
          await answerCallback(callbackQueryId, 'Session expired. Use /newgame to restart.');
        }
      } else if (cbData.startsWith('w_confirm:')) {
        const session = await getSession(telegramId);
        await answerCallback(callbackQueryId, '');
        if (!session) {
          await tgReply(chatId, 'Session expired. Use /newgame to restart.');
        } else if (cbData === 'w_confirm:yes') {
          await finishGame(chatId, telegramId, session.data);
        } else {
          await clearSession(telegramId);
          await tgReply(chatId, '❌ Cancelled.\n\n/newgame to start over.');
        }
      } else {
        await answerCallback(callbackQueryId, '');
      }

      return res.status(200).json({ ok: true });
    }

    // ── Text / photo messages ────────────────────────────────
    if (update.message) {
      const msg = update.message;
      const chatId    = msg.chat?.id;
      const telegramId = msg.from?.id;
      if (!chatId || !telegramId) return res.status(200).json({ ok: true });

      // Only handle private chats (not group/channel messages)
      if (msg.chat?.type !== 'private') return res.status(200).json({ ok: true });

      const text = msg.text || '';
      const photoFileId = msg.photo?.length ? msg.photo[msg.photo.length - 1].file_id : null;

      // Parse slash command
      const cmdMatch = text.match(/^\/(\w+)(?:@\S+)?\s*([\s\S]*)?$/);
      if (cmdMatch) {
        const cmd  = cmdMatch[1].toLowerCase();
        const args = (cmdMatch[2] || '').trim();

        if (cmd === 'start' || cmd === 'help') {
          await handleHelp(chatId);
        } else if (cmd === 'link') {
          await handleLink(chatId, telegramId, args);
        } else if (cmd === 'newgame') {
          await handleNewGame(chatId, telegramId);
        } else if (cmd === 'stats') {
          await handleStats(chatId, telegramId);
        } else if (cmd === 'setchannel') {
          await handleSetChannel(chatId, telegramId, args);
        } else if (cmd === 'cancel') {
          await handleCancel(chatId, telegramId);
        }
        // Unknown commands are silently ignored
        return res.status(200).json({ ok: true });
      }

      // Feed non-command messages into the wizard
      const consumed = await processWizard(chatId, telegramId, text, photoFileId);
      if (!consumed && text) {
        await tgReply(chatId, 'Use /help to see available commands.');
      }
    }
  } catch (e) {
    console.error('Telegram webhook error:', e.message);
  }

  return res.status(200).json({ ok: true });
};

// Exported for use by api/cron.js
module.exports.broadcastGame = broadcastGame;
