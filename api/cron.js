const { db, gameFromRow } = require('./_db');
const { broadcastGame, tgSend } = require('./telegram');

const HOST = process.env.APP_HOST || 'tungbola-market.vercel.app';

// 5 rotating reminder messages with Indian tambola energy
const REMINDERS = [
  (name, rem) =>
    `🔥 *${name}*\n\n${rem} sheets left! Aaj luck garam hai — abhi book karo! 🎯`,
  (name, rem) =>
    `⚡ *${name}*\n\nSirf *${rem} sheets* bacha hai! Miss mat karna 🎟`,
  (name, rem) =>
    `🏃 *${name}* filling fast!\n\n${rem} sheets remaining. Jo nahi aaya, woh miss karega 😅`,
  (name, rem) =>
    `🍀 *${name}*\n\n${rem} sheets still available! Aaj lucky feel ho raha hai? Book now! 🎰`,
  (name, rem) =>
    `⏰ *${name}*\n\nOnly *${rem} sheets* left — hurry up or miss the chance! 🔥`,
];

async function sendReminder(channelId, gRow, idx) {
  const total = gRow.sheet_count || (gRow.sheet_to - gRow.sheet_from + 1);
  const rem   = Math.max(0, total - (gRow.sold_count || 0));
  const url   = `https://${HOST}/g/${gRow.id}`;
  const text  = REMINDERS[idx % REMINDERS.length](gRow.name, rem);
  await tgSend('sendMessage', {
    chat_id: channelId,
    text: `${text}\n\n📋 _${rem} of ${total} sheets remaining_`,
    parse_mode: 'Markdown',
    reply_markup: { inline_keyboard: [[{ text: '🎟 Book Sheets Now', url }]] }
  });
}

async function sendMilestone(channelId, gRow) {
  const total = gRow.sheet_count || (gRow.sheet_to - gRow.sheet_from + 1);
  const rem   = Math.max(0, total - (gRow.sold_count || 0));
  const url   = `https://${HOST}/g/${gRow.id}`;
  await tgSend('sendMessage', {
    chat_id: channelId,
    text:
      `🎉 *${gRow.name}* is heating up!\n\n` +
      `First *${gRow.sold_count} sheets* sold already!\n` +
      `Only *${rem} sheets* remaining.\n\n` +
      `Aaj lucky feel ho raha hai? Book yours now! 🍀`,
    parse_mode: 'Markdown',
    reply_markup: { inline_keyboard: [[{ text: '🎟 Book Sheets Now', url }]] }
  });
}

module.exports = async function(req, res) {
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret && req.headers.authorization !== `Bearer ${cronSecret}`)
    return res.status(401).json({ error: 'Unauthorized' });

  const now   = Date.now();
  const today = new Date().toISOString().slice(0, 10);
  // Active hours check: IST = UTC+5:30. Allow 8am–10pm IST = 2:30am–4:30pm UTC
  const hourUTC = new Date().getUTCHours();
  const inActiveHours = hourUTC >= 2 && hourUTC <= 16;

  // ── 1. Publish scheduled games ──────────────────────────────
  const { data: scheduled, error: schedErr } = await db().from('games')
    .select('*')
    .eq('status', 'draft')
    .not('scheduled_for', 'is', null)
    .lte('scheduled_for', now);

  if (schedErr) {
    console.error('Cron schedule query:', schedErr.message);
    return res.status(500).json({ error: schedErr.message });
  }

  const published = [];
  for (const gRow of scheduled || []) {
    try {
      await db().from('games').update({ status: 'listed' }).eq('id', gRow.id);
      if (gRow.operator_id) {
        const { data: opRow } = await db().from('operators')
          .select('player_channel_id').eq('id', gRow.operator_id).single();
        if (opRow?.player_channel_id)
          await broadcastGame(opRow.player_channel_id, gameFromRow(gRow));
      }
      published.push(gRow.id);
    } catch (e) {
      console.error(`Cron publish ${gRow.id}:`, e.message);
    }
  }

  // ── 2. Milestone + periodic reminders ──────────────────────
  const reminded = [], milestones = [];

  if (inActiveHours) {
    const { data: active } = await db().from('games')
      .select('*')
      .eq('status', 'listed')
      .not('operator_id', 'is', null);

    if (active?.length) {
      // Batch-fetch operator channels
      const opIds = [...new Set(active.map(g => g.operator_id))];
      const { data: ops } = await db().from('operators')
        .select('id, player_channel_id')
        .in('id', opIds);
      const channelMap = Object.fromEntries((ops || []).map(o => [o.id, o.player_channel_id]));

      for (const gRow of active) {
        const channelId = channelMap[gRow.operator_id];
        if (!channelId) continue;

        const total = gRow.sheet_count || (gRow.sheet_to - gRow.sheet_from + 1);
        const sold  = gRow.sold_count || 0;
        if (sold >= total) continue; // sold out — no more reminders

        // 10% milestone (fires once)
        if (!gRow.milestone_10_sent && sold / total >= 0.1) {
          try {
            await sendMilestone(channelId, gRow);
            await db().from('games').update({ milestone_10_sent: true }).eq('id', gRow.id);
            milestones.push(gRow.id);
          } catch (e) { console.error(`Milestone ${gRow.id}:`, e.message); }
        }

        // Periodic reminders: max 5/day, min 2.5h apart
        const remToday = gRow.reminder_date === today ? (gRow.reminders_today || 0) : 0;
        if (remToday >= 5) continue;

        const lastAt = gRow.last_reminder_at || 0;
        const INTERVAL = 2.5 * 60 * 60 * 1000; // 2.5 hours in ms
        if (now - lastAt < INTERVAL) continue;

        try {
          await sendReminder(channelId, gRow, remToday);
          await db().from('games').update({
            last_reminder_at: now,
            reminders_today:  remToday + 1,
            reminder_date:    today
          }).eq('id', gRow.id);
          reminded.push(gRow.id);
        } catch (e) { console.error(`Reminder ${gRow.id}:`, e.message); }
      }
    }
  }

  // ── 3. Stale bot session cleanup (older than 24 hours) ────────
  try {
    const cutoff = new Date(Date.now() - 86400000).toISOString();
    await db().from('bot_sessions').delete().lt('updated_at', cutoff);
  } catch(e) { console.error('Session cleanup error:', e.message); }

  return res.json({ ok: true, published, milestones, reminded });
};
