const { db, gameFromRow } = require('./_db');
const { broadcastGame }   = require('./telegram');

// Vercel cron job — runs every minute (Pro plan required).
// Publishes draft games whose scheduled_for time has passed,
// then broadcasts them to the operator's player channel.

module.exports = async function(req, res) {
  // Vercel sends Authorization: Bearer {CRON_SECRET} for cron requests
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret) {
    if (req.headers.authorization !== `Bearer ${cronSecret}`)
      return res.status(401).json({ error: 'Unauthorized' });
  }

  const now = Date.now();

  const { data: games, error } = await db().from('games')
    .select('*')
    .eq('status', 'draft')
    .not('scheduled_for', 'is', null)
    .lte('scheduled_for', now);

  if (error) {
    console.error('Cron query error:', error.message);
    return res.status(500).json({ error: error.message });
  }

  const published = [];
  for (const gRow of games || []) {
    try {
      await db().from('games').update({ status: 'listed' }).eq('id', gRow.id);

      if (gRow.operator_id) {
        const { data: opRow } = await db()
          .from('operators')
          .select('player_channel_id')
          .eq('id', gRow.operator_id)
          .single();

        if (opRow?.player_channel_id) {
          await broadcastGame(opRow.player_channel_id, gameFromRow(gRow));
        }
      }
      published.push(gRow.id);
    } catch (e) {
      console.error(`Cron: failed to publish ${gRow.id}:`, e.message);
    }
  }

  return res.json({ ok: true, published, count: published.length });
};
