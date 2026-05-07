const { Redis } = require('@upstash/redis');
const kv = Redis.fromEnv();

function esc(s) {
  return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

module.exports = async function(req, res) {
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  const { id } = req.query;
  if (!id) return res.redirect('/');

  try {
    const games = await kv.get('tb:mkt:games') || [];
    const g = games.find(x => x.id === id);
    if (!g) return res.redirect('/');

    const host = req.headers.host || 'tungbola-market.vercel.app';
    const base = `https://${host}`;
    const appUrl  = `${base}/?game=${encodeURIComponent(id)}`;
    const ogUrl   = `${base}/api/og?id=${encodeURIComponent(id)}`;
    const image   = g.thumbnail || '';

    const title = `🎯 ${g.name}`;
    const topPrize = (g.prizes || []).find(p => /full house/i.test(p.name || ''));
    const prizeStr = topPrize && topPrize.kind === 'cash'
      ? `Win up to ₹${Number(topPrize.amount || 0).toLocaleString('en-IN')}! `
      : '';
    const dateStr = g.gameDate ? ` · ${g.gameDate}` : '';
    const desc = `${prizeStr}Book your Tambola sheet from ₹${g.pricePerSheet || 5} each${dateStr}. Tap to join!`;

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.setHeader('Cache-Control', 'public, max-age=300');
    res.send(`<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>${esc(title)}</title>
<meta property="og:title" content="${esc(title)}" />
<meta property="og:description" content="${esc(desc)}" />
<meta property="og:url" content="${esc(ogUrl)}" />
<meta property="og:type" content="website" />
<meta property="og:site_name" content="Tungbola Market" />
${image ? `<meta property="og:image" content="${esc(image)}" />
<meta property="og:image:width" content="1200" />
<meta property="og:image:height" content="630" />
<meta name="twitter:card" content="summary_large_image" />
<meta name="twitter:image" content="${esc(image)}" />` : '<meta name="twitter:card" content="summary" />'}
<meta name="twitter:title" content="${esc(title)}" />
<meta name="twitter:description" content="${esc(desc)}" />
<script>window.location.replace(${JSON.stringify(appUrl)});</script>
</head>
<body style="font-family:sans-serif;padding:20px;color:#333">
<p>Opening <a href="${esc(appUrl)}">${esc(title)}</a>…</p>
</body>
</html>`);
  } catch(e) {
    console.error('og.js error:', e.message);
    res.redirect('/');
  }
};
