const { db, gameFromRow } = require('./_db');

function esc(s) {
  return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

const PRIO = [
  [/^full house$/i, 1], [/^second full house$/i, 2], [/^third full house$/i, 3],
  [/^upper line$/i, 4], [/^middle line$/i, 5],        [/^bottom line$/i, 6],
  [/^ticket corners$/i, 7], [/^sheet corner$/i, 8],   [/^early [567]$/i, 9],
];
const PRIZE_ICONS = { 1:'👑', 2:'🥈', 3:'🥉', 4:'⭐', 5:'⭐', 6:'⭐', 7:'💎', 8:'💎', 9:'⚡' };

function prio(n) { for (const [re, r] of PRIO) if (re.test(n)) return r; return 10; }
function fmtAmount(p) {
  if (p.kind === 'cash') return `₹${Number(p.amount || 0).toLocaleString('en-IN')}`;
  return p.description || '';
}

module.exports = async function(req, res) {
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  const { id } = req.query;
  if (!id) return res.redirect('/');

  try {
    const { data: gRow } = await db().from('games').select('*').eq('id', id).single();
    if (!gRow) return res.redirect('/');
    const g = gameFromRow(gRow);

    const host    = req.headers.host || 'tungbola-market.vercel.app';
    const base    = `https://${host}`;
    const appUrl  = `${base}/?game=${encodeURIComponent(id)}`;
    const shareUrl = `${base}/g/${id}`;
    const image   = g.thumbnail || '';
    const title   = `🎯 ${g.name}`;

    const sorted = [...(g.prizes || [])].sort((a, b) => prio(a.name) - prio(b.name));
    const prizeLines = sorted.slice(0, 5).map(p => `${PRIZE_ICONS[prio(p.name)] || '⭐'} ${p.name}: ${fmtAmount(p)}`);

    const tiers = Array.isArray(g.pricingTiers) ? g.pricingTiers : [];
    let priceLine = `💰 Sheet: ₹${g.pricePerSheet || 5}`;
    if (tiers.length) priceLine = `💰 ${tiers.slice(0, 2).map(t => `${t.qty}×₹${t.price}`).join(' · ')} | Single ₹${g.pricePerSheet || 5}`;

    const dateStr = g.gameDate ? ` · 📅 ${g.gameDate}` : '';
    const desc = [prizeLines.join(' · '), priceLine + dateStr + ' · Tap to join! 🎱'].filter(Boolean).join('\n');

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.setHeader('Cache-Control', 'public, max-age=300');
    res.send(`<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>${esc(title)}</title>
<meta property="og:title" content="${esc(title)}" />
<meta property="og:description" content="${esc(desc)}" />
<meta property="og:url" content="${esc(shareUrl)}" />
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
