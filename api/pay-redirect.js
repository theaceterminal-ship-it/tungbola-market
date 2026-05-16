// Redirects to a upi:// deep link — needed because Telegram inline buttons
// only allow https:// URLs, not upi:// scheme.
module.exports = function (req, res) {
  const { pa, pn, am, tn } = req.query;
  if (!pa) return res.status(400).send('Missing UPI ID');

  const upiLink =
    `upi://pay?pa=${encodeURIComponent(pa)}` +
    `&pn=${encodeURIComponent(pn || '')}` +
    `&am=${encodeURIComponent(am || '')}` +
    `&tn=${encodeURIComponent(tn || '')}` +
    `&cu=INR`;

  const safeUpi  = String(pa).replace(/[<>"'&]/g, '');
  const safeAm   = String(am || '').replace(/[^0-9.]/g, '');
  const safeLink = upiLink.replace(/"/g, '&quot;');

  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Pay via UPI</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
      background:#0f172a;color:#f8fafc;min-height:100vh;
      display:flex;align-items:center;justify-content:center;padding:1.5rem}
    .card{background:#1e293b;border-radius:1.25rem;padding:2rem 1.5rem;
      max-width:360px;width:100%;text-align:center;box-shadow:0 20px 60px rgba(0,0,0,.4)}
    h1{font-size:1.25rem;font-weight:700;margin-bottom:.5rem}
    .amount{font-size:2.5rem;font-weight:900;color:#a78bfa;margin:1rem 0}
    .upi{font-size:.85rem;color:#94a3b8;margin-bottom:1.75rem;word-break:break-all}
    a.btn{display:block;background:#7c3aed;color:#fff;font-size:1rem;font-weight:700;
      padding:.9rem 1.5rem;border-radius:.75rem;text-decoration:none;
      transition:background .15s}
    a.btn:active{background:#6d28d9}
    .note{font-size:.75rem;color:#475569;margin-top:1.25rem;line-height:1.5}
  </style>
</head>
<body>
  <div class="card">
    <h1>💳 Pay via UPI</h1>
    <div class="amount">₹${safeAm}</div>
    <div class="upi">To: ${safeUpi}</div>
    <a class="btn" href="${safeLink}">📱 Open UPI App</a>
    <p class="note">Tap above to open PhonePe, GPay, Paytm or any UPI app with the amount pre-filled.</p>
  </div>
  <script>
    // Auto-redirect on load (works on most mobile browsers)
    setTimeout(function(){ window.location.href = "${safeLink}"; }, 300);
  </script>
</body>
</html>`);
};
