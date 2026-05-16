const { db } = require('./_db');

module.exports = async function(req, res) {
  if (req.method !== 'GET') return res.status(405).end();

  const { code, state: apiKey, error } = req.query;
  const returnUrl = process.env.TUKPAMASTER_URL || 'https://tukpamaster.vercel.app';

  if (error) {
    return res.redirect(`${returnUrl}?zoom=error&msg=${encodeURIComponent(error)}`);
  }
  if (!code || !apiKey) {
    return res.redirect(`${returnUrl}?zoom=error&msg=Missing+parameters`);
  }

  try {
    const tokenRes = await fetch('https://zoom.us/oauth/token', {
      method: 'POST',
      headers: {
        'Authorization': 'Basic ' + Buffer.from(
          `${process.env.ZOOM_CLIENT_ID}:${process.env.ZOOM_CLIENT_SECRET}`
        ).toString('base64'),
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        grant_type: 'authorization_code',
        code,
        redirect_uri: process.env.ZOOM_REDIRECT_URI,
      }).toString(),
    });

    if (!tokenRes.ok) {
      const err = await tokenRes.text();
      console.error('Zoom token exchange failed:', err);
      return res.redirect(`${returnUrl}?zoom=error&msg=${encodeURIComponent('Token exchange failed')}`);
    }

    const tokens = await tokenRes.json();
    const expiry = Date.now() + (tokens.expires_in * 1000);

    const { data: op } = await db().from('operators').select('id').eq('api_key', String(apiKey)).eq('active', true).single();
    if (!op) {
      return res.redirect(`${returnUrl}?zoom=error&msg=Operator+not+found`);
    }

    await db().from('operators').update({
      zoom_access_token:  tokens.access_token,
      zoom_refresh_token: tokens.refresh_token,
      zoom_token_expiry:  expiry,
    }).eq('id', op.id);

    return res.redirect(`${returnUrl}?zoom=connected`);
  } catch (e) {
    console.error('Zoom callback error:', e.message);
    return res.redirect(`${returnUrl}?zoom=error&msg=${encodeURIComponent('Server error')}`);
  }
};
