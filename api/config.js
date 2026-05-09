const { secureHeaders, rateLimit } = require('./_security');
const { db } = require('./_db');

module.exports = async function(req, res) {
  secureHeaders(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).end();
  if (await rateLimit(req, 'mktcfg', 60, 60))
    return res.status(429).json({ error: 'Too many requests' });
  try {
    const { data } = await db().from('config').select('value').eq('key', 'app_config').single();
    res.json(data?.value || { pricePerSheet: 5, upiId: '', whatsappNumber: '' });
  } catch(e) {
    res.json({ pricePerSheet: 5, upiId: '', whatsappNumber: '' });
  }
};
