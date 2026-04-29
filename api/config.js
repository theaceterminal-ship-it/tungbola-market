const { Redis } = require('@upstash/redis');
const { secureHeaders, rateLimit } = require('./_security');
const kv = Redis.fromEnv();

module.exports = async function(req, res) {
  secureHeaders(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).end();
  if (await rateLimit(req, 'mktcfg', 60, 60))
    return res.status(429).json({ error: 'Too many requests' });
  try {
    const cfg = await kv.get('tb:config') || { pricePerSheet: 5, upiId: '', whatsappNumber: '' };
    res.json(cfg);
  } catch(e) {
    res.json({ pricePerSheet: 5, upiId: '', whatsappNumber: '' });
  }
};
