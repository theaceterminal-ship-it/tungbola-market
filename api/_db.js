const { createClient } = require('@supabase/supabase-js');

let _client;
function db() {
  if (!_client) {
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY)
      throw new Error('SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set');
    _client = createClient(
      process.env.SUPABASE_URL,
      process.env.SUPABASE_SERVICE_ROLE_KEY,
      { auth: { persistSession: false, autoRefreshToken: false } }
    );
  }
  return _client;
}

// ── DB row ↔ API object transforms ──────────────────────────

function gameFromRow(r) {
  if (!r) return null;
  return {
    id: r.id, operatorId: r.operator_id || null, operatorName: r.operator_name || null,
    name: r.name, gameDate: r.game_date || null, gameDateRaw: r.game_date_raw || null,
    joinTime: r.join_time || null, joinLink: r.join_link || null, joinDetails: r.join_details || null,
    pricePerSheet: r.price_per_sheet,
    pricingTiers: r.pricing_tiers || [], description: r.description || '',
    prizes: r.prizes || [], thumbnail: r.thumbnail || null, status: r.status,
    sheetFrom: r.sheet_from, sheetTo: r.sheet_to, sheetCount: r.sheet_count,
    soldCount: r.sold_count, soldSheetNums: r.sold_sheet_nums || [],
    scheduledFor: r.scheduled_for || null,
    createdAt: r.created_at
  };
}

function gameToRow(g) {
  return {
    id: g.id, operator_id: g.operatorId || null, operator_name: g.operatorName || null,
    name: g.name, game_date: g.gameDate || null, game_date_raw: g.gameDateRaw || null,
    join_time: g.joinTime || null, join_link: g.joinLink || null, join_details: g.joinDetails || null,
    price_per_sheet: g.pricePerSheet || 5,
    pricing_tiers: g.pricingTiers || [], description: g.description || '',
    prizes: g.prizes || [], thumbnail: g.thumbnail || null,
    status: g.status || 'draft', sheet_from: g.sheetFrom || 0,
    sheet_to: g.sheetTo || 0, sheet_count: g.sheetCount || 0,
    sold_count: g.soldCount || 0, sold_sheet_nums: g.soldSheetNums || [],
    scheduled_for: g.scheduledFor || null,
    created_at: g.createdAt || Date.now()
  };
}

function purchaseFromRow(r) {
  if (!r) return null;
  return {
    purchaseId: r.purchase_id, playerName: r.player_name, phone: r.phone,
    gameId: r.game_id, gameName: r.game_name, quantity: r.quantity, amount: r.amount,
    requestedSheetNums: r.requested_sheet_nums || null, status: r.status,
    downloadToken: r.download_token || null, sheetNums: r.sheet_nums || null,
    createdAt: r.created_at, approvedAt: r.approved_at || null,
    downloaded: r.downloaded || false, downloadedAt: r.downloaded_at || null,
    utr: r.utr || null
  };
}

function purchaseToRow(p) {
  return {
    purchase_id: p.purchaseId, player_name: p.playerName, phone: p.phone,
    game_id: p.gameId, game_name: p.gameName, quantity: p.quantity, amount: p.amount,
    requested_sheet_nums: p.requestedSheetNums || null, status: p.status || 'pending',
    download_token: p.downloadToken || null, sheet_nums: p.sheetNums || null,
    created_at: p.createdAt || Date.now(), approved_at: p.approvedAt || null,
    downloaded: p.downloaded || false, downloaded_at: p.downloadedAt || null
  };
}

function operatorFromRow(r) {
  if (!r) return null;
  return {
    id: r.id, name: r.name, email: r.email || '', phone: r.phone || '',
    plan: r.plan, apiKey: r.api_key, telegramChatId: r.telegram_chat_id || null,
    telegramId: r.telegram_id || null, playerChannelId: r.player_channel_id || null,
    active: r.active, createdAt: r.created_at,
    displayName: r.display_name || null,
    supportPhone: r.support_phone || null,
    upiId: r.upi_id || null,
    zoomConnected:    !!(r.zoom_access_token),
    zoomAccessToken:  r.zoom_access_token  || null,
    zoomRefreshToken: r.zoom_refresh_token || null,
    zoomTokenExpiry:  r.zoom_token_expiry  || null,
  };
}

function sheetFromRow(r) {
  if (!r) return null;
  return { id: r.id, n: r.n, f: r.f, u: r.u, s: r.s || 0, ts: r.ts };
}

module.exports = { db, gameFromRow, gameToRow, purchaseFromRow, purchaseToRow, operatorFromRow, sheetFromRow };
