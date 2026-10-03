// Short-lived holds on picked sheet numbers.
//
// Sheets used to be locked only when an operator approved a purchase, which
// left a wide window: a buyer picks #42, leaves for their UPI app, and comes
// back to find someone else got it. A hold is taken the moment checkout opens
// and released on approve, reject or expiry.
//
// The sheet_reservations primary key is (game_id, n), so a concurrent insert
// for the same number fails at the database rather than being resolved here.

const { db } = require('./_db');

const HOLD_MS = 10 * 60 * 1000;          // while the buyer is in their UPI app
const PENDING_HOLD_MS = 2 * 60 * 60 * 1000; // once ordered, until approve/reject

function genReservationId() {
  return 'rs_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
}

// Expired rows are dropped lazily on every read and write — cheap, and it
// means a hold never outlives its TTL even if the cron has not run.
async function purgeExpired(gameId) {
  const q = db().from('sheet_reservations').delete().lt('expires_at', new Date().toISOString());
  if (gameId) q.eq('game_id', gameId);
  try { await q; } catch (e) { /* a stale hold is not worth failing a read over */ }
}

/** Sheet numbers currently held for `gameId`, excluding one caller's own hold. */
async function heldNums(gameId, exceptReservationId) {
  await purgeExpired(gameId);
  let q = db().from('sheet_reservations').select('n, reservation_id').eq('game_id', gameId);
  const { data } = await q;
  return new Set((data || [])
    .filter(r => !exceptReservationId || r.reservation_id !== exceptReservationId)
    .map(r => r.n));
}

/**
 * Hold `nums` for `gameId`. Replaces the caller's previous hold if given, so
 * re-opening checkout after changing the selection does not fight itself.
 * Returns { reservationId, expiresAt } or { conflicts: [n, ...] }.
 */
async function reserveSheets({ gameId, nums, phone, soldNums = [], previousReservationId, ttlMs = HOLD_MS }) {
  if (!Array.isArray(nums) || !nums.length) return { error: 'No sheets selected' };

  if (previousReservationId) await releaseReservation({ reservationId: previousReservationId });
  await purgeExpired(gameId);

  const soldSet = new Set(soldNums);
  const alreadySold = nums.filter(n => soldSet.has(n));
  if (alreadySold.length) return { conflicts: alreadySold, reason: 'sold' };

  const { data: held } = await db().from('sheet_reservations')
    .select('n').eq('game_id', gameId).in('n', nums);
  if (held?.length) return { conflicts: held.map(r => r.n).sort((a, b) => a - b), reason: 'held' };

  const reservationId = genReservationId();
  const expiresAt = new Date(Date.now() + ttlMs).toISOString();
  const { error } = await db().from('sheet_reservations').insert(
    nums.map(n => ({ game_id: gameId, n, reservation_id: reservationId, phone: phone || null, expires_at: expiresAt }))
  );

  if (error) {
    // Lost a race on the (game_id, n) key — report exactly which numbers went.
    await db().from('sheet_reservations').delete().eq('reservation_id', reservationId);
    const { data: now } = await db().from('sheet_reservations')
      .select('n').eq('game_id', gameId).in('n', nums);
    return { conflicts: (now || []).map(r => r.n).sort((a, b) => a - b), reason: 'held' };
  }

  return { reservationId, expiresAt };
}

/** Confirms a hold survives to the order, and extends it past the UPI window. */
async function attachToPurchase(reservationId, purchaseId, nums) {
  if (!reservationId) return { ok: false };
  const { data } = await db().from('sheet_reservations')
    .select('n').eq('reservation_id', reservationId).gt('expires_at', new Date().toISOString());
  const have = new Set((data || []).map(r => r.n));
  const lost = (nums || []).filter(n => !have.has(n));
  if (lost.length) return { ok: false, lost };

  await db().from('sheet_reservations')
    .update({ purchase_id: purchaseId, expires_at: new Date(Date.now() + PENDING_HOLD_MS).toISOString() })
    .eq('reservation_id', reservationId);
  return { ok: true };
}

/** Called from every approve and reject path once the sheets are settled. */
async function releaseReservation({ reservationId, purchaseId }) {
  try {
    if (reservationId) await db().from('sheet_reservations').delete().eq('reservation_id', reservationId);
    if (purchaseId)    await db().from('sheet_reservations').delete().eq('purchase_id', purchaseId);
  } catch (e) { console.error('releaseReservation failed:', e.message); }
}

module.exports = { HOLD_MS, PENDING_HOLD_MS, reserveSheets, heldNums, attachToPurchase, releaseReservation, purgeExpired };
