// Sources and prepares purchased sheets for a Plan B ("generate") operator's
// game at approve-purchase time.
//
// Plan A (own-sheets) operators upload ready-made PDF files, so operator_sheets
// rows already carry a filename/url to hand the buyer directly. Plan B
// operators generate sheets on demand via Sheet Factory (Sheet Factory ->
// generate-sheets -> generated_sheets), which only holds structured ticket
// numbers, not a rendered file — so at delivery time we render one PDF per
// purchased sheet here and upload it, then hand back the exact same
// {n, filename, url} shape the own-sheets path already produces. Every
// downstream consumer (download_tokens, the player download page, Telegram
// document delivery) needs no changes because of that.
//
// Used by all three purchase-approval entry points (api/operator.js's
// approve-purchase, api/marketplace.js's admin approve-purchase, and
// api/telegram.js's inline approval callback) so a purchase behaves
// identically no matter which surface approved it.

const { put } = require('@vercel/blob');
const { db } = require('./_db');
const { buildBulkPDF } = require('./_pdfRenderer');

async function resolveGenerateSheetsForPurchase(operatorId, game, purchase) {
  const { data: rows } = await db().from('generated_sheets')
    .select('n, tickets')
    .eq('operator_id', operatorId)
    .eq('game_id', game.id)
    .eq('status', 'available')
    .order('n', { ascending: true });

  const available = rows || [];
  if (available.length < purchase.quantity)
    return { error: available.length
      ? `Only ${available.length} sheets left`
      : 'No sheets generated for this game yet — ask the operator to generate more in Sheet Factory.' };

  let assigned;
  if (purchase.requestedSheetNums?.length) {
    const reqSet = new Set(purchase.requestedSheetNums);
    assigned = [...available.filter(s => reqSet.has(s.n)), ...available.filter(s => !reqSet.has(s.n))].slice(0, purchase.quantity);
  } else {
    assigned = available.slice(0, purchase.quantity);
  }

  const sheetList = [];
  for (const s of assigned) {
    const doc = buildBulkPDF([{ n: s.n, tickets: s.tickets }], { eventName: game.name });
    const buffer = Buffer.from(doc.output('arraybuffer'));
    const blob = await put(
      `tungbola/op-${operatorId}/gen-sheet-${s.n}-${Date.now()}.pdf`,
      buffer,
      { access: 'public', contentType: 'application/pdf' }
    );
    sheetList.push({ n: s.n, filename: `sheet-${s.n}.pdf`, url: blob.url });
  }

  // Mark these rows sold so Sheet Factory's Available/Sold counts stay accurate.
  await db().from('generated_sheets')
    .update({ status: 'sold' })
    .eq('operator_id', operatorId)
    .in('n', assigned.map(s => s.n));

  return { assigned, sheetList };
}

module.exports = { resolveGenerateSheetsForPurchase };
