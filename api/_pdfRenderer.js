// Server-side port of src/lib/pdfRenderer.ts in the tukpamaster repo — kept in
// lockstep with that client renderer so a purchased Plan B (generate) sheet
// looks identical to what the operator sees in Sheet Factory / their own
// exports. jsPDF's core vector/text drawing has no DOM dependency, so this
// runs unchanged in a Vercel serverless function.
//
// Sheet shape expected here: { n: number, tickets: [{ pos, numbers }, ...] }
// — n is the sheet number (generated_sheets.n), tickets come straight from
// generateTickets() in _tambola.js (numbers is a 3x9 grid, pos is 1-6).

const { jsPDF } = require('jspdf');

const DEFAULT_LAYOUT = {
  template: 'classic',
  eventName: 'Tukpa Game',
  showWatermark: false,
  watermarkText: '',
};

// Kept in lockstep with src/lib/pdfRenderer.ts in the tukpamaster repo —
// one shared 6-color-per-sheet palette (pink/cyan/green/purple/red/orange)
// matching the platform's original reference ticket design.
const TICKET_COLORS = [
  [219,  48, 130], // pink
  [  8, 145, 178], // cyan
  [ 34, 139,  34], // green
  [112,  48, 160], // purple
  [192,   0,   0], // red
  [215,  90,   0], // orange
];

function drawGrid(doc, gridX, gridY, contentW, colW, rowH, gridH, r, g, b) {
  doc.setDrawColor(r, g, b);
  doc.setLineWidth(0.55);
  doc.rect(gridX, gridY, contentW, gridH);
  for (let row = 1; row < 3; row++)
    doc.line(gridX, gridY + row * rowH, gridX + contentW, gridY + row * rowH);
  for (let col = 1; col < 9; col++)
    doc.line(gridX + col * colW, gridY, gridX + col * colW, gridY + gridH);
}

function drawNumbers(doc, ticket, gridX, gridY, colW, rowH, fontSize = 16) {
  doc.setFontSize(fontSize);
  doc.setFont('helvetica', 'bold');
  doc.setTextColor(0, 0, 0);
  for (let row = 0; row < 3; row++) {
    for (let col = 0; col < 9; col++) {
      const num = ticket.numbers[row][col];
      if (num !== null)
        doc.text(String(num), gridX + col * colW + colW / 2, gridY + row * rowH + rowH * 0.64, { align: 'center' });
    }
  }
}

function renderClassicSheet(doc, sheet, config) {
  const pageW = 210, mL = 12;
  const cW = pageW - mL * 2, colW = cW / 9;
  const rowH = 10, gridH = 30, blockH = 43;
  const sheetNum = sheet.n;
  const firstTicketNum = (sheetNum - 1) * 6 + 1;

  doc.setFontSize(20); doc.setFont('helvetica', 'bold'); doc.setTextColor(0, 0, 0);
  doc.text(config.eventName, pageW / 2, 14, { align: 'center' });

  let yPos = 22;
  for (let i = 0; i < sheet.tickets.length; i++) {
    const ticket = sheet.tickets[i];
    const [r, g, b] = TICKET_COLORS[i % 6];
    const ticketNum = String(firstTicketNum + i).padStart(3, '0');

    doc.setFontSize(9); doc.setFont('helvetica', 'normal'); doc.setTextColor(90, 90, 90);
    doc.text(ticketNum, mL + cW, yPos + 3.5, { align: 'right' });

    const gridY = yPos + 5;
    drawGrid(doc, mL, gridY, cW, colW, rowH, gridH, r, g, b);
    drawNumbers(doc, ticket, mL, gridY, colW, rowH, 16);

    const fY = gridY + gridH + 4.5;
    if (config.showWatermark && config.watermarkText) {
      doc.setFontSize(7); doc.setFont('helvetica', 'normal'); doc.setTextColor(160, 160, 160);
      doc.text(config.watermarkText, mL, fY);
    }
    doc.setFontSize(10); doc.setFont('helvetica', 'bold'); doc.setTextColor(0, 0, 0);
    doc.text(config.eventName, pageW / 2, fY, { align: 'center' });

    yPos += blockH;
  }
  doc.setFontSize(13); doc.setFont('helvetica', 'bold'); doc.setTextColor(0, 0, 0);
  doc.text(`Sheet No. ${sheetNum}`, pageW / 2, 291, { align: 'center' });
}

function renderCompactSheet(doc, sheet, config) {
  const pageW = 210, pageH = 297, sbW = 18;
  const cellW = 15, cellH = 13;
  const gridW = 9 * cellW, gridH = 3 * cellH;
  const gL = sbW + (pageW - sbW - gridW) / 2;
  const gCx = gL + gridW / 2;
  // blockH=48 with startY=6 used to leave only ~1mm before the fixed footer
  // position below — the 6th ticket's grid and "Sheet No. N" footer text
  // visibly collided on a full 6-ticket sheet. 47/5 leaves real clearance.
  const blockH = 47, startY = 5;
  const sheetNum = sheet.n;
  const firstTicketNum = (sheetNum - 1) * 6 + 1;

  doc.setFillColor(243, 244, 246);
  doc.rect(0, 0, sbW, pageH, 'F');
  doc.setFontSize(14); doc.setFont('helvetica', 'bold'); doc.setTextColor(80, 80, 80);
  doc.text(`Sheet No. ${sheetNum}`, sbW / 2, pageH / 2, { angle: 90, align: 'center' });

  doc.setFontSize(13); doc.setFont('helvetica', 'bold'); doc.setTextColor(0, 0, 0);
  doc.text(`Sheet No. ${sheetNum}`, pageW / 2, pageH - 4, { align: 'center' });

  let yPos = startY;
  for (let i = 0; i < sheet.tickets.length; i++) {
    const ticket = sheet.tickets[i];
    const [r, g, b] = TICKET_COLORS[i % 6];
    const ticketNum = String(firstTicketNum + i).padStart(3, '0');

    doc.setFontSize(11); doc.setFont('helvetica', 'bold'); doc.setTextColor(0, 0, 0);
    doc.text(config.eventName, gCx, yPos + 5, { align: 'center' });

    doc.setFontSize(9); doc.setFont('helvetica', 'normal'); doc.setTextColor(90, 90, 90);
    doc.text(ticketNum, gL + gridW, yPos + 5, { align: 'right' });

    const gridY = yPos + 7;
    drawGrid(doc, gL, gridY, gridW, cellW, cellH, gridH, r, g, b);
    drawNumbers(doc, ticket, gL, gridY, cellW, cellH, 18);

    yPos += blockH;
  }
}

function renderSheet(doc, sheet, config) {
  if (config.template === 'classic') renderClassicSheet(doc, sheet, config);
  else renderCompactSheet(doc, sheet, config);
}

// Builds one PDF containing every sheet passed in (one page per sheet) —
// matches buildBulkPDF in the client renderer.
function buildBulkPDF(sheets, config) {
  const cfg = { ...DEFAULT_LAYOUT, ...config };
  const doc = new jsPDF({ unit: 'mm', format: 'a4' });
  sheets.forEach((sheet, i) => {
    if (i > 0) doc.addPage();
    renderSheet(doc, sheet, cfg);
  });
  return doc;
}

module.exports = { buildBulkPDF, DEFAULT_LAYOUT };
