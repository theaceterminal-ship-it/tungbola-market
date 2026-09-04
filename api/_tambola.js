// Server-side port of src/lib/tambola.ts in the tukpamaster repo — kept in
// exact algorithmic lockstep with that (already-tested) client logic, because
// ticket generation and claim verification for real prize money need to run
// somewhere a client can't tamper with. If you change the generation or
// verification rules, change both copies together.

const COL_RANGES = [
  [1, 9], [10, 19], [20, 29], [30, 39], [40, 49],
  [50, 59], [60, 69], [70, 79], [80, 90]
];
const COL_COUNTS = COL_RANGES.map(([min, max]) => max - min + 1); // [9,10,10,10,10,10,10,10,11]

function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function buildDistribution() {
  const colExtras = COL_COUNTS.map(c => c - 6); // [3,4,4,4,4,4,4,4,5]
  const need = new Array(6).fill(6);
  const assigned = Array.from({ length: 6 }, () => new Array(9).fill(0));

  const colOrder = shuffle([0, 1, 2, 3, 4, 5, 6, 7, 8])
    .sort((a, b) => colExtras[b] - colExtras[a]);

  for (const col of colOrder) {
    const n = colExtras[col];
    const eligible = shuffle([0, 1, 2, 3, 4, 5])
      .filter(t => need[t] > 0)
      .sort((a, b) => need[b] - need[a]);
    for (let i = 0; i < n; i++) {
      assigned[eligible[i]][col] = 1;
      need[eligible[i]]--;
    }
  }
  return Array.from({ length: 6 }, (_, t) => assigned[t].map(e => 1 + e));
}

// Generates one sheet's worth of tickets: 6 tickets, each 3x9, each row
// exactly 5 filled, columns in ascending order, all 90 numbers used once
// across the sheet. Returns [{ pos, numbers }, ...] (pos 1-6, no ids — the
// caller assigns ids/sheet numbers).
function generateTickets() {
  const dist = buildDistribution();

  const colNums = [];
  for (let col = 0; col < 9; col++) {
    const [min, max] = COL_RANGES[col];
    const all = shuffle(Array.from({ length: max - min + 1 }, (_, i) => min + i));
    const perTicket = [];
    let idx = 0;
    for (let t = 0; t < 6; t++) {
      const count = dist[t][col];
      perTicket.push(all.slice(idx, idx + count).sort((a, b) => a - b));
      idx += count;
    }
    colNums.push(perTicket);
  }

  const tickets = [];
  for (let t = 0; t < 6; t++) {
    const grid = [new Array(9).fill(null), new Array(9).fill(null), new Array(9).fill(null)];
    const doubles = [], singles = [];
    for (let col = 0; col < 9; col++) (dist[t][col] === 2 ? doubles : singles).push(col);

    const pairTemplate = [[0, 1], [0, 1], [0, 2], [0, 2], [1, 2], [1, 2]];
    const pairs = shuffle(pairTemplate);
    const dblCols = shuffle(doubles);
    for (let i = 0; i < 6; i++) {
      const col = dblCols[i];
      const [r0, r1] = pairs[i];
      grid[r0][col] = colNums[col][t][0];
      grid[r1][col] = colNums[col][t][1];
    }
    const sglCols = shuffle(singles);
    for (let i = 0; i < 3; i++) grid[i][sglCols[i]] = colNums[sglCols[i]][t][0];

    tickets.push({ pos: t + 1, numbers: grid });
  }
  return tickets;
}

function getNthNumber(row, n) {
  const nums = row.filter(x => x !== null);
  return nums[n - 1] ?? null;
}

// ticket: { pos, numbers } — same shape generateTickets() produces.
function verifyDividend(ticket, calledNumbers, dividendType) {
  const calledSet = new Set(calledNumbers);
  const grid = ticket.numbers;

  switch (dividendType) {
    case 'early-five': {
      let count = 0;
      for (const row of grid) for (const num of row) if (num !== null && calledSet.has(num)) count++;
      return count >= 5;
    }
    case 'top-line':
      return grid[0].filter(n => n !== null).every(n => calledSet.has(n));
    case 'middle-line':
      return grid[1].filter(n => n !== null).every(n => calledSet.has(n));
    case 'bottom-line':
      return grid[2].filter(n => n !== null).every(n => calledSet.has(n));
    case 'corners': {
      const topRow = grid[0].filter(n => n !== null);
      const botRow = grid[2].filter(n => n !== null);
      if (topRow.length < 2 || botRow.length < 2) return false;
      const corners = [topRow[0], topRow[topRow.length - 1], botRow[0], botRow[botRow.length - 1]];
      return corners.every(n => calledSet.has(n));
    }
    case 'full-house':
      for (const row of grid) for (const num of row) if (num !== null && !calledSet.has(num)) return false;
      return true;
    case 'center':
      return grid[1][4] !== null && calledSet.has(grid[1][4]);
    case 'pyramid': {
      const pyramidNums = [
        getNthNumber(grid[0], 3),
        getNthNumber(grid[1], 2), getNthNumber(grid[1], 4),
        getNthNumber(grid[2], 1), getNthNumber(grid[2], 3), getNthNumber(grid[2], 5),
      ];
      return pyramidNums.every(n => n !== null && calledSet.has(n));
    }
    case 'star': {
      const topNums = grid[0].filter(n => n !== null);
      const midNums = grid[1].filter(n => n !== null);
      const botNums = grid[2].filter(n => n !== null);
      const starNums = [topNums[0], topNums[2], topNums[4], ...midNums, botNums[0], botNums[2], botNums[4]];
      return starNums.every(n => n !== null && calledSet.has(n));
    }
    case 'plus': {
      const plusNums = [
        getNthNumber(grid[0], 3),
        getNthNumber(grid[1], 2), getNthNumber(grid[1], 3), getNthNumber(grid[1], 4),
        getNthNumber(grid[2], 3),
      ];
      return plusNums.every(n => n !== null && calledSet.has(n));
    }
    case 'cross': {
      const topNums = grid[0].filter(n => n !== null);
      const botNums = grid[2].filter(n => n !== null);
      const crossNums = [topNums[0], topNums[4], getNthNumber(grid[1], 3), botNums[0], botNums[4]];
      return crossNums.every(n => n !== null && calledSet.has(n));
    }
    default:
      return false;
  }
}

module.exports = { generateTickets, verifyDividend };
