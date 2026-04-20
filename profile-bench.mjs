import { Database } from './src/db.js';

const db = new Database();

// Create a 10K-row table
db.execute('CREATE TABLE big (id INT PRIMARY KEY, val INT, cat TEXT, amount REAL)');
const cats = ['A','B','C','D','E'];
for (let i = 1; i <= 10000; i++) {
  db.execute(`INSERT INTO big VALUES (${i}, ${i*7 % 1000}, '${cats[i%5]}', ${(Math.random()*1000).toFixed(2)})`);
}
console.log('10K rows loaded');

// Create a second table for joins
db.execute('CREATE TABLE small (id INT PRIMARY KEY, big_id INT, label TEXT)');
for (let i = 1; i <= 1000; i++) {
  db.execute(`INSERT INTO small VALUES (${i}, ${1 + i % 10000}, 'label${i}')`);
}
console.log('1K rows loaded');

// Profile various operations
const tests = [
  { name: 'Full scan', sql: 'SELECT COUNT(*) FROM big' },
  { name: 'Filtered scan', sql: "SELECT COUNT(*) FROM big WHERE cat = 'A'" },
  { name: 'Aggregate', sql: 'SELECT cat, SUM(amount), AVG(amount), COUNT(*) FROM big GROUP BY cat' },
  { name: 'ORDER BY', sql: 'SELECT * FROM big ORDER BY amount DESC LIMIT 100' },
  { name: 'Window function', sql: 'SELECT id, amount, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY amount DESC) as rn FROM big LIMIT 100' },
  { name: 'Subquery IN', sql: 'SELECT COUNT(*) FROM big WHERE id IN (SELECT big_id FROM small)' },
  { name: 'Correlated EXISTS', sql: "SELECT COUNT(*) FROM big WHERE EXISTS (SELECT 1 FROM small WHERE small.big_id = big.id)" },
  { name: 'Self-join (NL)', sql: 'SELECT COUNT(*) FROM big a JOIN big b ON a.val = b.val' },
  { name: 'Cross-table join', sql: 'SELECT COUNT(*) FROM big JOIN small ON big.id = small.big_id' },
  { name: 'Complex aggregate', sql: 'SELECT cat, COUNT(*), SUM(amount), AVG(val), MIN(amount), MAX(amount) FROM big GROUP BY cat ORDER BY cat' },
];

for (const t of tests) {
  const runs = [];
  for (let r = 0; r < 3; r++) {
    const t0 = performance.now();
    try {
      const result = db.execute(t.sql);
      const ms = performance.now() - t0;
      runs.push(ms);
    } catch (e) {
      console.log(`❌ ${t.name}: ${e.message.split('\n')[0]}`);
      break;
    }
  }
  if (runs.length) {
    const avg = runs.reduce((a,b)=>a+b,0) / runs.length;
    const min = Math.min(...runs);
    console.log(`${t.name}: avg=${avg.toFixed(0)}ms, min=${min.toFixed(0)}ms (${runs.map(r=>r.toFixed(0)).join(',')}ms)`);
  }
}

// Now profile with Node's built-in profiler info
console.log('\n--- Detailed profiling for JOIN ---');
const t0 = performance.now();
const r = db.execute('SELECT big.cat, COUNT(*) FROM big JOIN small ON big.id = small.big_id GROUP BY big.cat');
console.log(`JOIN + GROUP BY: ${(performance.now()-t0).toFixed(0)}ms`);
console.log('Result:', JSON.stringify(r.rows));
