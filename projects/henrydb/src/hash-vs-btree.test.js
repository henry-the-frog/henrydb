// hash-vs-btree.test.js — Benchmark: B+tree index vs Hash index
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function setupDB(indexType) {
  const db = new Database();
  db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT, category TEXT, price INTEGER)');
  for (let i = 1; i <= 10000; i++) {
    const cat = ['electronics', 'books', 'clothing', 'food', 'toys'][i % 5];
    db.execute(`INSERT INTO items VALUES (${i}, 'CODE-${i}', '${cat}', ${i * 3})`);
  }
  db.execute(`CREATE INDEX idx_code ON items ${indexType === 'hash' ? 'USING HASH' : ''} (code)`);
  return db;
}

describe('B+tree vs Hash index benchmarks', () => {
  it('equality lookup: 1000 random lookups in 10K rows', () => {
    const dbBtree = setupDB('btree');
    const dbHash = setupDB('hash');

    // B+tree
    const t0 = performance.now();
    for (let i = 0; i < 1000; i++) {
      const code = `CODE-${Math.floor(Math.random() * 10000) + 1}`;
      dbBtree.execute(`SELECT * FROM items WHERE code = '${code}'`);
    }
    const btreeMs = performance.now() - t0;

    // Hash
    const t1 = performance.now();
    for (let i = 0; i < 1000; i++) {
      const code = `CODE-${Math.floor(Math.random() * 10000) + 1}`;
      dbHash.execute(`SELECT * FROM items WHERE code = '${code}'`);
    }
    const hashMs = performance.now() - t1;

    console.log(`  Equality 1K in 10K: B+tree ${btreeMs.toFixed(1)}ms | Hash ${hashMs.toFixed(1)}ms | ratio ${(btreeMs/hashMs).toFixed(2)}x`);
    assert.ok(true);
  });

  it('range scan: B+tree supports it, Hash does not', () => {
    const db = new Database();
    db.execute('CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)');
    for (let i = 1; i <= 1000; i++) db.execute(`INSERT INTO nums VALUES (${i}, ${i})`);
    
    db.execute('CREATE INDEX idx_val ON nums (val)');
    
    // Range query works with B+tree index
    const result = db.execute('SELECT COUNT(*) as cnt FROM nums WHERE val >= 100 AND val <= 200');
    assert.equal(result.rows[0].cnt, 101);
    
    // Hash index falls back to full scan for range queries
    const db2 = new Database();
    db2.execute('CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)');
    for (let i = 1; i <= 1000; i++) db2.execute(`INSERT INTO nums VALUES (${i}, ${i})`);
    db2.execute('CREATE INDEX idx_val ON nums USING HASH (val)');
    
    const result2 = db2.execute('SELECT COUNT(*) as cnt FROM nums WHERE val >= 100 AND val <= 200');
    assert.equal(result2.rows[0].cnt, 101); // Still correct via full scan
  });

  it('index creation time: 10K rows', () => {
    const db1 = new Database();
    db1.execute('CREATE TABLE t1 (id INTEGER PRIMARY KEY, code TEXT)');
    for (let i = 1; i <= 10000; i++) db1.execute(`INSERT INTO t1 VALUES (${i}, 'C-${i}')`);

    const db2 = new Database();
    db2.execute('CREATE TABLE t2 (id INTEGER PRIMARY KEY, code TEXT)');
    for (let i = 1; i <= 10000; i++) db2.execute(`INSERT INTO t2 VALUES (${i}, 'C-${i}')`);

    const t0 = performance.now();
    db1.execute('CREATE INDEX idx1 ON t1 (code)');
    const btreeMs = performance.now() - t0;

    const t1 = performance.now();
    db2.execute('CREATE INDEX idx2 ON t2 USING HASH (code)');
    const hashMs = performance.now() - t1;

    console.log(`  Index creation 10K: B+tree ${btreeMs.toFixed(1)}ms | Hash ${hashMs.toFixed(1)}ms | ratio ${(btreeMs/hashMs).toFixed(2)}x`);
  });

  it('summary: when to use which', () => {
    console.log('\n  ╔══════════════════════════════════════════════════╗');
    console.log('  ║  B+tree vs Hash Index — When to Use Which        ║');
    console.log('  ╠══════════════════════════════════════════════════╣');
    console.log('  ║  Use HASH when:                                   ║');
    console.log('  ║    • Only equality lookups (WHERE col = value)    ║');
    console.log('  ║    • High-cardinality columns (unique IDs, codes) ║');
    console.log('  ║    • O(1) lookup is critical                      ║');
    console.log('  ║  Use BTREE when:                                  ║');
    console.log('  ║    • Range queries (BETWEEN, >, <)                ║');
    console.log('  ║    • ORDER BY optimization needed                 ║');
    console.log('  ║    • Prefix matching (LIKE \'abc%\')                ║');
    console.log('  ║    • Default choice when unsure                   ║');
    console.log('  ╚══════════════════════════════════════════════════╝');
    assert.ok(true);
  });
});
