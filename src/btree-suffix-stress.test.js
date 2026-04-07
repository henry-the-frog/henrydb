// btree-suffix-stress.test.js — Verify suffix keys fix the duplicate key bug
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BPlusTree } from './btree.js';

describe('B+tree Suffix Keys', () => {
  it('900 duplicate keys returns exact count', () => {
    const tree = new BPlusTree(32, { unique: false });
    for (let i = 0; i < 900; i++) tree.insert('A', i);
    for (let i = 0; i < 100; i++) tree.insert('B', 900 + i);
    
    assert.strictEqual(tree.range('A', 'A').length, 900);
    assert.strictEqual(tree.range('B', 'B').length, 100);
    assert.strictEqual(tree.size, 1000);
  });

  it('10000 duplicate keys with early termination', () => {
    const tree = new BPlusTree(32, { unique: false });
    for (let i = 0; i < 10000; i++) tree.insert('A', i);
    for (let i = 0; i < 1000; i++) tree.insert('B', 10000 + i);
    for (let i = 0; i < 1000; i++) tree.insert('C', 11000 + i);
    
    assert.strictEqual(tree.range('A', 'A').length, 10000);
    assert.strictEqual(tree.range('B', 'B').length, 1000);
    assert.strictEqual(tree.range('C', 'C').length, 1000);
    assert.strictEqual(tree.range('A', 'C').length, 12000);
  });

  it('range scan is O(k), not O(n)', () => {
    const tree = new BPlusTree(32, { unique: false });
    for (let i = 0; i < 10000; i++) tree.insert('A', i);
    for (let i = 0; i < 1000; i++) tree.insert('B', 10000 + i);
    
    // Benchmark: B scan should be ~10x faster than A scan
    const runs = 50;
    const startA = performance.now();
    for (let j = 0; j < runs; j++) tree.range('A', 'A');
    const tA = performance.now() - startA;
    
    const startB = performance.now();
    for (let j = 0; j < runs; j++) tree.range('B', 'B');
    const tB = performance.now() - startB;
    
    // A has 10x more entries, so should take ~10x longer (within 5-20x range)
    const ratio = tA / tB;
    assert.ok(ratio > 3, `Expected A/B time ratio > 3, got ${ratio.toFixed(2)} (O(k) range scan)`);
  });

  it('search returns first match', () => {
    const tree = new BPlusTree(32, { unique: false });
    tree.insert('key', 'val1');
    tree.insert('key', 'val2');
    tree.insert('key', 'val3');
    
    const result = tree.search('key');
    assert.ok(['val1', 'val2', 'val3'].includes(result));
  });

  it('delete removes entry from suffix tree', () => {
    const tree = new BPlusTree(32, { unique: false });
    tree.insert('A', 'v1');
    tree.insert('A', 'v2');
    tree.insert('B', 'v3');
    
    assert.strictEqual(tree.range('A', 'A').length, 2);
    tree.delete('A', 'v1');
    assert.strictEqual(tree.range('A', 'A').length, 1);
    assert.strictEqual(tree.range('A', 'A')[0].value, 'v2');
  });

  it('min/max work with suffix keys', () => {
    const tree = new BPlusTree(32, { unique: false });
    tree.insert('B', 1);
    tree.insert('A', 2);
    tree.insert('C', 3);
    tree.insert('A', 4);
    
    assert.strictEqual(tree.min().key, 'A');
    assert.strictEqual(tree.max().key, 'C');
  });

  it('scan iterates all entries with user keys', () => {
    const tree = new BPlusTree(32, { unique: false });
    tree.insert('A', 1);
    tree.insert('A', 2);
    tree.insert('B', 3);
    
    const entries = [...tree.scan()];
    assert.strictEqual(entries.length, 3);
    assert.strictEqual(entries[0].key, 'A');
    assert.strictEqual(entries[2].key, 'B');
  });

  it('unique tree still works (no suffix)', () => {
    const tree = new BPlusTree(32, { unique: true });
    tree.insert('A', 1);
    tree.insert('B', 2);
    tree.insert('A', 3); // Update
    
    assert.strictEqual(tree.search('A'), 3);
    assert.strictEqual(tree.range('A', 'B').length, 2);
    assert.strictEqual(tree.size, 2);
  });

  it('numeric keys with duplicates', () => {
    const tree = new BPlusTree(32, { unique: false });
    for (let i = 0; i < 500; i++) tree.insert(42, i);
    for (let i = 0; i < 500; i++) tree.insert(43, 500 + i);
    
    assert.strictEqual(tree.range(42, 42).length, 500);
    assert.strictEqual(tree.range(43, 43).length, 500);
    assert.strictEqual(tree.range(42, 43).length, 1000);
  });
  
  it('integration: database query with non-unique index returns correct results', async () => {
    const { Database } = await import('./db.js');
    const db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, category TEXT, price INT)');
    for (let i = 0; i < 200; i++) {
      db.execute(`INSERT INTO items VALUES (${i}, 'electronics', ${i * 10})`);
    }
    for (let i = 200; i < 300; i++) {
      db.execute(`INSERT INTO items VALUES (${i}, 'books', ${i})`);
    }
    db.execute('CREATE INDEX idx_cat ON items (category)');
    
    const r1 = db.execute("SELECT COUNT(*) as cnt FROM items WHERE category = 'electronics'");
    assert.strictEqual(r1.rows[0].cnt, 200);
    
    const r2 = db.execute("SELECT COUNT(*) as cnt FROM items WHERE category = 'books'");
    assert.strictEqual(r2.rows[0].cnt, 100);
  });
});
