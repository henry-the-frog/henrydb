// table-stats.test.js — Tests for table statistics and ANALYZE
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { analyzeTable, estimateSelectivity } from './table-stats.js';
import { Database } from './db.js';
import { HeapFile } from './page.js';

describe('analyzeTable', () => {
  it('collects basic stats from heap', () => {
    const heap = new HeapFile('test');
    heap.insert([1, 'Alice', 30]);
    heap.insert([2, 'Bob', 25]);
    heap.insert([3, 'Charlie', 35]);
    heap.insert([4, 'Dave', null]);
    
    const schema = [
      { name: 'id', type: 'INTEGER' },
      { name: 'name', type: 'TEXT' },
      { name: 'age', type: 'INTEGER' },
    ];
    
    const stats = analyzeTable({ schema, heap });
    
    assert.equal(stats.rowCount, 4);
    
    const idStats = stats.columnStats.get('id');
    assert.equal(idStats.distinctCount, 4);
    assert.equal(idStats.min, 1);
    assert.equal(idStats.max, 4);
    assert.equal(idStats.nullCount, 0);
    
    const nameStats = stats.columnStats.get('name');
    assert.equal(nameStats.distinctCount, 4);
    assert.equal(nameStats.min, 'Alice');
    assert.equal(nameStats.max, 'Dave');
    
    const ageStats = stats.columnStats.get('age');
    assert.equal(ageStats.nullCount, 1);
    assert.equal(ageStats.distinctCount, 3);
    assert.ok(ageStats.nullFraction > 0);
  });

  it('most common values (MCV)', () => {
    const heap = new HeapFile('test');
    for (let i = 0; i < 100; i++) {
      const cat = i < 50 ? 'A' : (i < 80 ? 'B' : 'C');
      heap.insert([i, cat]);
    }
    
    const schema = [
      { name: 'id', type: 'INTEGER' },
      { name: 'category', type: 'TEXT' },
    ];
    
    const stats = analyzeTable({ schema, heap });
    const catStats = stats.columnStats.get('category');
    
    assert.equal(catStats.mcv.length, 3);
    assert.equal(catStats.mcv[0].value, 'A');
    assert.ok(catStats.mcv[0].frequency > 0.4);
  });

  it('histogram for numeric column', () => {
    const heap = new HeapFile('test');
    for (let i = 1; i <= 1000; i++) {
      heap.insert([i, i * 2]);
    }
    
    const schema = [
      { name: 'id', type: 'INTEGER' },
      { name: 'val', type: 'INTEGER' },
    ];
    
    const stats = analyzeTable({ schema, heap }, { histogramBuckets: 10 });
    const valStats = stats.columnStats.get('val');
    
    assert.ok(valStats.histogram.length > 0);
    assert.equal(valStats.histogram[0], 2); // min val
    assert.equal(valStats.histogram[valStats.histogram.length - 1], 2000); // max val
  });
});

describe('estimateSelectivity', () => {
  function getStats() {
    const heap = new HeapFile('test');
    for (let i = 1; i <= 100; i++) {
      const cat = i <= 50 ? 'A' : (i <= 80 ? 'B' : 'C');
      heap.insert([i, cat]);
    }
    const schema = [
      { name: 'id', type: 'INTEGER' },
      { name: 'category', type: 'TEXT' },
    ];
    return analyzeTable({ schema, heap });
  }

  it('equality on ID: ~1%', () => {
    const stats = getStats();
    const sel = estimateSelectivity(stats.columnStats.get('id'), 'EQ', 50);
    assert.ok(sel < 0.05, `Expected <5%, got ${(sel*100).toFixed(1)}%`);
  });

  it('equality on category MCV: matches frequency', () => {
    const stats = getStats();
    const sel = estimateSelectivity(stats.columnStats.get('category'), 'EQ', 'A');
    assert.ok(sel > 0.3, `Expected >30% for A, got ${(sel*100).toFixed(1)}%`);
  });

  it('less-than on numeric column', () => {
    const stats = getStats();
    const sel = estimateSelectivity(stats.columnStats.get('id'), 'LT', 50);
    assert.ok(sel > 0.3 && sel < 0.7, `Expected ~50%, got ${(sel*100).toFixed(1)}%`);
  });

  it('greater-than on numeric column', () => {
    const stats = getStats();
    const sel = estimateSelectivity(stats.columnStats.get('id'), 'GT', 50);
    assert.ok(sel > 0.3 && sel < 0.7, `Expected ~50%, got ${(sel*100).toFixed(1)}%`);
  });

  it('not-equal selectivity', () => {
    const stats = getStats();
    const sel = estimateSelectivity(stats.columnStats.get('id'), 'NEQ', 50);
    assert.ok(sel > 0.9, `Expected >90%, got ${(sel*100).toFixed(1)}%`);
  });

  it('no stats returns 50% default', () => {
    const sel = estimateSelectivity(null, 'EQ', 'anything');
    assert.equal(sel, 0.5);
  });
});

describe('ANALYZE SQL command', () => {
  it('ANALYZE collects stats for all tables', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE t2 (id INTEGER PRIMARY KEY, val INTEGER)');
    for (let i = 1; i <= 10; i++) {
      db.execute(`INSERT INTO t1 VALUES (${i}, 'name-${i}')`);
      db.execute(`INSERT INTO t2 VALUES (${i}, ${i * 10})`);
    }
    
    const result = db.execute('ANALYZE');
    assert.ok(result.message.includes('2 table'));
  });

  it('ANALYZE specific table', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO users VALUES (${i}, 'user${i}@example.com')`);
    
    const result = db.execute('ANALYZE users');
    assert.ok(result.message.includes('users'));
    assert.ok(result.message.includes('50 rows'));
  });

  it('ANALYZE returns column stats', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, price INTEGER)');
    for (let i = 1; i <= 100; i++) {
      const cat = ['electronics', 'books', 'clothing'][i % 3];
      db.execute(`INSERT INTO products VALUES (${i}, '${cat}', ${i * 10})`);
    }
    
    const result = db.execute('ANALYZE products');
    assert.ok(result.tables);
    assert.equal(result.tables[0].rows, 100);
    
    const cols = result.tables[0].columns;
    const idCol = cols.find(c => c.name === 'id');
    assert.equal(idCol.ndv, 100); // 100 distinct IDs
    assert.equal(idCol.min, 1);
    assert.equal(idCol.max, 100);
    
    const catCol = cols.find(c => c.name === 'category');
    assert.ok(catCol.ndv <= 3); // 3 categories
  });
});
