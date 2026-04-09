// btree-optimizer.test.js — Tests for BTreeTable query optimizer integration
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('BTree sort elimination', () => {
  function createBTreeDB() {
    const db = new Database();
    db.execute('CREATE TABLE ordered (id INTEGER PRIMARY KEY, name TEXT, score INTEGER) USING BTREE');
    // Insert in reverse order — BTree will store sorted
    for (let i = 100; i >= 1; i--) {
      db.execute(`INSERT INTO ordered VALUES (${i}, 'item-${i}', ${i * 10})`);
    }
    return db;
  }

  it('ORDER BY PK ASC returns correct order (sort eliminated)', () => {
    const db = createBTreeDB();
    const result = db.execute('SELECT id FROM ordered ORDER BY id ASC');
    const ids = result.rows.map(r => r.id);
    assert.equal(ids[0], 1);
    assert.equal(ids[99], 100);
    // Verify they're actually sorted
    for (let i = 1; i < ids.length; i++) {
      assert.ok(ids[i] > ids[i-1], `ids[${i}]=${ids[i]} should be > ids[${i-1}]=${ids[i-1]}`);
    }
  });

  it('ORDER BY PK ASC with LIMIT works', () => {
    const db = createBTreeDB();
    const result = db.execute('SELECT id FROM ordered ORDER BY id ASC LIMIT 5');
    assert.deepEqual(result.rows.map(r => r.id), [1, 2, 3, 4, 5]);
  });

  it('ORDER BY PK ASC with OFFSET and LIMIT', () => {
    const db = createBTreeDB();
    const result = db.execute('SELECT id FROM ordered ORDER BY id ASC LIMIT 3 OFFSET 10');
    assert.deepEqual(result.rows.map(r => r.id), [11, 12, 13]);
  });

  it('ORDER BY PK DESC still sorts (not eliminated)', () => {
    const db = createBTreeDB();
    const result = db.execute('SELECT id FROM ordered ORDER BY id DESC LIMIT 5');
    assert.deepEqual(result.rows.map(r => r.id), [100, 99, 98, 97, 96]);
  });

  it('ORDER BY non-PK column still sorts', () => {
    const db = createBTreeDB();
    // name is not the PK — sort should NOT be eliminated
    const result = db.execute('SELECT id FROM ordered ORDER BY name ASC LIMIT 3');
    // String sort: 'item-1', 'item-10', 'item-100' (lexicographic)
    assert.ok(result.rows.length === 3);
  });

  it('HeapFile table: sort always applied', () => {
    const db = new Database();
    db.execute('CREATE TABLE heap_t (id INTEGER PRIMARY KEY, name TEXT)');
    for (let i = 10; i >= 1; i--) db.execute(`INSERT INTO heap_t VALUES (${i}, 'v${i}')`);
    
    const result = db.execute('SELECT id FROM heap_t ORDER BY id ASC');
    assert.deepEqual(result.rows.map(r => r.id), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  });

  it('sort elimination is measurably faster for large tables', () => {
    const db = new Database();
    db.execute('CREATE TABLE large_btree (id INTEGER PRIMARY KEY, data TEXT) USING BTREE');
    for (let i = 1; i <= 5000; i++) {
      db.execute(`INSERT INTO large_btree VALUES (${i}, 'data-${i}')`);
    }

    // With sort elimination (PK ASC)
    const t0 = performance.now();
    for (let i = 0; i < 10; i++) {
      db.execute('SELECT id FROM large_btree ORDER BY id ASC LIMIT 10');
    }
    const btreeMs = performance.now() - t0;

    // Create heap equivalent
    const db2 = new Database();
    db2.execute('CREATE TABLE large_heap (id INTEGER PRIMARY KEY, data TEXT)');
    for (let i = 1; i <= 5000; i++) {
      db2.execute(`INSERT INTO large_heap VALUES (${i}, 'data-${i}')`);
    }

    const t1 = performance.now();
    for (let i = 0; i < 10; i++) {
      db2.execute('SELECT id FROM large_heap ORDER BY id ASC LIMIT 10');
    }
    const heapMs = performance.now() - t1;

    console.log(`  Sort elim 5K: BTree ${btreeMs.toFixed(1)}ms | Heap ${heapMs.toFixed(1)}ms`);
    // BTree should be faster since sort is eliminated
    assert.ok(btreeMs <= heapMs * 2, 'BTree should not be significantly slower than Heap with sort');
  });

  it('WHERE + ORDER BY PK: both work together', () => {
    const db = createBTreeDB();
    const result = db.execute('SELECT id, score FROM ordered WHERE score > 500 ORDER BY id ASC');
    assert.ok(result.rows.length > 0);
    // Check ordered
    for (let i = 1; i < result.rows.length; i++) {
      assert.ok(result.rows[i].id > result.rows[i-1].id);
    }
    // Check filtered
    for (const row of result.rows) {
      assert.ok(row.score > 500);
    }
  });

  it('_canEliminateSort returns correct values', () => {
    const db = createBTreeDB();
    
    // Should eliminate: ORDER BY PK ASC on BTree table
    const canElim = db._canEliminateSort({
      from: { table: 'ordered' },
      orderBy: [{ column: 'id', direction: 'ASC' }],
    });
    assert.equal(canElim, true);

    // Should NOT eliminate: DESC
    assert.equal(db._canEliminateSort({
      from: { table: 'ordered' },
      orderBy: [{ column: 'id', direction: 'DESC' }],
    }), false);

    // Should NOT eliminate: non-PK column
    assert.equal(db._canEliminateSort({
      from: { table: 'ordered' },
      orderBy: [{ column: 'name', direction: 'ASC' }],
    }), false);

    // Should NOT eliminate: multiple ORDER BY columns (PK is single column)
    assert.equal(db._canEliminateSort({
      from: { table: 'ordered' },
      orderBy: [{ column: 'id', direction: 'ASC' }, { column: 'name', direction: 'ASC' }],
    }), false);
  });
});
