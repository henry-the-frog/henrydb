// btree-engine.test.js — Integration test: BTreeTable as storage engine in Database
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('BTreeTable engine integration', () => {
  it('CREATE TABLE ... USING BTREE stores rows sorted by PK', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER) USING BTREE');
    
    // Insert out of order
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
    
    const result = db.execute('SELECT * FROM users');
    assert.equal(result.rows.length, 3);
    // B-tree table returns rows in PK order
    assert.equal(result.rows[0].id, 1);
    assert.equal(result.rows[1].id, 2);
    assert.equal(result.rows[2].id, 3);
    assert.equal(result.rows[0].name, 'Alice');
  });

  it('default CREATE TABLE uses HeapFile (insertion order)', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    
    db.execute("INSERT INTO users VALUES (3, 'Charlie')");
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("INSERT INTO users VALUES (2, 'Bob')");
    
    const result = db.execute('SELECT * FROM users');
    assert.equal(result.rows.length, 3);
    // HeapFile: insertion order
    assert.equal(result.rows[0].id, 3);
    assert.equal(result.rows[1].id, 1);
    assert.equal(result.rows[2].id, 2);
  });

  it('BTREE table supports WHERE on PK', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER) USING BTREE');
    
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO products VALUES (${i}, 'Product ${i}', ${i * 10})`);
    }
    
    const result = db.execute('SELECT * FROM products WHERE id = 50');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].id, 50);
    assert.equal(result.rows[0].name, 'Product 50');
  });

  it('BTREE table supports UPDATE and DELETE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT) USING BTREE');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (2, 'b')");
    db.execute("INSERT INTO t VALUES (3, 'c')");
    
    db.execute("UPDATE t SET val = 'B' WHERE id = 2");
    const updated = db.execute('SELECT * FROM t WHERE id = 2');
    assert.equal(updated.rows[0].val, 'B');
    
    db.execute('DELETE FROM t WHERE id = 1');
    const after = db.execute('SELECT * FROM t');
    assert.equal(after.rows.length, 2);
    assert.equal(after.rows[0].id, 2); // Sorted order
  });

  it('BTREE table with ORDER BY PK is naturally sorted', () => {
    const db = new Database();
    db.execute('CREATE TABLE sorted_data (id INTEGER PRIMARY KEY, label TEXT) USING BTREE');
    
    for (let i = 100; i >= 1; i--) {
      db.execute(`INSERT INTO sorted_data VALUES (${i}, 'label-${i}')`);
    }
    
    const result = db.execute('SELECT id FROM sorted_data ORDER BY id ASC LIMIT 5');
    assert.deepEqual(result.rows.map(r => r.id), [1, 2, 3, 4, 5]);
  });

  it('BTREE table supports aggregations', () => {
    const db = new Database();
    db.execute('CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER) USING BTREE');
    
    for (let i = 1; i <= 50; i++) {
      db.execute(`INSERT INTO nums VALUES (${i}, ${i * 2})`);
    }
    
    const result = db.execute('SELECT COUNT(*) as cnt, SUM(val) as s, MIN(val) as mn, MAX(val) as mx FROM nums');
    assert.equal(result.rows[0].cnt, 50);
    assert.equal(result.rows[0].s, 2550);
    assert.equal(result.rows[0].mn, 2);
    assert.equal(result.rows[0].mx, 100);
  });

  it('BTREE table supports JOINs', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER) USING BTREE');
    db.execute('CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT) USING BTREE');
    
    db.execute("INSERT INTO customers VALUES (1, 'Alice')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob')");
    db.execute("INSERT INTO orders VALUES (1, 1, 100)");
    db.execute("INSERT INTO orders VALUES (2, 2, 200)");
    db.execute("INSERT INTO orders VALUES (3, 1, 150)");
    
    const result = db.execute(
      'SELECT c.name, SUM(o.amount) as total FROM orders o JOIN customers c ON o.customer_id = c.id GROUP BY c.name ORDER BY c.name'
    );
    assert.equal(result.rows.length, 2);
    assert.equal(result.rows[0].name, 'Alice');
    assert.equal(result.rows[0].total, 250);
    assert.equal(result.rows[1].name, 'Bob');
    assert.equal(result.rows[1].total, 200);
  });

  it('BTREE table works with secondary indexes', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, price INTEGER) USING BTREE');
    
    for (let i = 1; i <= 50; i++) {
      const cat = i % 3 === 0 ? 'A' : i % 3 === 1 ? 'B' : 'C';
      db.execute(`INSERT INTO products VALUES (${i}, '${cat}', ${i * 10})`);
    }
    
    db.execute('CREATE INDEX idx_cat ON products (category)');
    
    const result = db.execute("SELECT COUNT(*) as cnt FROM products WHERE category = 'A'");
    assert.equal(result.rows[0].cnt, 16); // 3,6,9,...,48 = 16 items
  });

  it('BTREE table supports EXPLAIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE ex_test (id INTEGER PRIMARY KEY, val TEXT) USING BTREE');
    db.execute("INSERT INTO ex_test VALUES (1, 'a')");
    
    const result = db.execute('EXPLAIN SELECT * FROM ex_test WHERE id = 1');
    assert.ok(result);
  });

  it('BTREE engine: 1000 row stress test', () => {
    const db = new Database();
    db.execute('CREATE TABLE stress (id INTEGER PRIMARY KEY, data TEXT, num INTEGER) USING BTREE');
    
    for (let i = 1; i <= 1000; i++) {
      db.execute(`INSERT INTO stress VALUES (${i}, 'row-${i}', ${i * 3})`);
    }
    
    // Point query
    const one = db.execute('SELECT * FROM stress WHERE id = 500');
    assert.equal(one.rows.length, 1);
    assert.equal(one.rows[0].data, 'row-500');
    
    // Range query
    const range = db.execute('SELECT * FROM stress WHERE id >= 900 AND id <= 910');
    assert.equal(range.rows.length, 11);
    
    // Aggregation
    const agg = db.execute('SELECT COUNT(*) as cnt, MIN(num) as mn, MAX(num) as mx FROM stress');
    assert.equal(agg.rows[0].cnt, 1000);
    assert.equal(agg.rows[0].mn, 3);
    assert.equal(agg.rows[0].mx, 3000);
    
    // Delete + verify
    db.execute('DELETE FROM stress WHERE id > 990');
    const after = db.execute('SELECT COUNT(*) as cnt FROM stress');
    assert.equal(after.rows[0].cnt, 990);
  });

  it('USING HEAP is explicit heap', () => {
    const db = new Database();
    db.execute('CREATE TABLE heap_table (id INTEGER PRIMARY KEY, name TEXT) USING HEAP');
    db.execute("INSERT INTO heap_table VALUES (3, 'c')");
    db.execute("INSERT INTO heap_table VALUES (1, 'a')");
    
    const result = db.execute('SELECT * FROM heap_table');
    assert.equal(result.rows.length, 2);
    // HeapFile: insertion order preserved
    assert.equal(result.rows[0].id, 3);
  });

  it('BTREE table: TRUNCATE works', () => {
    const db = new Database();
    db.execute('CREATE TABLE trunc_test (id INTEGER PRIMARY KEY, val TEXT) USING BTREE');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO trunc_test VALUES (${i}, 'v${i}')`);
    
    db.execute('TRUNCATE TABLE trunc_test');
    const result = db.execute('SELECT COUNT(*) as cnt FROM trunc_test');
    assert.equal(result.rows[0].cnt, 0);
  });

  it('BTREE vs HeapFile: verify different storage engines coexist', () => {
    const db = new Database();
    db.execute('CREATE TABLE btree_t (id INTEGER PRIMARY KEY, val TEXT) USING BTREE');
    db.execute('CREATE TABLE heap_t (id INTEGER PRIMARY KEY, val TEXT)');
    
    for (let i = 5; i >= 1; i--) {
      db.execute(`INSERT INTO btree_t VALUES (${i}, 'v${i}')`);
      db.execute(`INSERT INTO heap_t VALUES (${i}, 'v${i}')`);
    }
    
    const btreeRows = db.execute('SELECT id FROM btree_t').rows.map(r => r.id);
    const heapRows = db.execute('SELECT id FROM heap_t').rows.map(r => r.id);
    
    // BTree: sorted
    assert.deepEqual(btreeRows, [1, 2, 3, 4, 5]);
    // Heap: insertion order (5,4,3,2,1)
    assert.deepEqual(heapRows, [5, 4, 3, 2, 1]);
  });
});
