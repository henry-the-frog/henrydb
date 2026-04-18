import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function q(db, sql) {
  const r = db.execute(sql);
  return r.rows || r || [];
}

describe('HOT Chains (Heap-Only Tuples)', () => {
  
  describe('basic HOT detection', () => {
    it('UPDATE on non-indexed column creates HOT chain (skips index update)', () => {
      const db = new Database();
      db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, salary INT)');
      db.execute('CREATE INDEX idx_name ON employees (name)');
      db.execute("INSERT INTO employees VALUES (1, 'Alice', 50000)");
      db.execute("INSERT INTO employees VALUES (2, 'Bob', 60000)");
      
      // Update salary (non-indexed column) — should be HOT
      db.execute('UPDATE employees SET salary = 70000 WHERE id = 1');
      
      // Verify the update worked via full scan
      const r1 = q(db, 'SELECT * FROM employees WHERE id = 1');
      assert.equal(r1[0].salary, 70000);
      
      // Verify index lookup still works (follows HOT chain)
      const byName = q(db, "SELECT * FROM employees WHERE name = 'Alice'");
      assert.equal(byName.length, 1);
      assert.equal(byName[0].salary, 70000);
      assert.equal(byName[0].id, 1);
    });

    it('UPDATE on indexed column does NOT create HOT chain (updates index normally)', () => {
      const db = new Database();
      db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, salary INT)');
      db.execute('CREATE INDEX idx_name ON employees (name)');
      db.execute("INSERT INTO employees VALUES (1, 'Alice', 50000)");
      
      // Update name (indexed column) — should NOT be HOT
      db.execute("UPDATE employees SET name = 'Alicia' WHERE id = 1");
      
      // Old name should not find anything
      const oldResult = q(db, "SELECT * FROM employees WHERE name = 'Alice'");
      assert.equal(oldResult.length, 0);
      
      // New name should find the row
      const newResult = q(db, "SELECT * FROM employees WHERE name = 'Alicia'");
      assert.equal(newResult.length, 1);
      assert.equal(newResult[0].salary, 50000);
    });

    it('multiple HOT updates chain correctly', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, data TEXT, counter INT)');
      db.execute('CREATE INDEX idx_data ON t (data)');
      db.execute("INSERT INTO t VALUES (1, 'hello', 0)");
      
      // Multiple updates to non-indexed column
      for (let i = 1; i <= 5; i++) {
        db.execute(`UPDATE t SET counter = ${i} WHERE id = 1`);
      }
      
      // Index lookup should find the latest version
      const result = q(db, "SELECT * FROM t WHERE data = 'hello'");
      assert.equal(result.length, 1);
      assert.equal(result[0].counter, 5);
    });
  });

  describe('HOT chains with multiple indexes', () => {
    it('HOT only when NO indexed column changes', () => {
      const db = new Database();
      db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, category TEXT, price INT)');
      db.execute('CREATE INDEX idx_name ON products (name)');
      db.execute('CREATE INDEX idx_category ON products (category)');
      db.execute("INSERT INTO products VALUES (1, 'Widget', 'Tools', 1000)");
      
      // Update price (not indexed) — HOT
      db.execute('UPDATE products SET price = 1200 WHERE id = 1');
      const r1 = q(db, "SELECT * FROM products WHERE name = 'Widget'");
      assert.equal(r1.length, 1);
      assert.equal(r1[0].price, 1200);
      
      const r2 = q(db, "SELECT * FROM products WHERE category = 'Tools'");
      assert.equal(r2.length, 1);
      assert.equal(r2[0].price, 1200);
    });

    it('changing one of multiple indexed columns triggers non-HOT update', () => {
      const db = new Database();
      db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, category TEXT, price INT)');
      db.execute('CREATE INDEX idx_name ON products (name)');
      db.execute('CREATE INDEX idx_cat ON products (category)');
      db.execute("INSERT INTO products VALUES (1, 'Widget', 'Tools', 1000)");
      
      // Update category (indexed) — NOT HOT
      db.execute("UPDATE products SET category = 'Hardware' WHERE id = 1");
      
      const oldCat = q(db, "SELECT * FROM products WHERE category = 'Tools'");
      assert.equal(oldCat.length, 0);
      
      const newCat = q(db, "SELECT * FROM products WHERE category = 'Hardware'");
      assert.equal(newCat.length, 1);
      assert.equal(newCat[0].name, 'Widget');
    });
  });

  describe('HOT chains with range scans', () => {
    it('range scan finds HOT-updated rows', () => {
      const db = new Database();
      db.execute('CREATE TABLE scores (id INT PRIMARY KEY, player TEXT, score INT)');
      db.execute('CREATE INDEX idx_player ON scores (player)');
      db.execute("INSERT INTO scores VALUES (1, 'Alice', 100)");
      db.execute("INSERT INTO scores VALUES (2, 'Alice', 200)");
      db.execute("INSERT INTO scores VALUES (3, 'Bob', 150)");
      
      // Update scores (non-indexed) for Alice's rows
      db.execute("UPDATE scores SET score = score + 50 WHERE player = 'Alice'");
      
      // Range scan on player index should find updated values
      const alice = q(db, "SELECT * FROM scores WHERE player = 'Alice' ORDER BY id");
      assert.equal(alice.length, 2);
      assert.equal(alice[0].score, 150);
      assert.equal(alice[1].score, 250);
    });
  });

  describe('HOT chains with table without indexes', () => {
    it('UPDATE on table with no secondary indexes works normally', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'a')");
      db.execute("UPDATE t SET val = 'b' WHERE id = 1");
      
      const result = q(db, 'SELECT * FROM t WHERE id = 1');
      assert.equal(result[0].val, 'b');
    });
  });

  describe('HOT chains with bulk updates', () => {
    it('bulk update of non-indexed column', () => {
      const db = new Database();
      db.execute('CREATE TABLE items (id INT PRIMARY KEY, category TEXT, quantity INT)');
      db.execute('CREATE INDEX idx_cat ON items (category)');
      
      for (let i = 1; i <= 20; i++) {
        const cat = i <= 10 ? 'A' : 'B';
        db.execute(`INSERT INTO items VALUES (${i}, '${cat}', ${i * 10})`);
      }
      
      // Bulk update quantity (non-indexed)
      db.execute("UPDATE items SET quantity = quantity * 2 WHERE category = 'A'");
      
      // Verify via index scan
      const catA = q(db, "SELECT * FROM items WHERE category = 'A' ORDER BY id");
      assert.equal(catA.length, 10);
      assert.equal(catA[0].quantity, 20); // 10 * 2
      assert.equal(catA[9].quantity, 200); // 100 * 2
      
      const catB = q(db, "SELECT * FROM items WHERE category = 'B' ORDER BY id");
      assert.equal(catB.length, 10);
      assert.equal(catB[0].quantity, 110); // unchanged
    });
  });

  describe('HOT chains with joins using index', () => {
    it('join using indexed column finds HOT-updated rows', () => {
      const db = new Database();
      db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, total INT)');
      db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)');
      db.execute('CREATE INDEX idx_cust ON orders (customer_id)');
      
      db.execute("INSERT INTO customers VALUES (1, 'Alice')");
      db.execute("INSERT INTO customers VALUES (2, 'Bob')");
      db.execute('INSERT INTO orders VALUES (1, 1, 100)');
      db.execute('INSERT INTO orders VALUES (2, 1, 200)');
      db.execute('INSERT INTO orders VALUES (3, 2, 150)');
      
      // Update total (non-indexed) — HOT
      db.execute('UPDATE orders SET total = 300 WHERE id = 1');
      
      // The join should find updated values
      const result = q(db, `
        SELECT c.name, o.total 
        FROM customers c 
        JOIN orders o ON c.id = o.customer_id 
        WHERE c.id = 1 
        ORDER BY o.total
      `);
      assert.equal(result.length, 2);
      const totals = result.map(r => r.total).sort((a, b) => a - b);
      assert.deepEqual(totals, [200, 300]);
    });
  });

  describe('HOT chain HeapFile API', () => {
    it('HeapFile tracks HOT chains correctly', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)');
      const table = db.tables.get('t');
      const heap = table.heap;
      
      if (heap.addHotChain) {
        heap.addHotChain(0, 0, 0, 1);
        assert.equal(heap.hasHotChain(0, 0), true);
        assert.equal(heap.hasHotChain(0, 1), false);
        
        const latest = heap.followHotChain(0, 0);
        assert.equal(latest.pageId, 0);
        assert.equal(latest.slotIdx, 1);
        
        // Multi-hop chain: 0:0 → 0:1 → 0:2
        heap.addHotChain(0, 1, 0, 2);
        const latest2 = heap.followHotChain(0, 0);
        assert.equal(latest2.pageId, 0);
        assert.equal(latest2.slotIdx, 2);
        
        heap.removeHotChain(0, 0);
        assert.equal(heap.hasHotChain(0, 0), false);
        
        // No chain returns original
        const same = heap.followHotChain(5, 5);
        assert.equal(same.pageId, 5);
        assert.equal(same.slotIdx, 5);
      }
    });
  });

  describe('HOT chains with mixed updates', () => {
    it('alternating HOT and non-HOT updates work correctly', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, indexed_col TEXT, data INT)');
      db.execute('CREATE INDEX idx ON t (indexed_col)');
      db.execute("INSERT INTO t VALUES (1, 'x', 0)");
      
      // HOT update (only data changes)
      db.execute('UPDATE t SET data = 1 WHERE id = 1');
      let r = q(db, "SELECT * FROM t WHERE indexed_col = 'x'");
      assert.equal(r.length, 1);
      assert.equal(r[0].data, 1);
      
      // Non-HOT update (indexed_col changes)
      db.execute("UPDATE t SET indexed_col = 'y' WHERE id = 1");
      r = q(db, "SELECT * FROM t WHERE indexed_col = 'y'");
      assert.equal(r.length, 1);
      assert.equal(r[0].data, 1);
      
      r = q(db, "SELECT * FROM t WHERE indexed_col = 'x'");
      assert.equal(r.length, 0);
      
      // HOT update again
      db.execute('UPDATE t SET data = 2 WHERE id = 1');
      r = q(db, "SELECT * FROM t WHERE indexed_col = 'y'");
      assert.equal(r.length, 1);
      assert.equal(r[0].data, 2);
    });

    it('DELETE after HOT update works', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)');
      db.execute('CREATE INDEX idx ON t (name)');
      db.execute("INSERT INTO t VALUES (1, 'a', 10)");
      
      // HOT update
      db.execute('UPDATE t SET val = 20 WHERE id = 1');
      
      // Delete the row
      db.execute('DELETE FROM t WHERE id = 1');
      
      const r1 = q(db, 'SELECT * FROM t');
      assert.equal(r1.length, 0);
      
      const r2 = q(db, "SELECT * FROM t WHERE name = 'a'");
      assert.equal(r2.length, 0);
    });

    it('INSERT after DELETE after HOT update works', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)');
      db.execute('CREATE INDEX idx ON t (name)');
      db.execute("INSERT INTO t VALUES (1, 'a', 10)");
      
      db.execute('UPDATE t SET val = 20 WHERE id = 1');
      db.execute('DELETE FROM t WHERE id = 1');
      db.execute("INSERT INTO t VALUES (1, 'a', 30)");
      
      const r = q(db, "SELECT * FROM t WHERE name = 'a'");
      assert.equal(r.length, 1);
      assert.equal(r[0].val, 30);
    });
  });
});
