// optimizer-correctness.test.js — Verify optimizer doesn't change query semantics
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Optimizer Correctness', () => {
  let db, dbNoOpt;
  before(() => {
    db = new Database();
    dbNoOpt = new Database();
    
    // Set up identical tables
    for (const d of [db, dbNoOpt]) {
      d.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, status TEXT, created TEXT)');
      d.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, region TEXT, tier TEXT)');
      
      for (let i = 1; i <= 50; i++) {
        d.execute(`INSERT INTO customers VALUES (${i}, 'customer_${i}', 'region-${i % 5}', '${i % 3 === 0 ? 'gold' : 'silver'}')`);
      }
      for (let i = 1; i <= 200; i++) {
        const cust = (i % 50) + 1;
        const amount = 10 + (i * 7) % 500;
        const status = i % 4 === 0 ? 'cancelled' : i % 3 === 0 ? 'pending' : 'completed';
        d.execute(`INSERT INTO orders VALUES (${i}, ${cust}, ${amount}, '${status}', '2024-${String(1 + (i % 12)).padStart(2, '0')}-${String(1 + (i % 28)).padStart(2, '0')}')`);
      }
    }
    
    // Disable optimizer on dbNoOpt
    if (dbNoOpt._optimizer) dbNoOpt._optimizer = null;
  });

  function compare(sql, orderBy = null) {
    const r1 = db.execute(sql);
    const r2 = dbNoOpt.execute(sql);
    
    // Compare row counts
    assert.strictEqual(r1.rows.length, r2.rows.length, `Row count mismatch for: ${sql}`);
    
    // Sort rows for comparison (unless already ordered)
    const sortKey = orderBy || (r1.rows[0] ? Object.keys(r1.rows[0])[0] : null);
    if (sortKey && !sql.includes('ORDER BY')) {
      const sort = (a, b) => String(a[sortKey]).localeCompare(String(b[sortKey]));
      r1.rows.sort(sort);
      r2.rows.sort(sort);
    }
    
    // Compare each row
    for (let i = 0; i < r1.rows.length; i++) {
      assert.deepStrictEqual(r1.rows[i], r2.rows[i], `Row ${i} mismatch for: ${sql}`);
    }
  }

  // Basic queries
  it('simple scan', () => compare('SELECT * FROM customers WHERE region = \'region-0\''));
  it('scan with filter', () => compare('SELECT * FROM orders WHERE amount > 300'));
  it('multi-condition filter', () => compare("SELECT * FROM orders WHERE amount > 100 AND status = 'completed'"));
  it('OR condition', () => compare("SELECT * FROM orders WHERE status = 'cancelled' OR amount < 20"));
  
  // JOINs
  it('inner join', () => compare('SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id WHERE o.amount > 400'));
  it('left join', () => compare('SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON c.id = o.customer_id WHERE c.id <= 5'));
  
  // Aggregates
  it('count', () => compare("SELECT COUNT(*) as cnt FROM orders WHERE status = 'completed'"));
  it('sum', () => compare('SELECT SUM(amount) as total FROM orders'));
  it('group by', () => compare('SELECT status, COUNT(*) as cnt, SUM(amount) as total FROM orders GROUP BY status'));
  it('group by having', () => compare('SELECT customer_id, COUNT(*) as cnt FROM orders GROUP BY customer_id HAVING COUNT(*) > 3'));
  it('group by with join', () => compare('SELECT c.region, COUNT(*) as cnt, AVG(o.amount) as avg FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.region'));
  
  // Subqueries
  it('in subquery', () => compare("SELECT name FROM customers WHERE id IN (SELECT DISTINCT customer_id FROM orders WHERE status = 'cancelled')"));
  it('scalar subquery', () => compare('SELECT name, (SELECT COUNT(*) FROM orders WHERE customer_id = customers.id) as order_count FROM customers WHERE id <= 5'));
  
  // LIMIT + ORDER
  it('order by limit', () => compare('SELECT * FROM orders ORDER BY amount DESC LIMIT 5'));
  it('order by offset', () => compare('SELECT * FROM orders ORDER BY id LIMIT 5 OFFSET 10'));
  
  // Complex
  it('CTE + join + aggregate', () => compare(`
    WITH big_orders AS (SELECT * FROM orders WHERE amount > 300)
    SELECT c.name, COUNT(*) as cnt FROM customers c 
    JOIN big_orders bo ON c.id = bo.customer_id 
    GROUP BY c.name 
    HAVING COUNT(*) >= 2
  `));
  
  it('window function', () => compare('SELECT id, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) as rn FROM orders WHERE id <= 20'));
  
  it('distinct', () => compare('SELECT DISTINCT status FROM orders'));
  
  it('union', () => compare("SELECT name FROM customers WHERE region = 'region-0' UNION SELECT name FROM customers WHERE tier = 'gold'"));
});
