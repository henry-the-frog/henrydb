// matview.test.js — Tests for materialized views with automatic refresh
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Materialized views', () => {
  it('CREATE MATERIALIZED VIEW', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (id INTEGER PRIMARY KEY, dept TEXT, amount INTEGER)');
    db.execute("INSERT INTO sales VALUES (1, 'A', 100)");
    db.execute("INSERT INTO sales VALUES (2, 'A', 200)");
    db.execute("INSERT INTO sales VALUES (3, 'B', 150)");
    
    db.execute('CREATE MATERIALIZED VIEW dept_totals AS SELECT dept, SUM(amount) as total FROM sales GROUP BY dept');
    
    const result = db.execute('SELECT * FROM dept_totals ORDER BY dept');
    assert.equal(result.rows.length, 2);
    assert.equal(result.rows[0].dept, 'A');
    assert.equal(result.rows[0].total, 300);
    assert.equal(result.rows[1].dept, 'B');
    assert.equal(result.rows[1].total, 150);
  });

  it('REFRESH MATERIALIZED VIEW updates stale data', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, price INTEGER)');
    db.execute('INSERT INTO products VALUES (1, 100)');
    db.execute('INSERT INTO products VALUES (2, 200)');
    
    db.execute('CREATE MATERIALIZED VIEW stats AS SELECT COUNT(*) as cnt, AVG(price) as avg_price FROM products');
    
    const initial = db.execute('SELECT * FROM stats');
    assert.equal(initial.rows.length, 1);
    
    // Add more data
    db.execute('INSERT INTO products VALUES (3, 300)');
    db.execute('INSERT INTO products VALUES (4, 400)');
    
    // Refresh updates the materialized data
    db.execute('REFRESH MATERIALIZED VIEW stats');
    
    const after = db.execute('SELECT * FROM stats');
    assert.equal(after.rows[0].cnt, 4);
    assert.equal(after.rows[0].avg_price, 250);
  });

  it('mat view with JOINs', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total INTEGER)');
    db.execute('CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO customers VALUES (1, 'Alice')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob')");
    db.execute('INSERT INTO orders VALUES (1, 1, 100)');
    db.execute('INSERT INTO orders VALUES (2, 1, 200)');
    db.execute('INSERT INTO orders VALUES (3, 2, 150)');
    
    db.execute('CREATE MATERIALIZED VIEW customer_summary AS SELECT c.name, SUM(o.total) as revenue FROM orders o JOIN customers c ON o.customer_id = c.id GROUP BY c.name');
    
    const result = db.execute('SELECT * FROM customer_summary ORDER BY name');
    assert.equal(result.rows[0].name, 'Alice');
    assert.equal(result.rows[0].revenue, 300);
    assert.equal(result.rows[1].name, 'Bob');
    assert.equal(result.rows[1].revenue, 150);
  });

  it('mat view queryable like regular table', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)');
    for (let i = 1; i <= 100; i++) {
      const cat = ['X', 'Y', 'Z'][i % 3];
      db.execute(`INSERT INTO data VALUES (${i}, '${cat}', ${i})`);
    }
    
    db.execute('CREATE MATERIALIZED VIEW cat_stats AS SELECT category, COUNT(*) as cnt, SUM(val) as total FROM data GROUP BY category');
    
    const result = db.execute("SELECT * FROM cat_stats WHERE cnt > 30");
    assert.ok(result.rows.length > 0);
  });

  it('mat view with ORDER BY in definition', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');
    for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO items VALUES (${i}, 'item-${i}', ${i * 5})`);
    
    db.execute('CREATE MATERIALIZED VIEW top_items AS SELECT * FROM items ORDER BY score DESC LIMIT 5');
    
    const result = db.execute('SELECT * FROM top_items');
    assert.equal(result.rows.length, 5);
    assert.equal(result.rows[0].score, 100);
  });

  it('DROP VIEW removes materialized view', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    
    db.execute('CREATE MATERIALIZED VIEW mv AS SELECT * FROM t');
    
    // Should be queryable
    const result = db.execute('SELECT * FROM mv');
    assert.equal(result.rows.length, 1);
    
    // Drop both view def and backing table
    db.execute('DROP VIEW mv');
    db.execute('DROP TABLE mv');
    
    // Should no longer exist
    assert.throws(() => db.execute('SELECT * FROM mv'), /not found|does not exist/i);
  });

  it('mat view as pre-computed lookup table', () => {
    const db = new Database();
    db.execute('CREATE TABLE big_data (id INTEGER PRIMARY KEY, grp INTEGER, val INTEGER)');
    for (let i = 1; i <= 1000; i++) {
      db.execute(`INSERT INTO big_data VALUES (${i}, ${i % 50}, ${i})`);
    }
    
    // Create mat view — pre-computes the aggregation
    db.execute('CREATE MATERIALIZED VIEW grp_summary AS SELECT grp, COUNT(*) as cnt, SUM(val) as total FROM big_data GROUP BY grp');
    
    // Query mat view 
    const result = db.execute('SELECT * FROM grp_summary WHERE grp = 25');
    assert.ok(result.rows.length > 0);
    assert.equal(result.rows[0].grp, 25);
    
    // Verify counts match
    const direct = db.execute('SELECT COUNT(*) as cnt FROM big_data WHERE grp = 25');
    assert.equal(result.rows[0].cnt, direct.rows[0].cnt);
  });

  it('mat view with window functions', () => {
    const db = new Database();
    db.execute('CREATE TABLE scores (id INTEGER PRIMARY KEY, player TEXT, score INTEGER)');
    db.execute("INSERT INTO scores VALUES (1, 'Alice', 100)");
    db.execute("INSERT INTO scores VALUES (2, 'Bob', 200)");
    db.execute("INSERT INTO scores VALUES (3, 'Charlie', 150)");
    
    db.execute('CREATE MATERIALIZED VIEW ranked_players AS SELECT player, score, RANK() OVER (ORDER BY score DESC) as rank FROM scores');
    
    const result = db.execute('SELECT * FROM ranked_players');
    assert.equal(result.rows.length, 3);
    const bob = result.rows.find(r => r.player === 'Bob');
    assert.equal(bob.rank, 1);
  });
});
