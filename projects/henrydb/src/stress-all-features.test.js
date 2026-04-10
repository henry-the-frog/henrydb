// stress-all-features.test.js — Rapid exercise of every major feature
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const { Client } = pg;

describe('Feature Stress Test', () => {
  let server, port, dir;
  
  before(async () => {
    port = 34100 + Math.floor(Math.random() * 2000);
    dir = mkdtempSync(join(tmpdir(), 'henrydb-stress-'));
    server = new HenryDBServer({ port, dataDir: dir });
    await server.start();
  });
  
  after(async () => {
    if (server) await server.stop();
    if (dir) rmSync(dir, { recursive: true });
  });

  it('complete lifecycle: schema, data, queries, persistence', async () => {
    const c = await connect(port);
    
    // 1. Schema creation
    await c.query('CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT, tier TEXT DEFAULT \'bronze\')');
    await c.query('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT, category TEXT)');
    await c.query('CREATE TABLE orders (id SERIAL PRIMARY KEY, user_id INT, product_id INT, qty INT, total INT, status TEXT DEFAULT \'pending\')');
    await c.query('CREATE INDEX idx_cat ON products (category)');
    
    // 2. Data population with RETURNING
    const u1 = await c.query("INSERT INTO users (name, email, tier) VALUES ('Alice', 'alice@test.com', 'gold') RETURNING *");
    assert.equal(String(u1.rows[0].id), '1');
    const u2 = await c.query("INSERT INTO users (name, email) VALUES ('Bob', 'bob@test.com') RETURNING id");
    
    for (let i = 1; i <= 20; i++) {
      await c.query('INSERT INTO products VALUES ($1, $2, $3, $4)', [i, 'Product-'+i, 100*i, 'cat-'+(i%5)]);
    }
    
    // 3. Orders with expressions
    for (let i = 0; i < 50; i++) {
      const uid = 1 + (i % 2);
      const pid = 1 + (i % 20);
      const qty = 1 + (i % 5);
      await c.query("INSERT INTO orders (user_id, product_id, qty, total, status) VALUES ($1, $2, $3, $4, $5)", 
        [uid, pid, qty, qty * pid * 100, i % 3 === 0 ? 'completed' : 'pending']);
    }
    
    // 4. Complex queries
    const revenue = await c.query(`
      SELECT p.category, SUM(o.total) as total_rev, COUNT(*) as order_count
      FROM orders o JOIN products p ON o.product_id = p.id
      WHERE o.status = 'completed'
      GROUP BY p.category
      HAVING COUNT(*) >= 1
      ORDER BY total_rev DESC
    `);
    assert.ok(revenue.rows.length >= 1);
    
    // 5. Subquery
    const above_avg = await c.query('SELECT name, price FROM products WHERE price > (SELECT AVG(price) FROM products) ORDER BY price DESC LIMIT 5');
    assert.ok(above_avg.rows.length >= 1);
    
    // 6. CASE WHEN
    const graded = await c.query(`
      SELECT name, price,
        CASE WHEN price > 1500 THEN 'premium' WHEN price > 500 THEN 'standard' ELSE 'budget' END as tier
      FROM products ORDER BY price DESC LIMIT 5
    `);
    assert.equal(graded.rows[0].tier, 'premium');
    
    // 7. String concatenation
    const labels = await c.query("SELECT name || ' ($' || CAST(price AS TEXT) || ')' as label FROM products WHERE id <= 3 ORDER BY id");
    assert.ok(labels.rows[0].label.includes('$'));
    
    // 8. UPSERT
    await c.query("INSERT INTO products VALUES (1, 'Updated Widget', 999, 'special') ON CONFLICT (id) DO UPDATE SET name = 'Updated Widget', price = 999");
    const upserted = await c.query('SELECT name FROM products WHERE id = 1');
    assert.equal(upserted.rows[0].name, 'Updated Widget');
    
    // 9. UPDATE RETURNING
    const updated = await c.query("UPDATE products SET price = price + 100 WHERE category = 'cat-0' RETURNING id, name, price");
    assert.ok(updated.rows.length >= 1);
    
    // 10. DELETE RETURNING
    const deleted = await c.query("DELETE FROM orders WHERE status = 'pending' AND qty = 1 RETURNING id");
    assert.ok(deleted.rows.length >= 0);
    
    // 11. Views
    await c.query('CREATE VIEW high_value AS SELECT * FROM products WHERE price > 1000');
    const hv = await c.query('SELECT COUNT(*) as cnt FROM high_value');
    assert.ok(parseInt(String(hv.rows[0].cnt)) >= 1);
    
    // 12. CTE
    const cte = await c.query('WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n < 5) SELECT SUM(n) as total FROM nums');
    assert.equal(String(cte.rows[0].total), '15');
    
    // 13. GENERATE_SERIES
    const series = await c.query('SELECT COUNT(*) as cnt FROM GENERATE_SERIES(1, 100)');
    assert.equal(String(series.rows[0].cnt), '100');
    
    // 14. Aggregate functions
    const stats = await c.query('SELECT COUNT(*) as cnt, SUM(price) as total, AVG(price) as avg, MIN(price) as min, MAX(price) as max FROM products');
    assert.ok(parseInt(String(stats.rows[0].cnt)) > 0);
    
    // 15. DISTINCT
    const cats = await c.query('SELECT DISTINCT category FROM products ORDER BY category');
    assert.equal(cats.rows.length, 5);
    
    // 16. EXPLAIN
    const plan = await c.query('EXPLAIN SELECT * FROM products WHERE category = \'cat-1\'');
    assert.ok(plan.rows.length > 0);
    
    // 17. VACUUM
    await c.query('VACUUM');
    
    // 18. SHOW TABLE STATUS
    const status = await c.query('SHOW TABLE STATUS');
    assert.ok(status.rows.length >= 3);
    
    // 19. DESCRIBE
    const desc = await c.query('DESCRIBE users');
    assert.ok(desc.rows.length >= 4);
    
    await c.end();
    
    // 20. Persistence: restart server, verify data
    await server.stop();
    server = new HenryDBServer({ port, dataDir: dir });
    await server.start();
    
    const c2 = await connect(port);
    const persisted = await c2.query('SELECT COUNT(*) as cnt FROM products');
    assert.ok(parseInt(String(persisted.rows[0].cnt)) >= 20);
    
    const persistedOrders = await c2.query('SELECT COUNT(*) as cnt FROM orders');
    assert.ok(parseInt(String(persistedOrders.rows[0].cnt)) >= 1);
    
    await c2.end();
  });
});

async function connect(port) {
  const c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
  await c.connect();
  return c;
}
