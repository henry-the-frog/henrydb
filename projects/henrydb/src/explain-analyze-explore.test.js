// explain-analyze-explore.test.js — Testing EXPLAIN ANALYZE quality
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';
import { Database } from './db.js';

const { Client } = pg;

function getPort() {
  return 25000 + Math.floor(Math.random() * 10000);
}

describe('EXPLAIN ANALYZE Exploration', () => {

  it('EXPLAIN ANALYZE through direct API', () => {
    const db = new Database();
    db.execute("CREATE TABLE products (id INT, name TEXT, price INT, category TEXT)");
    for (let i = 0; i < 1000; i++) {
      const cat = ['electronics', 'books', 'clothing'][i % 3];
      db.execute(`INSERT INTO products VALUES (${i}, 'product-${i}', ${10 + i}, '${cat}')`);
    }
    
    // Simple select with filter
    const result = db.execute("EXPLAIN ANALYZE SELECT * FROM products WHERE category = 'books'");
    console.log('\n=== EXPLAIN ANALYZE SELECT WHERE category = books ===');
    for (const row of result.rows) {
      console.log(row['QUERY PLAN']);
    }
    
    // Should show estimated vs actual rows
    assert.ok(result.actual_rows > 0, 'Should have actual rows');
    assert.ok(result.execution_time_ms >= 0, 'Should have execution time');
    
    // Check accuracy: ~333 rows expected for 1/3 of 1000
    console.log(`\nAccuracy: estimated=${result.analysis?.[0]?.estimated_rows}, actual=${result.actual_rows}`);
  });

  it('EXPLAIN ANALYZE with index vs seq scan', () => {
    const db = new Database();
    db.execute("CREATE TABLE indexed (id INT PRIMARY KEY, val INT)");
    for (let i = 0; i < 500; i++) {
      db.execute(`INSERT INTO indexed VALUES (${i}, ${i * 2})`);
    }
    
    // Index scan (using primary key)
    const indexed = db.execute("EXPLAIN ANALYZE SELECT * FROM indexed WHERE id = 250");
    console.log('\n=== EXPLAIN ANALYZE with PK lookup ===');
    for (const row of indexed.rows) console.log(row['QUERY PLAN']);
    console.log('Scan type:', indexed.analysis?.[0]?.operation);
    
    // Seq scan (using non-indexed column)
    const seqScan = db.execute("EXPLAIN ANALYZE SELECT * FROM indexed WHERE val = 500");
    console.log('\n=== EXPLAIN ANALYZE with non-indexed column ===');
    for (const row of seqScan.rows) console.log(row['QUERY PLAN']);
    console.log('Scan type:', seqScan.analysis?.[0]?.operation);
  });

  it('EXPLAIN ANALYZE with JOIN', () => {
    const db = new Database();
    db.execute("CREATE TABLE orders (id INT, customer_id INT, total INT)");
    db.execute("CREATE TABLE customers (id INT, name TEXT)");
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO customers VALUES (${i}, 'customer-${i}')`);
    }
    for (let i = 0; i < 500; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, ${i % 100}, ${10 + i})`);
    }
    
    const result = db.execute("EXPLAIN ANALYZE SELECT c.name, SUM(o.total) FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name");
    console.log('\n=== EXPLAIN ANALYZE with JOIN + GROUP BY ===');
    for (const row of result.rows) console.log(row['QUERY PLAN']);
    
    assert.ok(result.actual_rows > 0);
  });

  it('EXPLAIN ANALYZE through wire protocol', async () => {
    const port = getPort();
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-ea-'));
    const server = new HenryDBServer({ port, dataDir: dir, transactional: true });
    await server.start();
    
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query("CREATE TABLE test_ea (id INT, val TEXT)");
    for (let i = 0; i < 100; i++) {
      await client.query(`INSERT INTO test_ea VALUES (${i}, 'val-${i}')`);
    }
    
    const result = await client.query("EXPLAIN ANALYZE SELECT * FROM test_ea WHERE id > 50");
    console.log('\n=== EXPLAIN ANALYZE through wire protocol ===');
    for (const row of result.rows) {
      console.log(row['QUERY PLAN'] || JSON.stringify(row));
    }
    
    assert.ok(result.rows.length > 0, 'Should return plan rows');
    
    await client.end();
    await server.stop();
    rmSync(dir, { recursive: true });
  });

  it('planTree format (if available)', () => {
    const db = new Database();
    db.execute("CREATE TABLE tree_test (id INT, a INT, b INT)");
    for (let i = 0; i < 200; i++) {
      db.execute(`INSERT INTO tree_test VALUES (${i}, ${i % 10}, ${i % 20})`);
    }
    
    const result = db.execute("EXPLAIN ANALYZE SELECT a, COUNT(*) FROM tree_test WHERE b < 10 GROUP BY a ORDER BY a");
    console.log('\n=== Plan Tree ===');
    if (result.planTreeText) {
      console.log(result.planTreeText);
    } else {
      console.log('(no plan tree available)');
      for (const row of result.rows) console.log(row['QUERY PLAN']);
    }
    
    assert.ok(result.actual_rows > 0);
  });
});
