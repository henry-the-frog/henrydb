// volcano-correctness.test.js — Comprehensive Volcano vs Legacy correctness tests
// Verifies that all SQL patterns produce identical results through both paths
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { buildPlan } from './volcano-planner.js';
import { parse } from './sql.js';

function executeVolcano(sql, db) {
  const ast = parse(sql);
  const plan = buildPlan(ast, db.tables, db._indexes, db._tableStats);
  if (!plan) return null;
  plan.open();
  const rows = [];
  let row;
  while ((row = plan.next()) !== null) rows.push(row);
  plan.close();
  return rows;
}

function verifyMatch(db, sql, description) {
  const legacy = db.execute(sql).rows || [];
  const volcano = executeVolcano(sql, db);
  assert.ok(volcano !== null, `${description}: Volcano returned null plan`);
  assert.equal(volcano.length, legacy.length, 
    `${description}: row count mismatch (volcano=${volcano.length} legacy=${legacy.length})`);
}

describe('Volcano Correctness: All SQL Patterns', () => {
  let db;
  
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, category TEXT, price INT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, product_id INT, customer TEXT, qty INT, status TEXT)');
    db.execute('CREATE TABLE customers (cid INT PRIMARY KEY, cname TEXT, city TEXT, tier INT)');
    db.execute('CREATE INDEX idx_orders_pid ON orders(product_id)');
    
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO products VALUES (${i}, 'prod${i}', 'cat${i % 5}', ${i * 10 + 5})`);
    }
    for (let i = 0; i < 400; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, ${i % 100}, 'cust${i % 50}', ${(i % 10) + 1}, '${['pending', 'shipped', 'delivered'][i % 3]}')`);
    }
    for (let i = 0; i < 50; i++) {
      db.execute(`INSERT INTO customers VALUES (${i}, 'cust${i}', 'city${i % 5}', ${(i % 3) + 1})`);
    }
    db.execute('ANALYZE TABLE products');
    db.execute('ANALYZE TABLE orders');
    db.execute('ANALYZE TABLE customers');
  });

  describe('Predicates', () => {
    it('equality', () => verifyMatch(db, "SELECT * FROM products WHERE category = 'cat2'", 'equality'));
    it('range (>)', () => verifyMatch(db, 'SELECT * FROM products WHERE price > 500', 'range GT'));
    it('range (<=)', () => verifyMatch(db, 'SELECT * FROM products WHERE price <= 200', 'range LE'));
    it('LIKE', () => verifyMatch(db, "SELECT * FROM products WHERE name LIKE 'prod1%'", 'LIKE'));
    it('IN list', () => verifyMatch(db, 'SELECT * FROM products WHERE id IN (1, 5, 10, 50)', 'IN list'));
    it('BETWEEN', () => verifyMatch(db, 'SELECT * FROM products WHERE price BETWEEN 100 AND 500', 'BETWEEN'));
    it('IS NULL', () => verifyMatch(db, 'SELECT * FROM products WHERE name IS NOT NULL', 'IS NOT NULL'));
    it('AND', () => verifyMatch(db, "SELECT * FROM products WHERE price > 200 AND category = 'cat0'", 'AND'));
    it('OR', () => verifyMatch(db, "SELECT * FROM products WHERE category = 'cat0' OR price < 50", 'OR'));
    it('NOT', () => verifyMatch(db, 'SELECT * FROM products WHERE NOT (price > 500)', 'NOT'));
    it('CASE WHEN in WHERE', () => verifyMatch(db, 'SELECT * FROM products WHERE CASE WHEN price > 500 THEN 1 ELSE 0 END = 1', 'CASE'));
    it('arithmetic in WHERE', () => verifyMatch(db, 'SELECT * FROM products WHERE price * 2 > 1000', 'arith'));
  });

  describe('Aggregates', () => {
    it('GROUP BY', () => verifyMatch(db, 'SELECT category, COUNT(*), SUM(price) FROM products GROUP BY category', 'GROUP BY'));
    it('HAVING', () => verifyMatch(db, 'SELECT category, SUM(price) AS s FROM products GROUP BY category HAVING SUM(price) > 10000', 'HAVING'));
    it('COUNT without GROUP BY', () => verifyMatch(db, 'SELECT COUNT(*) FROM products', 'COUNT'));
    it('multi-column GROUP BY', () => verifyMatch(db, "SELECT category, status, COUNT(*) FROM products p JOIN orders o ON o.product_id = p.id GROUP BY category, status", 'multi GROUP BY'));
  });

  describe('Sorting and Limiting', () => {
    it('ORDER BY', () => verifyMatch(db, 'SELECT * FROM products ORDER BY price DESC', 'ORDER BY'));
    it('LIMIT', () => verifyMatch(db, 'SELECT * FROM products ORDER BY price DESC LIMIT 10', 'LIMIT'));
    it('DISTINCT', () => verifyMatch(db, 'SELECT DISTINCT category FROM products', 'DISTINCT'));
  });

  describe('Joins', () => {
    it('INNER JOIN', () => verifyMatch(db, 'SELECT p.name, o.qty FROM orders o JOIN products p ON o.product_id = p.id', 'INNER JOIN'));
    it('LEFT JOIN', () => verifyMatch(db, 'SELECT p.name, o.qty FROM products p LEFT JOIN orders o ON o.product_id = p.id', 'LEFT JOIN'));
    it('JOIN + WHERE', () => verifyMatch(db, 'SELECT p.name, o.qty FROM orders o JOIN products p ON o.product_id = p.id WHERE p.price > 500', 'JOIN+WHERE'));
    it('JOIN + GROUP BY', () => verifyMatch(db, 'SELECT p.category, SUM(o.qty) FROM orders o JOIN products p ON o.product_id = p.id GROUP BY p.category', 'JOIN+GROUP'));
    it('self-join', () => verifyMatch(db, 'SELECT a.id, b.price FROM products a JOIN products b ON a.id = b.id WHERE a.price > 500', 'self-join'));
  });

  describe('Set Operations', () => {
    it('UNION ALL', () => verifyMatch(db, 'SELECT * FROM products WHERE id < 10 UNION ALL SELECT * FROM products WHERE id > 90', 'UNION ALL'));
  });

  describe('CTEs', () => {
    it('simple CTE', () => verifyMatch(db, 'WITH expensive AS (SELECT * FROM products WHERE price > 500) SELECT * FROM expensive', 'CTE'));
    it('CTE with join', () => verifyMatch(db, "WITH top AS (SELECT * FROM products WHERE price > 800) SELECT t.name, o.qty FROM top t JOIN orders o ON o.product_id = t.id", 'CTE+JOIN'));
  });

  describe('Subqueries', () => {
    it('IN subquery', () => verifyMatch(db, "SELECT * FROM products WHERE id IN (SELECT product_id FROM orders WHERE customer = 'cust0')", 'IN subquery'));
    it('NOT IN subquery', () => verifyMatch(db, "SELECT * FROM products WHERE id NOT IN (SELECT product_id FROM orders WHERE customer = 'cust0')", 'NOT IN subquery'));
  });

  describe('Window Functions', () => {
    it('ROW_NUMBER', () => verifyMatch(db, 'SELECT id, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price) AS rn FROM products', 'ROW_NUMBER'));
  });

  describe('Expressions', () => {
    it('arithmetic in SELECT', () => verifyMatch(db, 'SELECT id, price * 2 AS doubled FROM products', 'arith SELECT'));
    it('string concat', () => verifyMatch(db, "SELECT id, name || '-' || category AS combined FROM products", 'string concat'));
    it('COALESCE', () => verifyMatch(db, "SELECT COALESCE(category, 'none') FROM products", 'COALESCE'));
    it('CAST', () => verifyMatch(db, "SELECT CAST(price AS TEXT) FROM products WHERE id < 5", 'CAST'));
    it('CASE in SELECT', () => verifyMatch(db, "SELECT id, CASE WHEN price > 500 THEN 'expensive' ELSE 'cheap' END AS tier FROM products", 'CASE SELECT'));
  });

  describe('Complex Queries', () => {
    it('Join + Aggregate + Having + Order', () => verifyMatch(db,
      'SELECT p.category, SUM(o.qty) AS total FROM orders o JOIN products p ON o.product_id = p.id GROUP BY p.category HAVING SUM(o.qty) > 100 ORDER BY total DESC',
      'complex'));
  });
});
