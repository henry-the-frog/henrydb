// orderby-alias.test.js — ORDER BY should resolve SELECT aliases
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('ORDER BY alias resolution', () => {
  let db;
  
  function setup() {
    db = new Database();
    db.execute('CREATE TABLE sales (id INT, product TEXT, amount INT)');
    db.execute('CREATE TABLE returns (id INT, product TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES (1, 'A', 100), (2, 'B', 200), (3, 'C', 150)");
    db.execute("INSERT INTO returns VALUES (1, 'A', 10), (2, 'D', 30)");
  }
  
  it('ORDER BY COALESCE alias with FULL JOIN', () => {
    setup();
    const r = db.execute(`
      SELECT COALESCE(s.product, r.product) as product,
             s.amount as sold, r.amount as returned
      FROM sales s FULL JOIN returns r ON s.product = r.product
      ORDER BY product
    `);
    assert.deepEqual(r.rows.map(r => r.product), ['A', 'B', 'C', 'D']);
  });
  
  it('ORDER BY COALESCE alias with FULL JOIN DESC', () => {
    setup();
    const r = db.execute(`
      SELECT COALESCE(s.product, r.product) as product
      FROM sales s FULL JOIN returns r ON s.product = r.product
      ORDER BY product DESC
    `);
    assert.deepEqual(r.rows.map(r => r.product), ['D', 'C', 'B', 'A']);
  });
  
  it('ORDER BY expression alias (addition)', () => {
    setup();
    const r = db.execute(`
      SELECT id, amount * 2 as doubled FROM sales ORDER BY doubled
    `);
    assert.deepEqual(r.rows.map(r => r.doubled), [200, 300, 400]);
  });
  
  it('ORDER BY alias with LEFT JOIN', () => {
    setup();
    const r = db.execute(`
      SELECT COALESCE(s.product, 'none') as name
      FROM sales s LEFT JOIN returns r ON s.product = r.product
      ORDER BY name
    `);
    assert.deepEqual(r.rows.map(r => r.name), ['A', 'B', 'C']);
  });
  
  it('ORDER BY alias with RIGHT JOIN', () => {
    setup();
    const r = db.execute(`
      SELECT COALESCE(s.product, r.product) as product
      FROM sales s RIGHT JOIN returns r ON s.product = r.product
      ORDER BY product
    `);
    assert.deepEqual(r.rows.map(r => r.product), ['A', 'D']);
  });
  
  it('ORDER BY simple alias still works', () => {
    setup();
    const r = db.execute('SELECT product as p FROM sales ORDER BY p');
    assert.deepEqual(r.rows.map(r => r.p), ['A', 'B', 'C']);
  });
  
  it('ORDER BY numeric reference still works', () => {
    setup();
    const r = db.execute('SELECT product, amount FROM sales ORDER BY 2');
    assert.deepEqual(r.rows.map(r => r.amount), [100, 150, 200]);
  });
});
