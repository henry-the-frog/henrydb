// sql-analyzer.test.js — Tests for SQL complexity analyzer
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { analyzeSQL, analyze } from './sql-analyzer.js';
import { parse } from './sql.js';

describe('SQL Analyzer', () => {
  function check(sql) { return analyzeSQL(parse(sql)); }

  it('simple SELECT', () => {
    const m = check('SELECT * FROM t');
    assert.equal(m.type, 'SELECT');
    assert.equal(m.tables, 1);
    assert.equal(m.class, 'simple');
  });

  it('SELECT with WHERE', () => {
    const m = check('SELECT name FROM users WHERE age > 21');
    assert.ok(m.conditions >= 1);
  });

  it('JOIN query', () => {
    const m = check('SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id');
    assert.ok(m.joins >= 1);
    assert.ok(m.complexity > 3);
  });

  it('complex multi-join', () => {
    const m = check(`
      SELECT u.name, p.title, c.body
      FROM users u
      JOIN posts p ON u.id = p.user_id
      JOIN comments c ON p.id = c.post_id
      WHERE u.active = 1
      ORDER BY p.created_at DESC
      LIMIT 10
    `);
    assert.ok(m.joins >= 2);
    assert.ok(m.hasLimit);
    assert.ok(m.class === 'complex' || m.class === 'moderate');
  });

  it('aggregate query', () => {
    const m = check('SELECT category, COUNT(*) as cnt, AVG(price) as avg FROM products GROUP BY category');
    assert.ok(m.groupBy >= 1);
  });

  it('DISTINCT', () => {
    const m = check('SELECT DISTINCT category FROM products');
    assert.equal(m.distinct, true);
  });

  it('ORDER BY', () => {
    const m = check('SELECT * FROM t ORDER BY a, b DESC');
    assert.ok(m.orderBy >= 1);
  });

  it('HAVING', () => {
    const m = check('SELECT grp, COUNT(*) FROM t GROUP BY grp HAVING COUNT(*) > 5');
    assert.equal(m.hasHaving, true);
  });

  it('CTE query', () => {
    const m = check('WITH cte AS (SELECT id FROM t) SELECT * FROM cte');
    assert.ok(m.ctes >= 1);
  });

  it('subquery', () => {
    const m = check('SELECT * FROM t WHERE id IN (SELECT id FROM other)');
    assert.ok(m.conditions >= 1); // IN subquery counted as condition
  });

  it('very complex query', () => {
    const m = check(`
      SELECT u.name, 
             COUNT(DISTINCT p.id) as posts,
             SUM(c.rating) as total_rating
      FROM users u
      JOIN posts p ON u.id = p.user_id
      JOIN comments c ON p.id = c.post_id
      WHERE u.active = 1 AND u.age > 18
      GROUP BY u.name
      HAVING COUNT(p.id) > 5
      ORDER BY total_rating DESC
      LIMIT 10
    `);
    assert.ok(m.complexity > 10);
    assert.ok(m.class === 'complex' || m.class === 'very complex');
  });

  it('analyze() works with string', () => {
    const m = analyze('SELECT 1', parse);
    assert.equal(m.type, 'SELECT');
  });

  it('analyze() handles invalid SQL', () => {
    const m = analyze('INVALID', parse);
    assert.equal(m.class, 'error');
  });
});
