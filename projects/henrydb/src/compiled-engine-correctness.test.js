// compiled-engine-correctness.test.js — Verify compiled engine handles unsupported expressions safely
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { CompiledQueryEngine } from './compiled-query.js';
import { parse } from './sql.js';

describe('Compiled Engine Expression Correctness', () => {
  let db, engine;
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT, name TEXT)');
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, ${i * 10}, 'name${i}')`);
    }
    engine = new CompiledQueryEngine(db, { compileThreshold: 50 });
  });

  it('BETWEEN filter works in compiled engine', () => {
    const ast = parse('SELECT * FROM t WHERE val BETWEEN 200 AND 500');
    const result = engine.executeSelect(ast);
    assert.ok(result);
    assert.equal(result.rows.length, 31); // val=200,210,...,500
  });

  it('IS NULL filter works', () => {
    db.execute('INSERT INTO t VALUES (100, NULL, NULL)');
    const ast = parse('SELECT * FROM t WHERE val IS NULL');
    const result = engine.executeSelect(ast);
    assert.ok(result);
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].id, 100);
  });

  it('IS NOT NULL filter works', () => {
    db.execute('INSERT INTO t VALUES (100, NULL, NULL)');
    const ast = parse('SELECT * FROM t WHERE val IS NOT NULL');
    const result = engine.executeSelect(ast);
    assert.ok(result);
    assert.equal(result.rows.length, 100);
  });

  it('IN list filter works', () => {
    const ast = parse('SELECT * FROM t WHERE val IN (100, 300, 500)');
    const result = engine.executeSelect(ast);
    assert.ok(result);
    assert.equal(result.rows.length, 3);
  });

  it('LIKE filter works', () => {
    const ast = parse("SELECT * FROM t WHERE name LIKE 'name1%'");
    const result = engine.executeSelect(ast);
    assert.ok(result);
    // name1, name10-name19 = 11 total
    assert.equal(result.rows.length, 11);
  });

  it('unsupported expression falls back to interpreter (returns null)', () => {
    const ast = parse('SELECT * FROM t');
    ast.where = { type: 'EXOTIC_UNSUPPORTED_TYPE', value: 42 };
    // executeSelect catches the throw and returns null (interpreter fallback)
    const result = engine.executeSelect(ast);
    assert.equal(result, null, 'Should return null for unsupported expressions');
  });

  it('AND with unsupported child falls back to interpreter', () => {
    const ast = parse('SELECT * FROM t');
    ast.where = {
      type: 'AND',
      left: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'val' }, right: { type: 'literal', value: 10 } },
      right: { type: 'EXOTIC_UNSUPPORTED_TYPE' }
    };
    // The AND returns null → filter is null → throws → caught → returns null
    const result = engine.executeSelect(ast);
    assert.equal(result, null, 'Should fall back when AND has unsupported child');
  });
});
