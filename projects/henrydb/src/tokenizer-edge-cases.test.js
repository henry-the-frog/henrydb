// tokenizer-edge-cases.test.js — Regression tests for tokenizer edge cases
// Especially: negative number vs minus operator disambiguation

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { tokenize } from './sql.js';
import { Database } from './db.js';

function types(sql) {
  return tokenize(sql).filter(t => t.type !== 'EOF').map(t =>
    t.type === 'NUMBER' ? `NUM(${t.value})` :
    t.type === 'IDENT' ? `ID(${t.value})` :
    t.type === 'KEYWORD' ? `KW(${t.value})` :
    t.type === 'STRING' ? `STR(${t.value})` :
    t.type
  );
}

describe('Tokenizer: Negative number vs MINUS operator', () => {
  it('1-1 → NUMBER MINUS NUMBER (not NUMBER NUMBER(-1))', () => {
    const toks = types('1-1');
    assert.deepStrictEqual(toks, ['NUM(1)', 'MINUS', 'NUM(1)']);
  });

  it('1 - 1 → NUMBER MINUS NUMBER', () => {
    assert.deepStrictEqual(types('1 - 1'), ['NUM(1)', 'MINUS', 'NUM(1)']);
  });

  it('-1 → negative NUMBER literal', () => {
    assert.deepStrictEqual(types('-1'), ['NUM(-1)']);
  });

  it('(-1) → ( negative-number )', () => {
    assert.deepStrictEqual(types('(-1)'), ['(', 'NUM(-1)', ')']);
  });

  it(',-5 → comma negative-number', () => {
    const toks = types(',-5');
    assert.deepStrictEqual(toks, [',', 'NUM(-5)']);
  });

  it('id-1 → IDENT MINUS NUMBER', () => {
    assert.deepStrictEqual(types('id-1'), ['ID(id)', 'MINUS', 'NUM(1)']);
  });

  it('a+b-c → IDENT PLUS IDENT MINUS IDENT', () => {
    assert.deepStrictEqual(types('a+b-c'), ['ID(a)', 'PLUS', 'ID(b)', 'MINUS', 'ID(c)']);
  });

  it('VALUES (1, -5) → negative after comma', () => {
    const toks = tokenize('(1, -5)').filter(t => t.type !== 'EOF');
    const numToks = toks.filter(t => t.type === 'NUMBER');
    assert.strictEqual(numToks[0].value, 1);
    assert.strictEqual(numToks[1].value, -5);
  });

  it('= -3 → comparison then negative', () => {
    const toks = tokenize('= -3').filter(t => t.type !== 'EOF');
    assert.strictEqual(toks[0].type, 'EQ');
    assert.strictEqual(toks[1].type, 'NUMBER');
    assert.strictEqual(toks[1].value, -3);
  });

  it(')-1 → ) MINUS NUMBER (not negative)', () => {
    const toks = types(')-1');
    assert.deepStrictEqual(toks, [')', 'MINUS', 'NUM(1)']);
  });

  it('"string"-1 → STRING MINUS NUMBER', () => {
    const toks = tokenize("'hello'-1").filter(t => t.type !== 'EOF');
    assert.strictEqual(toks[0].type, 'STRING');
    assert.strictEqual(toks[1].type, 'MINUS');
    assert.strictEqual(toks[2].type, 'NUMBER');
  });
});

describe('Tokenizer: End-to-end with Database', () => {
  let db;

  it('setup', () => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
  });

  it('WHERE 1-1 is falsy (subtraction = 0)', () => {
    assert.strictEqual(db.execute('SELECT * FROM t WHERE 1-1').rows.length, 0);
  });

  it('WHERE 2-1 is truthy (subtraction = 1)', () => {
    assert.strictEqual(db.execute('SELECT * FROM t WHERE 2-1').rows.length, 5);
  });

  it('WHERE id=3-1 returns id=2', () => {
    assert.strictEqual(db.execute('SELECT * FROM t WHERE id=3-1').rows[0].id, 2);
  });

  it('WHERE val=id*10-10+10 returns all rows', () => {
    assert.strictEqual(db.execute('SELECT * FROM t WHERE val=id*10-10+10').rows.length, 5);
  });

  it('INSERT with negative literal works', () => {
    db.execute('INSERT INTO t VALUES (-1, -100)');
    const r = db.execute('SELECT * FROM t WHERE id = -1');
    assert.strictEqual(r.rows[0].val, -100);
  });

  it('SELECT with negative in expression', () => {
    const r = db.execute('SELECT id, val-20 as adjusted FROM t WHERE id = 3');
    assert.strictEqual(r.rows[0].adjusted, 10); // 30 - 20
  });
});
