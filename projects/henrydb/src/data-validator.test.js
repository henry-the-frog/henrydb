// data-validator.test.js — Tests for data validator
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { validateTable, rules } from './data-validator.js';
import { Database } from './db.js';

describe('Data Validator', () => {
  function makeDb() {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, status TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 30, 'active')");
    db.execute("INSERT INTO t VALUES (2, 'Bob', 25, 'inactive')");
    db.execute("INSERT INTO t VALUES (3, 'Charlie', -5, 'active')"); // Bad age
    return db;
  }

  it('validates not null', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    db.execute("INSERT INTO t VALUES (2, 'ok')");
    const r = validateTable(db, 't', { val: [rules.notNull()] });
    assert.ok(!r.valid);
    assert.equal(r.errors.length, 1);
  });

  it('validates uniqueness', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'A')");
    db.execute("INSERT INTO t VALUES (2, 'B')");
    db.execute("INSERT INTO t VALUES (3, 'A')"); // Duplicate
    const r = validateTable(db, 't', { code: [rules.unique()] });
    assert.ok(!r.valid);
    assert.equal(r.errors.length, 1);
  });

  it('validates type', () => {
    const db = makeDb();
    const r = validateTable(db, 't', { age: [rules.type('number')] });
    assert.ok(r.valid);
  });

  it('validates range', () => {
    const db = makeDb();
    const r = validateTable(db, 't', { age: [rules.range(0, 150)] });
    assert.ok(!r.valid);
    assert.ok(r.errors.some(e => e.message.includes('-5')));
  });

  it('validates enum', () => {
    const db = makeDb();
    const r = validateTable(db, 't', { status: [rules.enum('active', 'inactive', 'pending')] });
    assert.ok(r.valid);
  });

  it('validates pattern', () => {
    const db = makeDb();
    const r = validateTable(db, 't', { name: [rules.pattern('^[A-Z]')] });
    assert.ok(r.valid);
  });

  it('validates custom rule', () => {
    const db = makeDb();
    const r = validateTable(db, 't', {
      age: [rules.custom((val) => val >= 0, 'Age must be non-negative')]
    });
    assert.ok(!r.valid);
    assert.ok(r.errors[0].message.includes('non-negative'));
  });

  it('multiple rules on same column', () => {
    const db = makeDb();
    const r = validateTable(db, 't', {
      age: [rules.notNull(), rules.type('number'), rules.range(0, 150)]
    });
    assert.ok(!r.valid);
    assert.equal(r.rulesChecked, 3);
  });

  it('valid data passes all checks', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 95)");
    db.execute("INSERT INTO t VALUES (2, 'Bob', 87)");
    const r = validateTable(db, 't', {
      name: [rules.notNull(), rules.unique()],
      score: [rules.range(0, 100)],
    });
    assert.ok(r.valid);
    assert.equal(r.errors.length, 0);
  });
});
