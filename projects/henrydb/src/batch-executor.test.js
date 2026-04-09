// batch-executor.test.js — Tests for batch executor
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { executeBatch, formatReport } from './batch-executor.js';
import { Database } from './db.js';

describe('Batch Executor', () => {
  it('executes multi-statement script', () => {
    const db = new Database();
    const r = executeBatch(db, `
      CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);
      INSERT INTO t VALUES (1, 'a');
      INSERT INTO t VALUES (2, 'b');
      SELECT * FROM t;
    `);
    assert.equal(r.total, 4);
    assert.equal(r.succeeded, 4);
    assert.equal(r.failed, 0);
  });

  it('stops on error with stopOnError=true', () => {
    const db = new Database();
    const r = executeBatch(db, `
      CREATE TABLE t (id INTEGER PRIMARY KEY);
      INVALID SQL HERE;
      SELECT * FROM t;
    `, { stopOnError: true });
    assert.equal(r.succeeded, 1);
    assert.equal(r.failed, 1);
    assert.equal(r.executed, 2); // Stopped after error
  });

  it('continues on error with stopOnError=false', () => {
    const db = new Database();
    const r = executeBatch(db, `
      CREATE TABLE t (id INTEGER PRIMARY KEY);
      INVALID SQL;
      INSERT INTO t VALUES (1);
    `, { stopOnError: false });
    assert.equal(r.succeeded, 2);
    assert.equal(r.failed, 1);
    assert.equal(r.executed, 3);
  });

  it('tracks progress', () => {
    const db = new Database();
    const progress = [];
    executeBatch(db, 'SELECT 1; SELECT 2; SELECT 3', {
      onProgress: (p) => progress.push(p)
    });
    assert.equal(progress.length, 3);
    assert.equal(progress[2].percent, 100);
  });

  it('reports duration', () => {
    const db = new Database();
    const r = executeBatch(db, 'SELECT 1');
    assert.ok(r.duration >= 0);
  });

  it('handles empty script', () => {
    const db = new Database();
    const r = executeBatch(db, '');
    assert.equal(r.total, 0);
    assert.equal(r.succeeded, 0);
  });

  it('skips comments', () => {
    const db = new Database();
    const r = executeBatch(db, `
      -- This is a comment
      SELECT 1;
      -- Another comment
      SELECT 2;
    `);
    assert.equal(r.total, 2);
    assert.equal(r.succeeded, 2);
  });

  it('formatReport generates readable output', () => {
    const db = new Database();
    const r = executeBatch(db, 'SELECT 1; INVALID; SELECT 2', { stopOnError: false });
    const output = formatReport(r);
    assert.ok(output.includes('Batch'));
    assert.ok(output.includes('Errors'));
  });
});
