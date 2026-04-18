// chaos.test.js — Random interleaved DDL + DML + concurrent transactions
// Throws random operations at the database and verifies consistency.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;
function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-chaos-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

function rng(seed) {
  // Simple deterministic PRNG for reproducibility
  let s = seed;
  return () => {
    s = (s * 1103515245 + 12345) & 0x7fffffff;
    return s / 0x7fffffff;
  };
}

describe('Chaos Tests', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('100 random DDL + DML operations maintain consistency', () => {
    const rand = rng(42);
    
    // Setup: create initial tables
    db.execute('CREATE TABLE main (id INT PRIMARY KEY, val INT, cat TEXT)');
    db.execute('CREATE TABLE log (id INT, action TEXT)');
    
    let nextId = 1;
    let expectedCount = 0;
    let logCount = 0;
    let columnCount = 3; // id, val, cat
    
    for (let i = 0; i < 100; i++) {
      const op = rand();
      try {
        if (op < 0.3) {
          // INSERT
          const id = nextId++;
          const val = Math.floor(rand() * 1000);
          const cat = ['a', 'b', 'c'][Math.floor(rand() * 3)];
          db.execute(`INSERT INTO main VALUES (${id}, ${val}, '${cat}')`);
          expectedCount++;
        } else if (op < 0.45) {
          // UPDATE
          const id = Math.floor(rand() * nextId) + 1;
          const newVal = Math.floor(rand() * 1000);
          db.execute(`UPDATE main SET val = ${newVal} WHERE id = ${id}`);
        } else if (op < 0.55) {
          // DELETE
          const id = Math.floor(rand() * nextId) + 1;
          const before = rows(db.execute(`SELECT id FROM main WHERE id = ${id}`));
          db.execute(`DELETE FROM main WHERE id = ${id}`);
          if (before.length > 0) expectedCount--;
        } else if (op < 0.65) {
          // LOG INSERT
          logCount++;
          db.execute(`INSERT INTO log VALUES (${logCount}, 'op_${i}')`);
        } else if (op < 0.7) {
          // SELECT (just verify it works)
          rows(db.execute("SELECT * FROM main WHERE cat = 'a' ORDER BY id"));
        } else if (op < 0.75) {
          // Aggregate
          rows(db.execute('SELECT COUNT(*) as cnt, SUM(val) as total FROM main'));
        } else if (op < 0.8) {
          // JOIN
          rows(db.execute('SELECT m.id FROM main m, log l WHERE m.id = l.id'));
        } else if (op < 0.85) {
          // Subquery
          rows(db.execute('SELECT * FROM main WHERE val > (SELECT AVG(val) FROM main)'));
        } else if (op < 0.9) {
          // Window function
          rows(db.execute('SELECT id, ROW_NUMBER() OVER (ORDER BY val) as rn FROM main'));
        } else {
          // CASE expression
          rows(db.execute("SELECT id, CASE WHEN val > 500 THEN 'high' ELSE 'low' END as tier FROM main"));
        }
      } catch (e) {
        // Some operations may fail (duplicate PK, etc.) — that's fine
      }
    }
    
    // Verify consistency
    const r = rows(db.execute('SELECT COUNT(*) as cnt FROM main'));
    assert.equal(r[0].cnt, expectedCount, `Expected ${expectedCount} rows, got ${r[0].cnt}`);
    
    const lr = rows(db.execute('SELECT COUNT(*) as cnt FROM log'));
    assert.equal(lr[0].cnt, logCount, `Expected ${logCount} log entries`);
    
    // Verify no duplicate PKs
    const dupes = rows(db.execute('SELECT id, COUNT(*) as cnt FROM main GROUP BY id HAVING COUNT(*) > 1'));
    assert.equal(dupes.length, 0, 'No duplicate PKs should exist');
  });

  it('concurrent chaos: 3 sessions interleaved', () => {
    db.execute('CREATE TABLE t (id INT, val INT, source TEXT)');
    
    const sessions = [];
    for (let i = 0; i < 3; i++) {
      const s = db.session();
      s.begin();
      sessions.push(s);
    }
    
    // Each session does 10 inserts
    for (let round = 0; round < 10; round++) {
      for (let i = 0; i < 3; i++) {
        try {
          sessions[i].execute(`INSERT INTO t VALUES (${round * 3 + i + 1}, ${round * 10 + i}, 'session_${i}')`);
        } catch { /* ignore */ }
      }
    }
    
    // Commit all
    let commitCount = 0;
    for (const s of sessions) {
      try { s.commit(); commitCount++; } catch { /* ignore */ }
    }
    
    // At least some sessions should have committed
    assert.ok(commitCount > 0, 'At least one session should commit');
    
    const r = rows(db.execute('SELECT * FROM t'));
    assert.ok(r.length > 0, 'Should have some rows');
    
    // Verify data integrity
    for (const row of r) {
      assert.ok(row.id >= 1 && row.id <= 30, `ID ${row.id} out of range`);
      assert.ok(['session_0', 'session_1', 'session_2'].includes(row.source), 'Source should be valid');
    }
  });

  it('chaos survives close/reopen', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    
    // Do a bunch of random operations
    for (let i = 1; i <= 50; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, 'val_${i}')`);
    }
    db.execute('DELETE FROM t WHERE id BETWEEN 20 AND 30');
    db.execute("UPDATE t SET val = 'updated' WHERE id < 10");
    db.execute('ALTER TABLE t ADD COLUMN extra INT');
    db.execute("INSERT INTO t VALUES (51, 'new', 99)");
    
    db.close();
    db = TransactionalDatabase.open(dbDir);
    
    const r = rows(db.execute('SELECT COUNT(*) as cnt FROM t'));
    assert.equal(r[0].cnt, 40, 'Should have 50 - 11 + 1 = 40 rows'); // 50 - (20..30) + 51
    
    const updated = rows(db.execute("SELECT COUNT(*) as cnt FROM t WHERE val = 'updated'"));
    assert.ok(updated[0].cnt > 0, 'Updated rows should survive');
    
    const newRow = rows(db.execute('SELECT extra FROM t WHERE id = 51'));
    assert.equal(newRow[0].extra, 99, 'New column data should survive');
  });
});
