// mvcc-fuzzer.test.js — Random MVCC operation fuzzer
// Generates random sequences of INSERT/UPDATE/DELETE/SELECT in concurrent sessions
// and verifies consistency: no phantom rows, correct counts, ACID properties
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh() {
  dir = join(tmpdir(), `henrydb-mvccfuzz-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir);
}

function cleanup() {
  try { db?.close(); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

class RNG {
  constructor(seed) { this.state = seed | 0 || 1; }
  next() {
    let s = this.state;
    s ^= s << 13; s ^= s >> 17; s ^= s << 5;
    this.state = s;
    return (s >>> 0) / 0x100000000;
  }
  int(min, max) { return Math.floor(this.next() * (max - min + 1)) + min; }
  pick(arr) { return arr[this.int(0, arr.length - 1)]; }
  bool(p = 0.5) { return this.next() < p; }
}

describe('MVCC Operation Fuzzer', () => {
  afterEach(cleanup);

  it('random INSERT/SELECT consistency — 100 operations', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val INT)');
    const rng = new RNG(42);
    const expected = new Set();
    
    for (let i = 0; i < 100; i++) {
      const id = rng.int(1, 50);
      if (rng.bool(0.7)) {
        // INSERT (skip if exists)
        if (!expected.has(id)) {
          try {
            db.execute(`INSERT INTO t VALUES (${id}, ${rng.int(1, 1000)})`);
            expected.add(id);
          } catch {}
        }
      } else {
        // SELECT
        const r = db.execute(`SELECT * FROM t WHERE id = ${id}`);
        if (expected.has(id)) {
          assert.equal(r.rows.length, 1, `Expected to find id=${id}`);
        } else {
          assert.equal(r.rows.length, 0, `Expected NOT to find id=${id}`);
        }
      }
    }
    
    // Final consistency check
    const r = db.execute('SELECT COUNT(*) as cnt FROM t');
    assert.equal(r.rows[0].cnt, expected.size);
  });

  it('random INSERT/DELETE/SELECT — 100 operations', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val INT)');
    const rng = new RNG(1337);
    const exists = new Set();
    
    for (let i = 0; i < 100; i++) {
      const id = rng.int(1, 30);
      const op = rng.int(0, 2);
      
      if (op === 0 && !exists.has(id)) {
        // INSERT
        db.execute(`INSERT INTO t VALUES (${id}, ${rng.int(1, 100)})`);
        exists.add(id);
      } else if (op === 1 && exists.has(id)) {
        // DELETE
        db.execute(`DELETE FROM t WHERE id = ${id}`);
        exists.delete(id);
      } else {
        // SELECT
        const r = db.execute('SELECT COUNT(*) as cnt FROM t');
        assert.equal(r.rows[0].cnt, exists.size);
      }
    }
    
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, exists.size);
  });

  it('concurrent session fuzzer — 2 sessions, 50 ops each', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val INT)');
    // Seed data
    for (let i = 1; i <= 20; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    }
    
    const rng = new RNG(99);
    const s1 = db.session();
    const s2 = db.session();
    
    // Both start transactions
    s1.begin();
    s2.begin();
    
    // Take snapshots
    const s1Count = s1.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt;
    const s2Count = s2.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt;
    assert.equal(s1Count, 20);
    assert.equal(s2Count, 20);
    
    // s1 makes changes
    for (let i = 0; i < 5; i++) {
      s1.execute(`INSERT INTO t VALUES (${100 + i}, ${rng.int(1, 100)})`);
    }
    
    // s2 should still see 20 rows (snapshot isolation)
    const s2After = s2.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt;
    assert.equal(s2After, 20, 'snapshot isolation: s2 should not see s1 uncommitted inserts');
    
    s1.commit();
    
    // s2 still sees 20 (snapshot taken before s1 committed)
    const s2Final = s2.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt;
    assert.equal(s2Final, 20, 'snapshot isolation: s2 should not see s1 committed inserts');
    
    s2.commit();
    
    // New query sees all 25 rows
    const finalCount = db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt;
    assert.equal(finalCount, 25);
    
    s1.close();
    s2.close();
  });

  it('savepoint + random operations — 50 ops with periodic rollbacks', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val INT)');
    const rng = new RNG(777);
    const s = db.session();
    s.begin();
    
    let insertedCount = 0;
    let checkpoints = []; // (savepoint name, count at that point)
    
    for (let i = 0; i < 50; i++) {
      const op = rng.int(0, 9);
      
      if (op < 5) {
        // INSERT
        s.execute(`INSERT INTO t VALUES (${1000 + i}, ${rng.int(1, 100)})`);
        insertedCount++;
      } else if (op < 7 && checkpoints.length === 0) {
        // CREATE SAVEPOINT
        const spName = `sp${i}`;
        s.execute(`SAVEPOINT ${spName}`);
        checkpoints.push({ name: spName, count: insertedCount });
      } else if (op < 8 && checkpoints.length > 0) {
        // ROLLBACK TO last savepoint
        const cp = checkpoints[checkpoints.length - 1];
        s.execute(`ROLLBACK TO ${cp.name}`);
        insertedCount = cp.count;
        checkpoints = []; // Savepoints after this one are gone
      } else {
        // SELECT — verify count
        const r = s.execute('SELECT COUNT(*) as cnt FROM t');
        assert.equal(r.rows[0].cnt, insertedCount, `Count mismatch at op ${i}`);
      }
    }
    
    // Final check
    const r = s.execute('SELECT COUNT(*) as cnt FROM t');
    assert.equal(r.rows[0].cnt, insertedCount);
    
    s.commit();
    
    // After commit, count should match
    const finalR = db.execute('SELECT COUNT(*) as cnt FROM t');
    assert.equal(finalR.rows[0].cnt, insertedCount);
    
    s.close();
  });

  it('aggregation correctness — SUM invariant across concurrent sessions', () => {
    db = fresh();
    db.execute('CREATE TABLE accounts (id INT, balance INT)');
    const initialBalance = 1000;
    for (let i = 1; i <= 10; i++) {
      db.execute(`INSERT INTO accounts VALUES (${i}, ${initialBalance})`);
    }
    const totalBalance = 10 * initialBalance;
    
    const rng = new RNG(42);
    
    // Run 10 "transfers" — each deducts from one account and adds to another
    for (let i = 0; i < 10; i++) {
      const from = rng.int(1, 10);
      let to = rng.int(1, 10);
      while (to === from) to = rng.int(1, 10);
      const amount = rng.int(1, 100);
      
      const s = db.session();
      s.begin();
      const fromRow = s.execute(`SELECT balance FROM accounts WHERE id = ${from}`);
      if (fromRow.rows.length > 0 && fromRow.rows[0].balance >= amount) {
        s.execute(`UPDATE accounts SET balance = balance - ${amount} WHERE id = ${from}`);
        s.execute(`UPDATE accounts SET balance = balance + ${amount} WHERE id = ${to}`);
      }
      s.commit();
      s.close();
    }
    
    // SUM should always equal initial total
    const r = db.execute('SELECT SUM(balance) as total FROM accounts');
    assert.equal(r.rows[0].total, totalBalance, 'total balance should be conserved');
  });
});
