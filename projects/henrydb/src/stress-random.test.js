// stress-random.test.js — Random SQL stress testing for HenryDB
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

describe('Random SQL Stress Tests', () => {
  let dir, db;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'stress-'));
    db = TransactionalDatabase.open(dir);
    db.execute('CREATE TABLE stress (id INT PRIMARY KEY, val INT, name TEXT, score DECIMAL(10,2))');
    db.execute('CREATE INDEX idx_val ON stress (val)');
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO stress VALUES (${i}, ${i * 10}, 'name${i}', ${(i * 1.5).toFixed(2)})`);
    }
  });

  it('100 random SELECT queries', () => {
    const queries = [];
    for (let i = 0; i < 100; i++) {
      const id = Math.floor(Math.random() * 100);
      const val = Math.floor(Math.random() * 1000);
      const ops = ['=', '<', '>', '<=', '>='];
      const op = ops[Math.floor(Math.random() * ops.length)];
      queries.push(`SELECT * FROM stress WHERE val ${op} ${val}`);
      queries.push(`SELECT * FROM stress WHERE id = ${id}`);
      queries.push(`SELECT COUNT(*) AS cnt FROM stress WHERE val > ${val}`);
    }
    
    for (const q of queries) {
      try {
        const result = db.execute(q);
        assert.ok(result.rows !== undefined, `Query should return rows: ${q}`);
      } catch (e) {
        assert.fail(`Query crashed: ${q}: ${e.message}`);
      }
    }
  });

  it('50 random UPDATE then verify', () => {
    for (let i = 0; i < 50; i++) {
      const id = Math.floor(Math.random() * 100);
      const newVal = Math.floor(Math.random() * 10000);
      db.execute(`UPDATE stress SET val = ${newVal} WHERE id = ${id}`);
      
      // Verify update
      const result = db.execute(`SELECT val FROM stress WHERE id = ${id}`);
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].val, newVal);
    }
    
    // Overall count should still be 100
    const count = db.execute('SELECT COUNT(*) AS cnt FROM stress');
    assert.equal(count.rows[0].cnt, 100);
  });

  it('concurrent sessions with conflicting updates', () => {
    let conflicts = 0;
    for (let round = 0; round < 20; round++) {
      const id = Math.floor(Math.random() * 10); // Small range for conflicts
      const s1 = db.session();
      const s2 = db.session();
      
      s1.begin();
      s2.begin();
      
      try {
        s1.execute(`UPDATE stress SET val = ${round * 100} WHERE id = ${id}`);
        s2.execute(`UPDATE stress SET val = ${round * 200} WHERE id = ${id}`);
        s1.commit();
        s2.commit();
      } catch (e) {
        if (e.message.includes('conflict')) {
          conflicts++;
          try { s1.rollback(); } catch {}
          try { s2.rollback(); } catch {}
        } else {
          assert.fail(`Unexpected error: ${e.message}`);
        }
      }
    }
    // Should have detected some conflicts
    assert.ok(conflicts > 0, `Expected some write-write conflicts, got ${conflicts}`);
  });

  it('mixed DML with vacuum', () => {
    for (let i = 0; i < 50; i++) {
      const id = Math.floor(Math.random() * 100);
      db.execute(`UPDATE stress SET val = ${i * 100}, name = 'updated${i}' WHERE id = ${id}`);
    }
    
    // Vacuum
    const stats = db.vacuum();
    const deadRemoved = Object.values(stats).reduce((sum, t) => sum + (t.deadTuplesRemoved || 0), 0);
    assert.ok(deadRemoved > 0, 'Should have removed some dead tuples');
    
    // Verify data integrity
    const count = db.execute('SELECT COUNT(*) AS cnt FROM stress');
    assert.equal(count.rows[0].cnt, 100);
    
    // All rows should be readable
    for (let i = 0; i < 100; i++) {
      const row = db.execute(`SELECT * FROM stress WHERE id = ${i}`);
      assert.equal(row.rows.length, 1, `Row ${i} should exist`);
      assert.equal(row.rows[0].id, i);
    }
  });

  it('aggregation with division on DECIMAL columns', () => {
    const result = db.execute('SELECT AVG(score) AS avg_score, SUM(score)/COUNT(*) AS manual_avg FROM stress');
    assert.ok(result.rows.length === 1);
    assert.ok(typeof result.rows[0].avg_score === 'number');
    // Average of 0, 1.5, 3.0, ..., 148.5 = 74.25
    assert.ok(Math.abs(result.rows[0].avg_score - 74.25) < 0.1);
  });

  it('window functions with ORDER BY', () => {
    const result = db.execute(`
      SELECT id, val, 
        ROW_NUMBER() OVER (ORDER BY val DESC) AS rn,
        RANK() OVER (ORDER BY val DESC) AS rnk
      FROM stress 
      WHERE id < 10
      ORDER BY val DESC
    `);
    assert.equal(result.rows.length, 10);
    assert.equal(result.rows[0].rn, 1);
    assert.equal(result.rows[0].val, 90); // id=9, val=90
  });

  it('subquery with MVCC visibility', () => {
    const s = db.session();
    s.begin();
    
    // External update while session is open
    db.execute('UPDATE stress SET val = 9999 WHERE id = 0');
    
    // Session should see old value (snapshot isolation)
    const result = s.execute(`
      SELECT * FROM stress 
      WHERE val = (SELECT MIN(val) FROM stress)
    `);
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].id, 0);
    assert.equal(result.rows[0].val, 0); // Original value
    
    s.rollback();
  });
});
