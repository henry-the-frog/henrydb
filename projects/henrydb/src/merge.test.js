// merge.test.js — MERGE statement (SQL:2003)
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('MERGE', () => {
  it('updates matched and inserts unmatched', () => {
    const db = new Database();
    db.execute('CREATE TABLE target (id INT, name TEXT, val INT)');
    db.execute('CREATE TABLE source (id INT, name TEXT, val INT)');
    db.execute("INSERT INTO target VALUES (1, 'A', 10), (2, 'B', 20)");
    db.execute("INSERT INTO source VALUES (1, 'A', 15), (3, 'C', 30)");

    const r = db.execute(`
      MERGE INTO target t USING source s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET val = s.val
      WHEN NOT MATCHED THEN INSERT (id, name, val) VALUES (s.id, s.name, s.val)
    `);
    
    assert.equal(r.updated, 1);
    assert.equal(r.inserted, 1);
    
    const rows = db.execute('SELECT * FROM target ORDER BY id').rows;
    assert.equal(rows.length, 3);
    assert.equal(rows[0].val, 15); // updated
    assert.equal(rows[1].val, 20); // unchanged
    assert.equal(rows[2].val, 30); // inserted
  });

  it('only update (no insert clause)', () => {
    const db = new Database();
    db.execute('CREATE TABLE target (id INT, val INT)');
    db.execute('CREATE TABLE source (id INT, val INT)');
    db.execute('INSERT INTO target VALUES (1, 10), (2, 20)');
    db.execute('INSERT INTO source VALUES (1, 99), (3, 30)');

    db.execute(`
      MERGE INTO target t USING source s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET val = s.val
    `);
    
    const rows = db.execute('SELECT * FROM target ORDER BY id').rows;
    assert.equal(rows.length, 2);
    assert.equal(rows[0].val, 99);
    assert.equal(rows[1].val, 20);
  });
});
