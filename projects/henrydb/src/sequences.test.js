// sequences.test.js — SEQUENCE and SERIAL tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Sequences', () => {
  it('CREATE SEQUENCE + nextval', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE test_seq');
    const v1 = db.execute("SELECT nextval('test_seq') as v").rows[0].v;
    const v2 = db.execute("SELECT nextval('test_seq') as v").rows[0].v;
    assert.equal(v2, v1 + 1);
  });

  it('SERIAL column auto-increments', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE t_id_seq');
    db.execute("CREATE TABLE t (id INT DEFAULT nextval('t_id_seq'), name TEXT)");
    db.execute("INSERT INTO t (name) VALUES ('alice')");
    db.execute("INSERT INTO t (name) VALUES ('bob')");
    db.execute("INSERT INTO t (name) VALUES ('charlie')");
    
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 3);
    assert.ok(r.rows[0].id < r.rows[1].id);
    assert.ok(r.rows[1].id < r.rows[2].id);
  });

  it('sequence values are unique', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE unique_seq');
    const values = new Set();
    for (let i = 0; i < 100; i++) {
      const v = db.execute("SELECT nextval('unique_seq') as v").rows[0].v;
      assert.ok(!values.has(v), `Duplicate sequence value: ${v}`);
      values.add(v);
    }
    assert.equal(values.size, 100);
  });

  it('currval returns current value', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE cv_seq');
    db.execute("SELECT nextval('cv_seq')"); // advance to 1
    const curr = db.execute("SELECT currval('cv_seq') as v").rows[0].v;
    assert.equal(curr, 1);
  });
});
