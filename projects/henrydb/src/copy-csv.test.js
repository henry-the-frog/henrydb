// copy-csv.test.js — CSV import/export
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('COPY CSV', () => {
  it('imports CSV with header', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT, score INT)');
    
    const csv = `id,name,score
1,Alice,95
2,Bob,87
3,Carol,92`;
    
    const result = db.copyFrom('t', csv);
    assert.equal(result.count, 3);
    
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].name, 'Alice');
    assert.equal(r.rows[2].score, 92);
  });

  it('exports query as CSV', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')");
    
    const csv = db.copyTo('SELECT * FROM t ORDER BY id');
    const lines = csv.trim().split('\n');
    assert.equal(lines[0], 'id,name');
    assert.equal(lines[1], '1,Alice');
    assert.equal(lines[2], '2,Bob');
  });

  it('handles quoted fields in CSV', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, desc TEXT)');
    
    const csv = `id,desc
1,"Hello, World"
2,"She said ""hi"""`;
    
    db.copyFrom('t', csv);
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows[0].desc, 'Hello, World');
    assert.equal(r.rows[1].desc, 'She said "hi"');
  });

  it('exports fields with special characters', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'has,comma')");
    
    const csv = db.copyTo('SELECT * FROM t');
    assert.ok(csv.includes('"has,comma"'));
  });

  it('round-trip: export then import', () => {
    const db = new Database();
    db.execute('CREATE TABLE src (id INT, name TEXT, score INT)');
    db.execute("INSERT INTO src VALUES (1, 'Alice', 95), (2, 'Bob', 87)");
    
    const csv = db.copyTo('SELECT * FROM src ORDER BY id');
    
    db.execute('CREATE TABLE dst (id INT, name TEXT, score INT)');
    db.copyFrom('dst', csv);
    
    const r = db.execute('SELECT * FROM dst ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'Alice');
    assert.equal(r.rows[1].score, 87);
  });
});
