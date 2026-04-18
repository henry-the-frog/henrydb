// copy.test.js — Tests for COPY FROM/TO
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('COPY TO', () => {
  it('exports table as CSV', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')");
    
    const r = db.execute('COPY t TO STDOUT WITH (FORMAT CSV)');
    assert.equal(r.rowCount, 2);
    assert.ok(r.data.includes('1,Alice'));
    assert.ok(r.data.includes('2,Bob'));
  });

  it('exports with header', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    
    const r = db.execute('COPY t TO STDOUT WITH (FORMAT CSV, HEADER true)');
    const lines = r.data.split('\n');
    assert.equal(lines[0], 'id,name');
    assert.equal(lines[1], '1,Alice');
  });

  it('exports with tab delimiter (default)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    
    const r = db.execute('COPY t TO STDOUT');
    assert.ok(r.data.includes('1\tAlice'));
  });

  it('exports query results', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT, score INT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 90), (2, 'Bob', 60), (3, 'Carol', 85)");
    
    const r = db.execute('COPY (SELECT name, score FROM t WHERE score >= 80) TO STDOUT WITH (FORMAT CSV)');
    assert.equal(r.rowCount, 2);
    assert.ok(r.data.includes('Alice,90'));
    assert.ok(r.data.includes('Carol,85'));
    assert.ok(!r.data.includes('Bob'));
  });

  it('quotes values containing delimiters', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, desc TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello, world')");
    
    const r = db.execute('COPY t TO STDOUT WITH (FORMAT CSV)');
    assert.ok(r.data.includes('"hello, world"'));
  });

  it('handles empty table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    
    const r = db.execute('COPY t TO STDOUT WITH (FORMAT CSV)');
    assert.equal(r.rowCount, 0);
    assert.equal(r.data, '');
  });
});

describe('COPY FROM', () => {
  it('imports CSV data', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)');
    
    db.execute("COPY t FROM '1,Alice,30\n2,Bob,25' WITH (FORMAT CSV)");
    
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'Alice');
    assert.equal(r.rows[0].age, 30);
    assert.equal(r.rows[1].name, 'Bob');
  });

  it('imports with header', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    
    db.execute("COPY t FROM 'id,name\n1,Alice\n2,Bob' WITH (FORMAT CSV, HEADER true)");
    
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 2);
  });

  it('handles NULL values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score INT)');
    
    db.execute("COPY t FROM '1,Alice,\\N\n2,Bob,85' WITH (FORMAT CSV)");
    
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows[0].score, null);
    assert.equal(r.rows[1].score, 85);
  });

  it('reports row count', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    
    const r = db.execute("COPY t FROM '1,a\n2,b\n3,c' WITH (FORMAT CSV)");
    assert.equal(r.rowCount, 3);
  });

  it('roundtrip: COPY TO then COPY FROM', () => {
    const db = new Database();
    db.execute('CREATE TABLE src (id INT PRIMARY KEY, name TEXT, val FLOAT)');
    db.execute("INSERT INTO src VALUES (1, 'Alice', 3.14), (2, 'Bob', 2.72)");
    
    // Export
    const exported = db.execute('COPY src TO STDOUT WITH (FORMAT CSV)');
    
    // Import into new table
    db.execute('CREATE TABLE dst (id INT PRIMARY KEY, name TEXT, val FLOAT)');
    db.execute(`COPY dst FROM '${exported.data}' WITH (FORMAT CSV)`);
    
    const r = db.execute('SELECT * FROM dst ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'Alice');
    assert.ok(Math.abs(r.rows[0].val - 3.14) < 0.01);
  });
});
