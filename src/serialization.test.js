import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function query(db, sql) {
  const r = db.execute(sql);
  return r && r.rows ? r.rows : r;
}

describe('Database Serialization Roundtrip', () => {
  
  it('should roundtrip a simple table with all column types', () => {
    const db = new Database();
    db.execute(`CREATE TABLE t1 (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      score REAL,
      active BOOLEAN,
      data TEXT DEFAULT 'none'
    )`);
    db.execute(`INSERT INTO t1 VALUES (1, 'Alice', 95.5, true, 'hello')`);
    db.execute(`INSERT INTO t1 VALUES (2, 'Bob', null, false, 'none')`);
    db.execute(`INSERT INTO t1 VALUES (3, 'Charlie', 87.3, true, 'world')`);
    
    const json = db.toJSON();
    const db2 = Database.fromJSON(json);
    
    const rows = query(db2, 'SELECT * FROM t1 ORDER BY id');
    assert.equal(rows.length, 3);
    assert.equal(rows[0].name, 'Alice');
    assert.equal(rows[0].score, 95.5);
    assert.equal(rows[1].score, null);
    assert.equal(rows[2].name, 'Charlie');
  });

  it('should preserve NULL values correctly', () => {
    const db = new Database();
    db.execute('CREATE TABLE null_test (id INTEGER, a TEXT, b INTEGER, c REAL)');
    db.execute(`INSERT INTO null_test VALUES (1, null, null, null)`);
    db.execute(`INSERT INTO null_test VALUES (2, 'text', 42, 3.14)`);
    db.execute(`INSERT INTO null_test VALUES (3, null, 0, null)`);
    
    const json = db.toJSON();
    const db2 = Database.fromJSON(json);
    
    const rows = query(db2, 'SELECT * FROM null_test ORDER BY id');
    assert.equal(rows.length, 3);
    assert.equal(rows[0].a, null);
    assert.equal(rows[0].b, null);
    assert.equal(rows[1].a, 'text');
    assert.equal(rows[2].b, 0);
  });

  it('should handle strings with special characters', () => {
    const db = new Database();
    db.execute('CREATE TABLE special (id INTEGER, val TEXT)');
    db.execute(`INSERT INTO special VALUES (1, 'it''s a test')`);
    db.execute(`INSERT INTO special VALUES (2, 'line1')`);
    db.execute(`INSERT INTO special VALUES (3, '')`);
    
    const json = db.toJSON();
    const db2 = Database.fromJSON(json);
    
    const rows = query(db2, 'SELECT * FROM special ORDER BY id');
    assert.equal(rows.length, 3);
    assert.equal(rows[0].val, "it's a test");
    assert.equal(rows[2].val, '');
  });

  it('should preserve indexes and verify they work after restore', () => {
    const db = new Database();
    db.execute('CREATE TABLE indexed (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    db.execute('CREATE INDEX idx_name ON indexed (name)');
    db.execute('CREATE INDEX idx_age ON indexed (age)');
    db.execute(`INSERT INTO indexed VALUES (1, 'Alice', 30)`);
    db.execute(`INSERT INTO indexed VALUES (2, 'Bob', 25)`);
    db.execute(`INSERT INTO indexed VALUES (3, 'Charlie', 30)`);
    
    const json = db.toJSON();
    const db2 = Database.fromJSON(json);
    
    const rows = query(db2, 'SELECT * FROM indexed WHERE age = 30 ORDER BY id');
    assert.equal(rows.length, 2);
    
    const byName = query(db2, `SELECT * FROM indexed WHERE name = 'Bob'`);
    assert.equal(byName.length, 1);
    assert.equal(byName[0].id, 2);
  });

  it('should preserve sequences and their state', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE user_id_seq START WITH 10 INCREMENT BY 5');
    query(db, `SELECT nextval('user_id_seq')`); // 10
    query(db, `SELECT nextval('user_id_seq')`); // 15
    
    const json = db.toJSON();
    const db2 = Database.fromJSON(json);
    
    const result = query(db2, `SELECT nextval('user_id_seq')`);
    const val = result[0][Object.keys(result[0])[0]];
    assert.equal(val, 20);
  });

  it('should preserve views', () => {
    const db = new Database();
    db.execute('CREATE TABLE employees (id INTEGER, name TEXT, dept TEXT)');
    db.execute(`INSERT INTO employees VALUES (1, 'Alice', 'eng')`);
    db.execute(`INSERT INTO employees VALUES (2, 'Bob', 'sales')`);
    db.execute(`INSERT INTO employees VALUES (3, 'Charlie', 'eng')`);
    db.execute(`CREATE VIEW eng_employees AS SELECT * FROM employees WHERE dept = 'eng'`);
    
    const json = db.toJSON();
    const db2 = Database.fromJSON(json);
    
    const rows = query(db2, 'SELECT * FROM eng_employees ORDER BY id');
    assert.equal(rows.length, 2);
    assert.equal(rows[0].name, 'Alice');
    assert.equal(rows[1].name, 'Charlie');
  });

  it('should handle empty tables', () => {
    const db = new Database();
    db.execute('CREATE TABLE empty (id INTEGER, name TEXT)');
    
    const json = db.toJSON();
    const db2 = Database.fromJSON(json);
    
    const rows = query(db2, 'SELECT * FROM empty');
    assert.equal(rows.length, 0);
    
    db2.execute(`INSERT INTO empty VALUES (1, 'test')`);
    const rows2 = query(db2, 'SELECT * FROM empty');
    assert.equal(rows2.length, 1);
  });

  it('should handle multiple tables with joins', () => {
    const db = new Database();
    db.execute('CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)');
    db.execute(`INSERT INTO departments VALUES (1, 'Engineering')`);
    db.execute(`INSERT INTO departments VALUES (2, 'Sales')`);
    db.execute(`INSERT INTO employees VALUES (1, 'Alice', 1)`);
    db.execute(`INSERT INTO employees VALUES (2, 'Bob', 2)`);
    db.execute(`INSERT INTO employees VALUES (3, 'Charlie', 1)`);
    
    const json = db.toJSON();
    const db2 = Database.fromJSON(json);
    
    const result = query(db2, `
      SELECT e.name, d.name AS dept 
      FROM employees e 
      JOIN departments d ON e.dept_id = d.id 
      ORDER BY e.id
    `);
    assert.equal(result.length, 3);
    assert.equal(result[0].dept, 'Engineering');
    assert.equal(result[1].dept, 'Sales');
  });

  it('should handle large row counts', () => {
    const db = new Database();
    db.execute('CREATE TABLE big (id INTEGER PRIMARY KEY, val TEXT)');
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO big VALUES (${i}, 'row_${i}')`);
    }
    
    const json = db.toJSON();
    const db2 = Database.fromJSON(json);
    
    const count = query(db2, 'SELECT COUNT(*) AS c FROM big');
    assert.equal(count[0].c, 100);
    
    const row50 = query(db2, 'SELECT * FROM big WHERE id = 50');
    assert.equal(row50[0].val, 'row_50');
  });

  it('should handle double serialization roundtrip', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER, name TEXT)');
    db.execute(`INSERT INTO t VALUES (1, 'Alice')`);
    db.execute(`INSERT INTO t VALUES (2, 'Bob')`);
    
    const json1 = db.toJSON();
    const db2 = Database.fromJSON(json1);
    const json2 = db2.toJSON();
    const db3 = Database.fromJSON(json2);
    
    const rows = query(db3, 'SELECT * FROM t ORDER BY id');
    assert.equal(rows.length, 2);
    assert.equal(rows[0].name, 'Alice');
    assert.equal(rows[1].name, 'Bob');
  });

  it('should handle negative and zero numeric values', () => {
    const db = new Database();
    db.execute('CREATE TABLE nums (id INTEGER, val REAL)');
    db.execute('INSERT INTO nums VALUES (-1, -99.5)');
    db.execute('INSERT INTO nums VALUES (0, 0)');
    db.execute('INSERT INTO nums VALUES (1, 0.001)');
    
    const json = db.toJSON();
    const db2 = Database.fromJSON(json);
    
    const rows = query(db2, 'SELECT * FROM nums ORDER BY id');
    assert.equal(rows[0].id, -1);
    assert.equal(rows[0].val, -99.5);
    assert.equal(rows[1].id, 0);
    assert.equal(rows[1].val, 0);
  });

  it('should handle tables with DEFAULT values preserved', () => {
    const db = new Database();
    db.execute(`CREATE TABLE defaults (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'active', count INTEGER DEFAULT 0)`);
    db.execute('INSERT INTO defaults (id) VALUES (1)');
    db.execute(`INSERT INTO defaults VALUES (2, 'inactive', 5)`);
    
    const json = db.toJSON();
    const db2 = Database.fromJSON(json);
    
    // Insert using defaults in restored db
    db2.execute('INSERT INTO defaults (id) VALUES (3)');
    
    const rows = query(db2, 'SELECT * FROM defaults ORDER BY id');
    assert.equal(rows.length, 3);
    assert.equal(rows[0].status, 'active');
    assert.equal(rows[0].count, 0);
    assert.equal(rows[2].status, 'active');
  });

  it('should preserve data after UPDATE in restored db', () => {
    const db = new Database();
    db.execute('CREATE TABLE mutable (id INTEGER PRIMARY KEY, val TEXT)');
    db.execute(`INSERT INTO mutable VALUES (1, 'original')`);
    
    const json = db.toJSON();
    const db2 = Database.fromJSON(json);
    
    db2.execute(`UPDATE mutable SET val = 'modified' WHERE id = 1`);
    const rows = query(db2, 'SELECT * FROM mutable WHERE id = 1');
    assert.equal(rows[0].val, 'modified');
  });

  it('should preserve data after DELETE in restored db', () => {
    const db = new Database();
    db.execute('CREATE TABLE deletable (id INTEGER, val TEXT)');
    db.execute(`INSERT INTO deletable VALUES (1, 'keep')`);
    db.execute(`INSERT INTO deletable VALUES (2, 'remove')`);
    
    const json = db.toJSON();
    const db2 = Database.fromJSON(json);
    
    db2.execute('DELETE FROM deletable WHERE id = 2');
    const rows = query(db2, 'SELECT * FROM deletable');
    assert.equal(rows.length, 1);
    assert.equal(rows[0].val, 'keep');
  });
});
