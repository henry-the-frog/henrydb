// agg-advanced.test.js — Advanced aggregate tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Advanced Aggregates', () => {
  it('MIN on strings', () => {
    const db = new Database();
    db.execute('CREATE TABLE names (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO names VALUES (1, 'Charlie')");
    db.execute("INSERT INTO names VALUES (2, 'Alice')");
    db.execute("INSERT INTO names VALUES (3, 'Bob')");
    const r = db.execute('SELECT MIN(name) AS first FROM names');
    assert.equal(r.rows[0].first, 'Alice');
  });

  it('MAX on strings', () => {
    const db = new Database();
    db.execute('CREATE TABLE names (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO names VALUES (1, 'Charlie')");
    db.execute("INSERT INTO names VALUES (2, 'Alice')");
    db.execute("INSERT INTO names VALUES (3, 'Bob')");
    const r = db.execute('SELECT MAX(name) AS last FROM names');
    assert.equal(r.rows[0].last, 'Charlie');
  });

  it('COUNT(column) excludes NULLs', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO data VALUES (1, 10)');
    db.execute('INSERT INTO data VALUES (2, NULL)');
    db.execute('INSERT INTO data VALUES (3, 30)');
    const r = db.execute('SELECT COUNT(val) AS cnt FROM data');
    assert.equal(r.rows[0].cnt, 2); // NULL excluded
    const rAll = db.execute('SELECT COUNT(*) AS cnt FROM data');
    assert.equal(rAll.rows[0].cnt, 3); // All rows
  });

  it('SUM skips NULLs', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO data VALUES (1, 10)');
    db.execute('INSERT INTO data VALUES (2, NULL)');
    db.execute('INSERT INTO data VALUES (3, 30)');
    const r = db.execute('SELECT SUM(val) AS total FROM data');
    assert.equal(r.rows[0].total, 40);
  });

  it('AVG skips NULLs', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO data VALUES (1, 10)');
    db.execute('INSERT INTO data VALUES (2, NULL)');
    db.execute('INSERT INTO data VALUES (3, 30)');
    const r = db.execute('SELECT AVG(val) AS avg FROM data');
    assert.equal(r.rows[0].avg, 20); // (10+30)/2
  });

  it('multiple aggregates mixed', () => {
    const db = new Database();
    db.execute('CREATE TABLE scores (id INT PRIMARY KEY, subject TEXT, score INT)');
    db.execute("INSERT INTO scores VALUES (1, 'Math', 90)");
    db.execute("INSERT INTO scores VALUES (2, 'English', 85)");
    db.execute("INSERT INTO scores VALUES (3, 'Math', 95)");
    db.execute("INSERT INTO scores VALUES (4, 'English', 80)");
    db.execute("INSERT INTO scores VALUES (5, 'Science', 88)");
    const r = db.execute('SELECT subject, MIN(score) AS low, MAX(score) AS high, AVG(score) AS avg, COUNT(*) AS cnt FROM scores GROUP BY subject ORDER BY subject');
    assert.equal(r.rows.length, 3);
    const eng = r.rows.find(row => row.subject === 'English');
    assert.equal(eng.low, 80);
    assert.equal(eng.high, 85);
    assert.equal(eng.cnt, 2);
  });

  it('aggregate on empty group', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, cat TEXT, val INT)');
    db.execute("INSERT INTO data VALUES (1, 'A', 10)");
    db.execute("INSERT INTO data VALUES (2, 'A', 20)");
    const r = db.execute("SELECT cat, SUM(val) AS total FROM data WHERE cat = 'B' GROUP BY cat");
    assert.equal(r.rows.length, 0);
  });

  it('GROUP BY with NULL category', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, cat TEXT, val INT)');
    db.execute("INSERT INTO data VALUES (1, 'A', 10)");
    db.execute('INSERT INTO data VALUES (2, NULL, 20)');
    db.execute('INSERT INTO data VALUES (3, NULL, 30)');
    const r = db.execute('SELECT cat, SUM(val) AS total FROM data GROUP BY cat');
    assert.equal(r.rows.length, 2); // 'A' and NULL groups
  });
});
