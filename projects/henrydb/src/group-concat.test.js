// group-concat.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('GROUP_CONCAT', () => {
  it('concatenates values with default separator', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, dept TEXT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Eng', 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Eng', 'Bob')");
    db.execute("INSERT INTO t VALUES (3, 'Sales', 'Carol')");

    const r = db.execute('SELECT dept, GROUP_CONCAT(name) AS members FROM t GROUP BY dept ORDER BY dept');
    assert.equal(r.rows.find(r => r.dept === 'Eng').members, 'Alice,Bob');
    assert.equal(r.rows.find(r => r.dept === 'Sales').members, 'Carol');
  });

  it('uses custom separator', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, dept TEXT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Eng', 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Eng', 'Bob')");

    const r = db.execute("SELECT GROUP_CONCAT(name SEPARATOR ' | ') AS result FROM t");
    assert.equal(r.rows[0].result, 'Alice | Bob');
  });
});
