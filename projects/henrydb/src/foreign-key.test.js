// foreign-key.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Foreign Key Constraints', () => {
  it('rejects insert with invalid foreign key', () => {
    const db = new Database();
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO parents VALUES (1, 'Parent')");
    db.execute('CREATE TABLE children (id INT PRIMARY KEY, parent_id INT REFERENCES parents(id))');
    
    assert.throws(
      () => db.execute('INSERT INTO children VALUES (1, 99)'),
      /Foreign key/
    );
  });

  it('allows valid foreign key', () => {
    const db = new Database();
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY)');
    db.execute('INSERT INTO parents VALUES (1)');
    db.execute('CREATE TABLE children (id INT PRIMARY KEY, parent_id INT REFERENCES parents(id))');
    db.execute('INSERT INTO children VALUES (1, 1)');
    assert.equal(db.execute('SELECT * FROM children').rows.length, 1);
  });

  it('allows NULL foreign key', () => {
    const db = new Database();
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE children (id INT PRIMARY KEY, parent_id INT REFERENCES parents(id))');
    db.execute('INSERT INTO children VALUES (1, NULL)');
    assert.equal(db.execute('SELECT * FROM children').rows[0].parent_id, null);
  });

  it('CASCADE deletes child rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO parents VALUES (1, 'A')");
    db.execute("INSERT INTO parents VALUES (2, 'B')");
    db.execute('CREATE TABLE children (id INT PRIMARY KEY, parent_id INT REFERENCES parents(id) ON DELETE CASCADE)');
    db.execute('INSERT INTO children VALUES (1, 1)');
    db.execute('INSERT INTO children VALUES (2, 1)');
    db.execute('INSERT INTO children VALUES (3, 2)');

    db.execute('DELETE FROM parents WHERE id = 1');
    assert.equal(db.execute('SELECT * FROM children').rows.length, 1);
    assert.equal(db.execute('SELECT * FROM children').rows[0].parent_id, 2);
  });

  it('RESTRICT prevents delete when children exist', () => {
    const db = new Database();
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY)');
    db.execute('INSERT INTO parents VALUES (1)');
    db.execute('CREATE TABLE children (id INT PRIMARY KEY, parent_id INT REFERENCES parents(id))');
    db.execute('INSERT INTO children VALUES (1, 1)');

    assert.throws(
      () => db.execute('DELETE FROM parents WHERE id = 1'),
      /Cannot delete.*referenced/
    );
  });

  it('SET NULL on delete', () => {
    const db = new Database();
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY)');
    db.execute('INSERT INTO parents VALUES (1)');
    db.execute('CREATE TABLE children (id INT PRIMARY KEY, parent_id INT REFERENCES parents(id) ON DELETE SET NULL)');
    db.execute('INSERT INTO children VALUES (1, 1)');

    db.execute('DELETE FROM parents WHERE id = 1');
    const r = db.execute('SELECT * FROM children');
    assert.equal(r.rows[0].parent_id, null);
  });
});
