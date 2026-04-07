// json.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('JSON Functions', () => {
  it('JSON_EXTRACT extracts values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, data TEXT)');
    db.execute("INSERT INTO t VALUES (1, '{\"name\": \"Alice\", \"age\": 30}')");
    
    assert.equal(db.execute("SELECT JSON_EXTRACT(data, '$.name') AS name FROM t").rows[0].name, 'Alice');
    assert.equal(db.execute("SELECT JSON_EXTRACT(data, '$.age') AS age FROM t").rows[0].age, 30);
  });

  it('JSON_EXTRACT with nested path', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, data TEXT)');
    db.execute("INSERT INTO t VALUES (1, '{\"user\": {\"name\": \"Bob\"}}')");
    
    assert.equal(db.execute("SELECT JSON_EXTRACT(data, '$.user.name') AS name FROM t").rows[0].name, 'Bob');
  });

  it('JSON_ARRAY_LENGTH counts elements', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, data TEXT)');
    db.execute("INSERT INTO t VALUES (1, '[1, 2, 3]')");
    
    assert.equal(db.execute("SELECT JSON_ARRAY_LENGTH(data) AS len FROM t").rows[0].len, 3);
  });

  it('JSON_TYPE identifies types', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, data TEXT)');
    db.execute("INSERT INTO t VALUES (1, '{\"a\": 1}')");
    db.execute("INSERT INTO t VALUES (2, '[1, 2]')");
    
    assert.equal(db.execute("SELECT JSON_TYPE(data) AS jtype FROM t WHERE id = 1").rows[0].jtype, 'object');
    assert.equal(db.execute("SELECT JSON_TYPE(data) AS jtype FROM t WHERE id = 2").rows[0].jtype, 'array');
  });

  it('JSON_EXTRACT returns null for missing path', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, data TEXT)');
    db.execute("INSERT INTO t VALUES (1, '{\"a\": 1}')");
    
    assert.equal(db.execute("SELECT JSON_EXTRACT(data, '$.missing') AS val FROM t").rows[0].val, null);
  });
});
