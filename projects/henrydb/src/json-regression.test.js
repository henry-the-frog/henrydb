import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function val(db, sql) {
  return Object.values(db.execute(sql).rows[0])[0];
}

describe('JSON Scalar Functions', () => {
  let db;
  
  it('json_extract from object', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_extract('{"a": 42}', '$.a')`), 42);
  });

  it('json_extract nested', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_extract('{"a": {"b": 99}}', '$.a.b')`), 99);
  });

  it('json_extract from array', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_extract('[10, 20, 30]', '$[1]')`), 20);
  });

  it('json_extract returns null for missing path', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_extract('{"a": 1}', '$.b')`), null);
  });

  it('json_array_length', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_array_length('[1,2,3,4,5]')`), 5);
  });

  it('json_type for integer', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_type('42')`), 'integer');
  });

  it('json_type for object', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_type('{"a": 1}')`), 'object');
  });

  it('json_valid true', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_valid('{"a": 1}')`), 1);
  });

  it('json_valid false', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_valid('not json')`), 0);
  });

  it('json minifies', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json('  { "a" : 1 ,  "b" : 2 }  ')`), '{"a":1,"b":2}');
  });
});

describe('JSON Mutation Functions', () => {
  let db;

  it('json_set adds key', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_set('{"a": 1}', '$.b', 2)`), '{"a":1,"b":2}');
  });

  it('json_set overwrites key', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_set('{"a": 1}', '$.a', 99)`), '{"a":99}');
  });

  it('json_replace overwrites existing', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_replace('{"a": 1}', '$.a', 99)`), '{"a":99}');
  });

  it('json_replace ignores missing', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_replace('{"a": 1}', '$.b', 2)`), '{"a":1}');
  });

  it('json_insert adds missing', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_insert('{"a": 1}', '$.b', 2)`), '{"a":1,"b":2}');
  });

  it('json_insert keeps existing', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_insert('{"a": 1}', '$.a', 99)`), '{"a":1}');
  });

  it('json_remove removes key', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_remove('{"a": 1, "b": 2}', '$.b')`), '{"a":1}');
  });

  it('json_patch merges objects', () => {
    db = new Database();
    assert.strictEqual(
      val(db, `SELECT json_patch('{"a": 1, "b": 2}', '{"b": 99, "c": 3}')`),
      '{"a":1,"b":99,"c":3}'
    );
  });

  it('json_patch removes null keys', () => {
    db = new Database();
    assert.strictEqual(
      val(db, `SELECT json_patch('{"a": 1, "b": 2}', '{"b": null}')`),
      '{"a":1}'
    );
  });
});

describe('JSON Constructor Functions', () => {
  let db;

  it('json_object creates object', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_object('a', 1, 'b', 2)`), '{"a":1,"b":2}');
  });

  it('json_array creates array', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_array(1, 2, 3)`), '[1,2,3]');
  });

  it('json_quote quotes string', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_quote('hello')`), '"hello"');
  });

  it('json_quote quotes number', () => {
    db = new Database();
    assert.strictEqual(val(db, `SELECT json_quote(42)`), '42');
  });
});

describe('JSON Aggregate Functions', () => {
  let db;

  it('json_group_array collects values', () => {
    db = new Database();
    db.execute('CREATE TABLE t (v INT)');
    db.execute('INSERT INTO t VALUES (1), (2), (3)');
    assert.strictEqual(val(db, 'SELECT json_group_array(v) FROM t'), '[1,2,3]');
  });

  it('json_group_object creates key-value pairs', () => {
    db = new Database();
    db.execute("CREATE TABLE t (k TEXT, v INT)");
    db.execute("INSERT INTO t VALUES ('a', 1), ('b', 2)");
    const result = val(db, 'SELECT json_group_object(k, v) FROM t');
    const parsed = JSON.parse(result);
    assert.strictEqual(parsed.a, 1);
    assert.strictEqual(parsed.b, 2);
  });

  it('json_group_array with GROUP BY', () => {
    db = new Database();
    db.execute("CREATE TABLE users (name TEXT, dept TEXT)");
    db.execute("INSERT INTO users VALUES ('Alice', 'eng'), ('Bob', 'eng'), ('Charlie', 'hr')");
    const r = db.execute("SELECT dept, json_group_array(name) as names FROM users GROUP BY dept ORDER BY dept");
    assert.strictEqual(r.rows.length, 2);
    assert.ok(r.rows[0].names.includes('Alice'));
  });
});
