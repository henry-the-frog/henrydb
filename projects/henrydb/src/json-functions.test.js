// json-functions.test.js — JSON function tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('JSON Functions', () => {
  let db;
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE events (id INT, data TEXT)');
    db.execute(`INSERT INTO events VALUES (1, '{"name": "click", "x": 100, "y": 200, "tags": ["ui", "input"]}')`);
    db.execute(`INSERT INTO events VALUES (2, '{"name": "scroll", "distance": 50, "nested": {"depth": 3}}')`);
    db.execute(`INSERT INTO events VALUES (3, '{"name": "keypress", "key": "Enter", "mods": []}')`);
    db.execute(`INSERT INTO events VALUES (4, NULL)`);
  });

  describe('JSON_EXTRACT', () => {
    it('extracts string value', () => {
      const r = db.execute("SELECT JSON_EXTRACT(data, '$.name') AS name FROM events WHERE id = 1");
      assert.equal(r.rows[0].name, 'click');
    });

    it('extracts number value', () => {
      const r = db.execute("SELECT JSON_EXTRACT(data, '$.x') AS x FROM events WHERE id = 1");
      assert.equal(r.rows[0].x, 100);
    });

    it('extracts nested value', () => {
      const r = db.execute("SELECT JSON_EXTRACT(data, '$.nested.depth') AS depth FROM events WHERE id = 2");
      assert.equal(r.rows[0].depth, 3);
    });

    it('extracts array element', () => {
      const r = db.execute("SELECT JSON_EXTRACT(data, '$.tags[0]') AS tag FROM events WHERE id = 1");
      assert.equal(r.rows[0].tag, 'ui');
    });

    it('extracts array element 1', () => {
      const r = db.execute("SELECT JSON_EXTRACT(data, '$.tags[1]') AS tag FROM events WHERE id = 1");
      assert.equal(r.rows[0].tag, 'input');
    });

    it('returns null for missing path', () => {
      const r = db.execute("SELECT JSON_EXTRACT(data, '$.missing') AS val FROM events WHERE id = 1");
      assert.equal(r.rows[0].val, null);
    });

    it('returns null for NULL input', () => {
      const r = db.execute("SELECT JSON_EXTRACT(data, '$.name') AS val FROM events WHERE id = 4");
      assert.equal(r.rows[0].val, null);
    });

    it('returns JSON string for object extraction', () => {
      const r = db.execute("SELECT JSON_EXTRACT(data, '$.nested') AS obj FROM events WHERE id = 2");
      assert.equal(r.rows[0].obj, '{"depth":3}');
    });

    it('returns JSON string for array extraction', () => {
      const r = db.execute("SELECT JSON_EXTRACT(data, '$.tags') AS arr FROM events WHERE id = 1");
      assert.equal(r.rows[0].arr, '["ui","input"]');
    });

    it('extracts whole document with $', () => {
      const r = db.execute("SELECT JSON_EXTRACT(data, '$') AS doc FROM events WHERE id = 3");
      const parsed = JSON.parse(r.rows[0].doc);
      assert.equal(parsed.name, 'keypress');
    });
  });

  describe('JSON_TYPE', () => {
    it('returns object for JSON object', () => {
      const r = db.execute("SELECT JSON_TYPE(data) AS t FROM events WHERE id = 1");
      assert.equal(r.rows[0].t, 'object');
    });

    it('returns null for NULL', () => {
      const r = db.execute("SELECT JSON_TYPE(data) AS t FROM events WHERE id = 4");
      assert.equal(r.rows[0].t, 'null');
    });
  });

  describe('JSON_ARRAY_LENGTH', () => {
    it('returns length of top-level array', () => {
      db.execute(`INSERT INTO events VALUES (5, '[1, 2, 3, 4, 5]')`);
      const r = db.execute("SELECT JSON_ARRAY_LENGTH(data) AS len FROM events WHERE id = 5");
      assert.equal(r.rows[0].len, 5);
    });

    it('returns null for non-array', () => {
      const r = db.execute("SELECT JSON_ARRAY_LENGTH(data) AS len FROM events WHERE id = 1");
      assert.equal(r.rows[0].len, null);
    });

    it('works with nested JSON_EXTRACT', () => {
      const r = db.execute("SELECT JSON_ARRAY_LENGTH(JSON_EXTRACT(data, '$.tags')) AS len FROM events WHERE id = 1");
      assert.equal(r.rows[0].len, 2);
    });

    it('returns 0 for empty array', () => {
      const r = db.execute("SELECT JSON_ARRAY_LENGTH(JSON_EXTRACT(data, '$.mods')) AS len FROM events WHERE id = 3");
      assert.equal(r.rows[0].len, 0);
    });
  });

  describe('JSON_OBJECT', () => {
    it('creates simple object', () => {
      const r = db.execute("SELECT JSON_OBJECT('a', 1, 'b', 2) AS obj FROM events LIMIT 1");
      const parsed = JSON.parse(r.rows[0].obj);
      assert.equal(parsed.a, 1);
      assert.equal(parsed.b, 2);
    });

    it('creates object with string values', () => {
      const r = db.execute("SELECT JSON_OBJECT('name', 'test') AS obj FROM events LIMIT 1");
      const parsed = JSON.parse(r.rows[0].obj);
      assert.equal(parsed.name, 'test');
    });
  });

  describe('JSON_ARRAY', () => {
    it('creates array from values', () => {
      const r = db.execute("SELECT JSON_ARRAY(1, 2, 3) AS arr FROM events LIMIT 1");
      const parsed = JSON.parse(r.rows[0].arr);
      assert.deepEqual(parsed, [1, 2, 3]);
    });

    it('creates array with mixed types', () => {
      const r = db.execute("SELECT JSON_ARRAY(1, 'two', 3) AS arr FROM events LIMIT 1");
      const parsed = JSON.parse(r.rows[0].arr);
      assert.equal(parsed[0], 1);
      assert.equal(parsed[1], 'two');
    });
  });

  describe('JSON_VALID', () => {
    it('returns 1 for valid JSON', () => {
      const r = db.execute("SELECT JSON_VALID(data) AS valid FROM events WHERE id = 1");
      assert.equal(r.rows[0].valid, 1);
    });

    it('returns 0 for invalid JSON', () => {
      db.execute("INSERT INTO events VALUES (5, 'not json')");
      const r = db.execute("SELECT JSON_VALID(data) AS valid FROM events WHERE id = 5");
      assert.equal(r.rows[0].valid, 0);
    });

    it('returns 0 for NULL', () => {
      const r = db.execute("SELECT JSON_VALID(data) AS valid FROM events WHERE id = 4");
      assert.equal(r.rows[0].valid, 0);
    });
  });

  describe('JSON in WHERE', () => {
    it('filter by JSON value', () => {
      const r = db.execute("SELECT id FROM events WHERE JSON_EXTRACT(data, '$.name') = 'click'");
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].id, 1);
    });

    it('filter by numeric JSON value', () => {
      const r = db.execute("SELECT id FROM events WHERE JSON_EXTRACT(data, '$.x') > 50");
      assert.equal(r.rows.length, 1);
    });

    it('filter by JSON_VALID', () => {
      db.execute("INSERT INTO events VALUES (5, 'bad')");
      const r = db.execute("SELECT id FROM events WHERE JSON_VALID(data) = 1");
      assert.equal(r.rows.length, 3); // ids 1, 2, 3 (4 is NULL)
    });
  });

  describe('JSON with GROUP BY', () => {
    it('group by extracted JSON value', () => {
      db.execute("INSERT INTO events VALUES (5, '{\"name\": \"click\", \"x\": 50}')");
      const r = db.execute("SELECT JSON_EXTRACT(data, '$.name') AS event_name, COUNT(*) AS cnt FROM events WHERE data IS NOT NULL GROUP BY event_name ORDER BY cnt DESC");
      assert.ok(r.rows.length >= 2);
      const click = r.rows.find(r => r.event_name === 'click');
      assert.equal(click.cnt, 2);
    });
  });
});
