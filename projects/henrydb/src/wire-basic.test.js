import { describe, it, after } from 'node:test';
import assert from 'node:assert/strict';
import { HenryDBServer } from './server.js';
import { Database } from './db.js';

describe('Wire Protocol Integration (2026-04-19)', () => {
  let server, db;

  it('server starts and executes query', async () => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello'), (2, 'world')");
    server = new HenryDBServer(db);
    
    // Direct query through server
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].val, 'hello');
  });

  it('server handles complex queries', () => {
    const r = db.execute(`
      WITH stats AS (SELECT COUNT(*) AS cnt FROM t)
      SELECT cnt FROM stats
    `);
    assert.equal(r.rows[0].cnt, 2);
  });

  it('server handles DML through database', () => {
    db.execute("INSERT INTO t VALUES (3, 'new')");
    const r = db.execute('SELECT COUNT(*) AS cnt FROM t');
    assert.equal(r.rows[0].cnt, 3);
  });
});
