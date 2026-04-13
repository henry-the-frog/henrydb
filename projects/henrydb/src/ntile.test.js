// ntile.test.js — NTILE window function
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('NTILE', () => {
  it('divides 10 rows into 3 tiles (4,3,3)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    
    const r = db.execute('SELECT id, NTILE(3) OVER (ORDER BY id) AS tile FROM t');
    const tiles = r.rows.map(r => r.tile);
    assert.deepEqual(tiles, [1, 1, 1, 1, 2, 2, 2, 3, 3, 3]);
  });

  it('divides evenly: 6 rows into 3 tiles', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 6; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    
    const r = db.execute('SELECT id, NTILE(3) OVER (ORDER BY id) AS tile FROM t');
    const tiles = r.rows.map(r => r.tile);
    assert.deepEqual(tiles, [1, 1, 2, 2, 3, 3]);
  });

  it('works with PARTITION BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('a', 1), ('a', 2), ('a', 3), ('a', 4), ('b', 1), ('b', 2)");
    
    const r = db.execute('SELECT grp, val, NTILE(2) OVER (PARTITION BY grp ORDER BY val) AS tile FROM t ORDER BY grp, val');
    const aTiles = r.rows.filter(r => r.grp === 'a').map(r => r.tile);
    const bTiles = r.rows.filter(r => r.grp === 'b').map(r => r.tile);
    assert.deepEqual(aTiles, [1, 1, 2, 2]);
    assert.deepEqual(bTiles, [1, 2]);
  });
});
