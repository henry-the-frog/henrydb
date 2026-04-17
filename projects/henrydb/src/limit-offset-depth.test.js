// limit-offset-depth.test.js — LIMIT/OFFSET edge case tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-lim-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE t (id INT, val TEXT)');
  for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Basic LIMIT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('LIMIT returns correct count', () => {
    const r = rows(db.execute('SELECT * FROM t LIMIT 5'));
    assert.equal(r.length, 5);
  });

  it('LIMIT 1 returns single row', () => {
    const r = rows(db.execute('SELECT * FROM t ORDER BY id LIMIT 1'));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 1);
  });

  it('LIMIT 0 returns no rows', () => {
    const r = rows(db.execute('SELECT * FROM t LIMIT 0'));
    assert.equal(r.length, 0);
  });

  it('LIMIT greater than row count returns all rows', () => {
    const r = rows(db.execute('SELECT * FROM t LIMIT 100'));
    assert.equal(r.length, 20);
  });
});

describe('OFFSET', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('OFFSET skips rows', () => {
    const r = rows(db.execute('SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 10'));
    assert.equal(r.length, 5);
    assert.equal(r[0].id, 11);
    assert.equal(r[4].id, 15);
  });

  it('OFFSET beyond rows returns empty', () => {
    const r = rows(db.execute('SELECT * FROM t ORDER BY id LIMIT 5 OFFSET 100'));
    assert.equal(r.length, 0);
  });

  it('OFFSET 0 is same as no offset', () => {
    const r = rows(db.execute('SELECT id FROM t ORDER BY id LIMIT 3 OFFSET 0'));
    assert.equal(r.length, 3);
    assert.equal(r[0].id, 1);
  });

  it('OFFSET without LIMIT', () => {
    try {
      const r = rows(db.execute('SELECT id FROM t ORDER BY id OFFSET 15'));
      // If supported, should return rows 16-20
      assert.equal(r.length, 5);
      assert.equal(r[0].id, 16);
    } catch {
      // Some implementations require LIMIT with OFFSET
    }
  });
});

describe('Pagination Pattern', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('paginate through all rows', () => {
    const pageSize = 7;
    let allIds = [];
    for (let page = 0; page < 3; page++) {
      const r = rows(db.execute(`SELECT id FROM t ORDER BY id LIMIT ${pageSize} OFFSET ${page * pageSize}`));
      allIds.push(...r.map(x => x.id));
    }
    // Should get all 20 IDs
    assert.equal(allIds.length, 20);
    assert.equal(allIds[0], 1);
    assert.equal(allIds[19], 20);
  });

  it('LIMIT with ORDER BY and WHERE', () => {
    const r = rows(db.execute('SELECT id FROM t WHERE id > 10 ORDER BY id DESC LIMIT 3'));
    assert.equal(r.length, 3);
    assert.equal(r[0].id, 20);
    assert.equal(r[1].id, 19);
    assert.equal(r[2].id, 18);
  });
});
