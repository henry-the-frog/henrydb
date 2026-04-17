// advanced-window-depth.test.js — Advanced window function tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-wfn-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE scores (id INT, name TEXT, score INT)');
  db.execute("INSERT INTO scores VALUES (1, 'Alice', 95)");
  db.execute("INSERT INTO scores VALUES (2, 'Bob', 87)");
  db.execute("INSERT INTO scores VALUES (3, 'Carol', 92)");
  db.execute("INSERT INTO scores VALUES (4, 'Dave', 78)");
  db.execute("INSERT INTO scores VALUES (5, 'Eve', 91)");
  db.execute("INSERT INTO scores VALUES (6, 'Frank', 85)");
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('LAG / LEAD', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('LAG returns previous row value', () => {
    const r = rows(db.execute(
      'SELECT name, score, LAG(score) OVER (ORDER BY score DESC) AS prev_score ' +
      'FROM scores ORDER BY score DESC'
    ));
    assert.equal(r[0].prev_score, null); // First row has no previous
    assert.equal(r[1].prev_score, 95);   // Second row's prev = first row
  });

  it('LEAD returns next row value', () => {
    const r = rows(db.execute(
      'SELECT name, score, LEAD(score) OVER (ORDER BY score DESC) AS next_score ' +
      'FROM scores ORDER BY score DESC'
    ));
    assert.equal(r[r.length - 1].next_score, null); // Last has no next
    assert.equal(r[0].next_score, r[1].score);
  });

  it('LAG with offset and default', () => {
    const r = rows(db.execute(
      'SELECT name, score, LAG(score, 2, 0) OVER (ORDER BY score DESC) AS prev2 ' +
      'FROM scores ORDER BY score DESC'
    ));
    assert.equal(r[0].prev2, 0); // Default value
    assert.equal(r[1].prev2, 0); // Still within offset
    assert.ok(r[2].prev2 > 0);  // Has a value 2 rows back
  });
});

describe('NTILE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('NTILE(2) splits into 2 groups', () => {
    const r = rows(db.execute(
      'SELECT name, NTILE(2) OVER (ORDER BY score DESC) AS tile ' +
      'FROM scores ORDER BY score DESC'
    ));
    assert.equal(r.length, 6);
    // First 3 in tile 1, last 3 in tile 2
    const tiles = r.map(x => x.tile);
    assert.equal(tiles.filter(t => t === 1).length, 3);
    assert.equal(tiles.filter(t => t === 2).length, 3);
  });

  it('NTILE(3) splits into 3 groups', () => {
    const r = rows(db.execute(
      'SELECT name, NTILE(3) OVER (ORDER BY score DESC) AS tile FROM scores'
    ));
    const tiles = r.map(x => x.tile);
    assert.equal(tiles.filter(t => t === 1).length, 2);
    assert.equal(tiles.filter(t => t === 2).length, 2);
    assert.equal(tiles.filter(t => t === 3).length, 2);
  });
});

describe('FIRST_VALUE / LAST_VALUE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('FIRST_VALUE returns first in window', () => {
    const r = rows(db.execute(
      'SELECT name, score, FIRST_VALUE(name) OVER (ORDER BY score DESC) AS top_name ' +
      'FROM scores ORDER BY score DESC'
    ));
    // Alice has highest score (95), should be FIRST_VALUE for all rows
    for (const row of r) {
      assert.equal(row.top_name, 'Alice');
    }
  });
});

describe('PERCENT_RANK / CUME_DIST', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('PERCENT_RANK ranges from 0 to 1', () => {
    const r = rows(db.execute(
      'SELECT name, score, PERCENT_RANK() OVER (ORDER BY score) AS pct_rank ' +
      'FROM scores ORDER BY score'
    ));
    assert.equal(r[0].pct_rank, 0); // Lowest score = 0
    assert.ok(r[r.length - 1].pct_rank <= 1);
  });

  it('CUME_DIST is always > 0 and <= 1', () => {
    const r = rows(db.execute(
      'SELECT name, score, CUME_DIST() OVER (ORDER BY score) AS cd ' +
      'FROM scores ORDER BY score'
    ));
    for (const row of r) {
      assert.ok(row.cd > 0 && row.cd <= 1, `CUME_DIST should be in (0,1], got ${row.cd}`);
    }
    // Last row should be 1.0
    assert.ok(Math.abs(r[r.length - 1].cd - 1.0) < 0.001);
  });
});
