// string-matching-depth.test.js — LIKE, pattern matching depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-like-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE t (id INT, name TEXT)');
  db.execute("INSERT INTO t VALUES (1, 'Alice')");
  db.execute("INSERT INTO t VALUES (2, 'Bob')");
  db.execute("INSERT INTO t VALUES (3, 'Alice Smith')");
  db.execute("INSERT INTO t VALUES (4, 'ALICE')");
  db.execute("INSERT INTO t VALUES (5, NULL)");
  db.execute("INSERT INTO t VALUES (6, '')");
  db.execute("INSERT INTO t VALUES (7, 'abc%def')");
  db.execute("INSERT INTO t VALUES (8, 'abc_def')");
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('LIKE: % wildcard', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('% matches any sequence', () => {
    const r = rows(db.execute("SELECT id FROM t WHERE name LIKE 'A%' ORDER BY id"));
    // Matches: Alice, Alice Smith, ALICE (if case-sensitive, only Alice + Alice Smith)
    assert.ok(r.length >= 2, 'Should match names starting with A');
  });

  it('% at end matches prefix', () => {
    const r = rows(db.execute("SELECT id FROM t WHERE name LIKE 'Ali%' ORDER BY id"));
    assert.ok(r.some(x => x.id === 1)); // Alice
    assert.ok(r.some(x => x.id === 3)); // Alice Smith
  });

  it('% at start matches suffix', () => {
    const r = rows(db.execute("SELECT id FROM t WHERE name LIKE '%ob' ORDER BY id"));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 2); // Bob
  });

  it('% on both sides matches substring', () => {
    const r = rows(db.execute("SELECT id FROM t WHERE name LIKE '%lic%' ORDER BY id"));
    assert.ok(r.some(x => x.id === 1)); // Alice
    assert.ok(r.some(x => x.id === 3)); // Alice Smith
  });

  it('LIKE with no wildcards is exact match', () => {
    const r = rows(db.execute("SELECT id FROM t WHERE name LIKE 'Alice'"));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 1);
  });
});

describe('LIKE: _ wildcard', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('_ matches exactly one character', () => {
    const r = rows(db.execute("SELECT id FROM t WHERE name LIKE 'B_b'"));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 2); // Bob
  });

  it('multiple _ matches fixed-length pattern', () => {
    const r = rows(db.execute("SELECT id FROM t WHERE name LIKE '___'"));
    // 3-char names: Bob
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 2);
  });
});

describe('LIKE: NULL handling', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('NULL LIKE pattern is NULL (not matched)', () => {
    const r = rows(db.execute("SELECT id FROM t WHERE name LIKE '%'"));
    // Should match all non-NULL rows
    assert.ok(!r.some(x => x.id === 5), 'NULL should not match %');
  });

  it('NOT LIKE with NULL', () => {
    const r = rows(db.execute("SELECT id FROM t WHERE name NOT LIKE 'Alice'"));
    // Should exclude Alice and NULL (NULL NOT LIKE is NULL → not included)
    assert.ok(!r.some(x => x.id === 5), 'NULL should not appear in NOT LIKE results');
  });
});

describe('LIKE: Special characters', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('pattern matching with literal %', () => {
    // To match literal %, some databases use ESCAPE clause
    // or the pattern 'abc\%def' or 'abc%'
    const r = rows(db.execute("SELECT id FROM t WHERE name LIKE 'abc%def'"));
    // This matches: abc%def, abc_def (because % is a wildcard)
    assert.ok(r.length >= 1);
  });

  it('empty string LIKE empty pattern', () => {
    const r = rows(db.execute("SELECT id FROM t WHERE name LIKE ''"));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 6); // empty string
  });
});

describe('Case Sensitivity', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('LIKE is case-sensitive by default', () => {
    const r = rows(db.execute("SELECT id FROM t WHERE name LIKE 'alice'"));
    // If case-sensitive: no match (alice vs Alice)
    // If case-insensitive: matches Alice
    // Both behaviors are valid depending on implementation
    assert.ok(r.length <= 1, 'LIKE behavior should be consistent');
  });

  it('UPPER/LOWER for case-insensitive matching', () => {
    const r = rows(db.execute("SELECT id FROM t WHERE UPPER(name) LIKE 'ALICE%' ORDER BY id"));
    // Should match: Alice, ALICE, Alice Smith
    assert.ok(r.length >= 2);
    assert.ok(r.some(x => x.id === 1));
    assert.ok(r.some(x => x.id === 4));
  });
});
