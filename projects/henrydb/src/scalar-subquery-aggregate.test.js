// scalar-subquery-aggregate.test.js — Scalar subqueries with aggregate functions
// Tests the parser fix for (SELECT MAX/MIN/SUM/AVG/COUNT(...) FROM ...) in expressions.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;
function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-sq-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE items (id INT, score INT, cat TEXT)');
  db.execute("INSERT INTO items VALUES (1, 10, 'a')");
  db.execute("INSERT INTO items VALUES (2, 30, 'a')");
  db.execute("INSERT INTO items VALUES (3, 50, 'b')");
  db.execute("INSERT INTO items VALUES (4, 70, 'b')");
  db.execute("INSERT INTO items VALUES (5, 90, 'c')");
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Scalar Subqueries with Aggregates', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('WHERE val > (SELECT AVG(...))', () => {
    const r = rows(db.execute(
      'SELECT id FROM items WHERE score > (SELECT AVG(score) FROM items) ORDER BY id'
    ));
    // AVG = (10+30+50+70+90)/5 = 50. Items with score > 50: 4 (70), 5 (90)
    assert.equal(r.length, 2);
    assert.equal(r[0].id, 4);
    assert.equal(r[1].id, 5);
  });

  it('WHERE val = (SELECT MAX(...))', () => {
    const r = rows(db.execute(
      'SELECT id FROM items WHERE score = (SELECT MAX(score) FROM items)'
    ));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 5);
  });

  it('WHERE val > (SELECT MIN(...))', () => {
    const r = rows(db.execute(
      'SELECT id FROM items WHERE score > (SELECT MIN(score) FROM items) ORDER BY id'
    ));
    // MIN = 10. Items with score > 10: 2,3,4,5
    assert.equal(r.length, 4);
  });

  it('WHERE val > (SELECT SUM(...))', () => {
    // SUM(score) = 250, items with score > 250: none
    // Use a single aggregate subquery without division
    const r = rows(db.execute(
      'SELECT id FROM items WHERE score > (SELECT SUM(score) FROM items WHERE cat = \'a\') ORDER BY id'
    ));
    // SUM for cat 'a' = 10+30 = 40. Items > 40: 3(50), 4(70), 5(90)
    assert.equal(r.length, 3);
    assert.equal(r[0].id, 3);
  });

  it('SELECT col + (SELECT scalar)', () => {
    db.execute('CREATE TABLE config (adj INT)');
    db.execute('INSERT INTO config VALUES (5)');
    
    const r = rows(db.execute(
      'SELECT id, score + (SELECT adj FROM config) as adjusted FROM items ORDER BY id'
    ));
    assert.equal(r.length, 5);
    assert.equal(r[0].adjusted, 15); // 10 + 5
    assert.equal(r[4].adjusted, 95); // 90 + 5
  });

  it('SELECT col * (SELECT scalar) with aggregate', () => {
    const r = rows(db.execute(
      'SELECT id, score * (SELECT COUNT(*) FROM items) as weighted FROM items WHERE id = 1'
    ));
    assert.equal(r.length, 1);
    assert.equal(r[0].weighted, 50); // 10 * 5
  });

  it('UPDATE ... SET col = (SELECT MAX(...))', () => {
    db.execute('UPDATE items SET score = (SELECT MAX(score) FROM items) WHERE id = 1');
    const r = rows(db.execute('SELECT score FROM items WHERE id = 1'));
    assert.equal(r[0].score, 90);
  });

  it('scalar subquery returning no rows gives NULL', () => {
    const r = rows(db.execute(
      'SELECT id, (SELECT score FROM items WHERE id = 999) as missing FROM items WHERE id = 1'
    ));
    assert.equal(r.length, 1);
    assert.equal(r[0].missing, null);
  });

  it('nested scalar subqueries', () => {
    db.execute('CREATE TABLE thresholds (name TEXT, val INT)');
    db.execute("INSERT INTO thresholds VALUES ('high', 60)");
    
    const r = rows(db.execute(
      "SELECT id FROM items WHERE score > (SELECT val FROM thresholds WHERE name = 'high') ORDER BY id"
    ));
    // Items with score > 60: 4 (70), 5 (90)
    assert.equal(r.length, 2);
    assert.equal(r[0].id, 4);
  });

  it('correlated scalar subquery with aggregate per category', () => {
    // This tests: for each item, check if score > average of its category
    const r = rows(db.execute(
      'SELECT id, score, cat FROM items i WHERE score > (SELECT AVG(score) FROM items i2 WHERE i2.cat = i.cat) ORDER BY id'
    ));
    // cat 'a': avg=20. id=2 (30) > 20 ✓
    // cat 'b': avg=60. id=4 (70) > 60 ✓  
    // cat 'c': avg=90. nothing > 90
    assert.equal(r.length, 2);
    assert.equal(r[0].id, 2);
    assert.equal(r[1].id, 4);
  });
});
