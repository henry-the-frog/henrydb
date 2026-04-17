// misc-sql-depth.test.js — Miscellaneous SQL feature depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-misc-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE items (id INT, name TEXT, price INT, category TEXT)');
  db.execute("INSERT INTO items VALUES (1, 'Widget', 1000, 'tools')");
  db.execute("INSERT INTO items VALUES (2, 'Gadget', 2500, 'electronics')");
  db.execute("INSERT INTO items VALUES (3, 'Doohickey', 500, 'tools')");
  db.execute("INSERT INTO items VALUES (4, 'Thingamajig', 3000, 'electronics')");
  db.execute("INSERT INTO items VALUES (5, 'Whatsit', 1500, 'tools')");
  db.execute("INSERT INTO items VALUES (6, 'Gizmo', NULL, 'electronics')");
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('BETWEEN', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('BETWEEN filters inclusive range', () => {
    const r = rows(db.execute('SELECT name FROM items WHERE price BETWEEN 1000 AND 2500 ORDER BY name'));
    assert.equal(r.length, 3); // Widget(1000), Gadget(2500), Whatsit(1500)
    assert.ok(r.some(x => x.name === 'Widget'));
    assert.ok(r.some(x => x.name === 'Gadget'));
    assert.ok(r.some(x => x.name === 'Whatsit'));
  });

  it('NOT BETWEEN excludes range', () => {
    const r = rows(db.execute('SELECT name FROM items WHERE price NOT BETWEEN 1000 AND 2500 AND price IS NOT NULL ORDER BY name'));
    assert.equal(r.length, 2); // Doohickey(500), Thingamajig(3000)
  });

  it('BETWEEN with NULLs excludes NULL values', () => {
    const r = rows(db.execute('SELECT name FROM items WHERE price BETWEEN 0 AND 10000'));
    // Gizmo has NULL price, should NOT appear
    assert.ok(!r.some(x => x.name === 'Gizmo'), 'NULL should not match BETWEEN');
  });
});

describe('CASE expressions', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('CASE WHEN in SELECT', () => {
    const r = rows(db.execute(
      "SELECT name, CASE WHEN price > 2000 THEN 'expensive' " +
      "WHEN price > 1000 THEN 'moderate' " +
      "ELSE 'cheap' END AS tier FROM items WHERE price IS NOT NULL ORDER BY name"
    ));
    assert.equal(r.find(x => x.name === 'Thingamajig').tier, 'expensive');
    assert.equal(r.find(x => x.name === 'Whatsit').tier, 'moderate');
    assert.equal(r.find(x => x.name === 'Doohickey').tier, 'cheap');
  });

  it('nested CASE expressions', () => {
    const r = rows(db.execute(
      "SELECT name, " +
      "CASE category WHEN 'tools' THEN " +
      "  CASE WHEN price > 1000 THEN 'premium-tool' ELSE 'basic-tool' END " +
      "ELSE 'non-tool' END AS label " +
      "FROM items WHERE price IS NOT NULL ORDER BY name"
    ));
    assert.equal(r.find(x => x.name === 'Whatsit').label, 'premium-tool');
    assert.equal(r.find(x => x.name === 'Doohickey').label, 'basic-tool');
    assert.equal(r.find(x => x.name === 'Gadget').label, 'non-tool');
  });

  it('CASE with aggregate', () => {
    const r = rows(db.execute(
      "SELECT category, " +
      "SUM(CASE WHEN price > 1500 THEN 1 ELSE 0 END) AS expensive_count, " +
      "SUM(CASE WHEN price <= 1500 THEN 1 ELSE 0 END) AS cheap_count " +
      "FROM items WHERE price IS NOT NULL GROUP BY category ORDER BY category"
    ));
    // electronics: expensive=2 (Gadget 2500, Thingamajig 3000), cheap=0
    // tools: expensive=0, cheap=3 (Widget 1000, Doohickey 500, Whatsit 1500)
    const elec = r.find(x => x.category === 'electronics');
    assert.equal(elec.expensive_count, 2);
    const tools = r.find(x => x.category === 'tools');
    assert.equal(tools.cheap_count, 3);
  });
});

describe('Multi-value IN', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('IN with multiple values', () => {
    const r = rows(db.execute("SELECT name FROM items WHERE name IN ('Widget', 'Gizmo', 'Gadget') ORDER BY name"));
    assert.equal(r.length, 3);
    assert.deepEqual(r.map(x => x.name), ['Gadget', 'Gizmo', 'Widget']);
  });

  it('NOT IN', () => {
    const r = rows(db.execute("SELECT name FROM items WHERE name NOT IN ('Widget', 'Gizmo') ORDER BY name"));
    assert.equal(r.length, 4);
  });

  it('IN with subquery', () => {
    const r = rows(db.execute(
      "SELECT name FROM items WHERE category IN (SELECT DISTINCT category FROM items WHERE price > 2000) ORDER BY name"
    ));
    // electronics has items > 2000, so all electronics items
    assert.ok(r.some(x => x.name === 'Gadget'));
    assert.ok(r.some(x => x.name === 'Gizmo'));
  });
});

describe('Compound Operators', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('arithmetic operators in SELECT', () => {
    const r = rows(db.execute('SELECT name, price * 2 AS doubled, price + 100 AS plus100 FROM items WHERE id = 1'));
    assert.equal(r[0].doubled, 2000);
    assert.equal(r[0].plus100, 1100);
  });

  it('modulo operator', () => {
    const r = rows(db.execute('SELECT name, price % 1000 AS remainder FROM items WHERE id = 2'));
    assert.equal(r[0].remainder, 500); // 2500 % 1000 = 500
  });

  it('string concatenation with ||', () => {
    const r = rows(db.execute("SELECT name || ' - ' || category AS label FROM items WHERE id = 1"));
    assert.equal(r[0].label, 'Widget - tools');
  });
});
