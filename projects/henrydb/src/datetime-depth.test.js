// datetime-depth.test.js — Date/time function depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-dt-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('NOW() and CURRENT_TIMESTAMP', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('NOW() returns valid timestamp', () => {
    const r = rows(db.execute('SELECT NOW() AS ts'));
    assert.ok(r[0].ts, 'NOW() should return a value');
    // Should be parseable as a date
    const d = new Date(r[0].ts);
    assert.ok(!isNaN(d.getTime()), 'NOW() should return a valid date');
  });

  it('CURRENT_DATE returns date', () => {
    const r = rows(db.execute('SELECT CURRENT_DATE AS d'));
    assert.ok(r[0].d);
    assert.ok(r[0].d.match(/^\d{4}-\d{2}-\d{2}/), 'Should be date format');
  });

  it('CURRENT_TIMESTAMP returns timestamp', () => {
    const r = rows(db.execute('SELECT CURRENT_TIMESTAMP AS ts'));
    assert.ok(r[0].ts);
  });
});

describe('EXTRACT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('EXTRACT YEAR', () => {
    const r = rows(db.execute("SELECT EXTRACT(YEAR FROM '2024-06-15') AS y"));
    assert.equal(r[0].y, 2024);
  });

  it('EXTRACT MONTH', () => {
    const r = rows(db.execute("SELECT EXTRACT(MONTH FROM '2024-06-15') AS m"));
    assert.equal(r[0].m, 6);
  });

  it('EXTRACT DAY', () => {
    const r = rows(db.execute("SELECT EXTRACT(DAY FROM '2024-06-15') AS d"));
    assert.equal(r[0].d, 15);
  });
});

describe('Date Comparisons', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('compare dates with < and >', () => {
    db.execute('CREATE TABLE events (id INT, name TEXT, event_date TEXT)');
    db.execute("INSERT INTO events VALUES (1, 'Past', '2020-01-01')");
    db.execute("INSERT INTO events VALUES (2, 'Recent', '2024-06-01')");
    db.execute("INSERT INTO events VALUES (3, 'Future', '2030-12-31')");

    const r = rows(db.execute("SELECT name FROM events WHERE event_date > '2024-01-01' ORDER BY name"));
    assert.equal(r.length, 2); // Future and Recent
  });

  it('ORDER BY date', () => {
    db.execute('CREATE TABLE events (id INT, dt TEXT)');
    db.execute("INSERT INTO events VALUES (1, '2024-03-15')");
    db.execute("INSERT INTO events VALUES (2, '2024-01-01')");
    db.execute("INSERT INTO events VALUES (3, '2024-12-25')");

    const r = rows(db.execute('SELECT id FROM events ORDER BY dt'));
    assert.equal(r[0].id, 2); // Jan 1
    assert.equal(r[1].id, 1); // Mar 15
    assert.equal(r[2].id, 3); // Dec 25
  });
});

describe('Date in WHERE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('BETWEEN with dates', () => {
    db.execute('CREATE TABLE logs (id INT, ts TEXT)');
    db.execute("INSERT INTO logs VALUES (1, '2024-01-15')");
    db.execute("INSERT INTO logs VALUES (2, '2024-06-01')");
    db.execute("INSERT INTO logs VALUES (3, '2024-12-25')");

    const r = rows(db.execute("SELECT id FROM logs WHERE ts BETWEEN '2024-01-01' AND '2024-06-30'"));
    assert.equal(r.length, 2); // Jan 15, Jun 01
  });
});
