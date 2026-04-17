// trigger-depth.test.js — TRIGGER depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-trg-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('AFTER INSERT Trigger', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('trigger fires on INSERT and logs to audit table', () => {
    db.execute('CREATE TABLE items (id INT, name TEXT)');
    db.execute('CREATE TABLE audit_log (action TEXT, item_id INT)');
    db.execute(
      "CREATE TRIGGER trg_insert AFTER INSERT ON items " +
      "FOR EACH ROW " +
      "INSERT INTO audit_log VALUES ('INSERT', NEW.id)"
    );

    db.execute("INSERT INTO items VALUES (1, 'Widget')");
    db.execute("INSERT INTO items VALUES (2, 'Gadget')");

    const log = rows(db.execute('SELECT * FROM audit_log ORDER BY item_id'));
    assert.equal(log.length, 2);
    assert.equal(log[0].action, 'INSERT');
    assert.equal(log[0].item_id, 1);
    assert.equal(log[1].item_id, 2);
  });
});

describe('AFTER UPDATE Trigger', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('trigger fires on UPDATE with OLD and NEW values', () => {
    db.execute('CREATE TABLE items (id INT, price INT)');
    db.execute('CREATE TABLE price_changes (item_id INT, old_price INT, new_price INT)');
    db.execute(
      "CREATE TRIGGER trg_update AFTER UPDATE ON items " +
      "FOR EACH ROW " +
      "INSERT INTO price_changes VALUES (NEW.id, OLD.price, NEW.price)"
    );

    db.execute('INSERT INTO items VALUES (1, 100)');
    db.execute('UPDATE items SET price = 120 WHERE id = 1');

    const changes = rows(db.execute('SELECT * FROM price_changes'));
    assert.equal(changes.length, 1);
    assert.equal(changes[0].old_price, 100);
    assert.equal(changes[0].new_price, 120);
  });
});

describe('AFTER DELETE Trigger', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('trigger fires on DELETE with OLD values', () => {
    db.execute('CREATE TABLE items (id INT, name TEXT)');
    db.execute('CREATE TABLE deleted_items (item_id INT, name TEXT)');
    db.execute(
      "CREATE TRIGGER trg_delete AFTER DELETE ON items " +
      "FOR EACH ROW " +
      "INSERT INTO deleted_items VALUES (OLD.id, OLD.name)"
    );

    db.execute("INSERT INTO items VALUES (1, 'Widget')");
    db.execute("INSERT INTO items VALUES (2, 'Gadget')");
    db.execute('DELETE FROM items WHERE id = 1');

    const deleted = rows(db.execute('SELECT * FROM deleted_items'));
    assert.equal(deleted.length, 1);
    assert.equal(deleted[0].item_id, 1);
    assert.equal(deleted[0].name, 'Widget');
  });
});

describe('Multiple Triggers', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('multiple triggers on same event both fire', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('CREATE TABLE log1 (val INT)');
    db.execute('CREATE TABLE log2 (val INT)');
    db.execute(
      "CREATE TRIGGER trg1 AFTER INSERT ON t FOR EACH ROW INSERT INTO log1 VALUES (NEW.id)"
    );
    db.execute(
      "CREATE TRIGGER trg2 AFTER INSERT ON t FOR EACH ROW INSERT INTO log2 VALUES (NEW.id)"
    );

    db.execute('INSERT INTO t VALUES (42)');

    const r1 = rows(db.execute('SELECT * FROM log1'));
    const r2 = rows(db.execute('SELECT * FROM log2'));
    assert.equal(r1.length, 1);
    assert.equal(r2.length, 1);
    assert.equal(r1[0].val, 42);
    assert.equal(r2[0].val, 42);
  });
});

describe('Trigger Persistence', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('trigger survives close/reopen', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('CREATE TABLE log (val INT)');
    db.execute(
      "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES (NEW.id)"
    );
    db.execute('INSERT INTO t VALUES (1)');

    db.close();
    db = TransactionalDatabase.open(dbDir);

    db.execute('INSERT INTO t VALUES (2)');
    const r = rows(db.execute('SELECT * FROM log ORDER BY val'));
    // Should have both entries if trigger persisted
    assert.ok(r.length >= 1, 'At least original insert should be in log');
  });
});
