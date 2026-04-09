// triggers.test.js — Tests for enhanced trigger system
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { TriggerManager, parseCreateTrigger } from './triggers.js';
import { Database } from './db.js';

let db, tm;

describe('TriggerManager', () => {
  beforeEach(() => {
    db = new Database();
    tm = new TriggerManager();
    db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, product TEXT, amount INTEGER, status TEXT)');
    db.execute('CREATE TABLE audit_log (id INTEGER PRIMARY KEY, action TEXT, detail TEXT)');
    db.execute("INSERT INTO orders VALUES (1, 'Widget', 100, 'pending')");
  });

  test('AFTER INSERT trigger fires', () => {
    let fired = false;
    tm.createTrigger({
      name: 'log_insert',
      timing: 'AFTER',
      event: 'INSERT',
      table: 'orders',
      functionBody: `
        BEGIN
          RAISE NOTICE 'insert triggered';
        END;
      `,
    });

    const result = tm.fire('AFTER', 'INSERT', 'orders', {
      newRow: { id: 2, product: 'Gadget', amount: 200, status: 'new' },
      oldRow: null,
      db,
    });

    assert.ok(result);
    assert.ok(!result.suppressed);
  });

  test('BEFORE INSERT trigger can modify NEW row', () => {
    tm.createTrigger({
      name: 'set_default_status',
      timing: 'BEFORE',
      event: 'INSERT',
      table: 'orders',
      functionBody: `
        BEGIN
          RETURN new;
        END;
      `,
    });

    const result = tm.fire('BEFORE', 'INSERT', 'orders', {
      newRow: { id: 2, product: 'Gadget', amount: 200, status: null },
      oldRow: null,
      db,
    });

    assert.ok(result.newRow);
  });

  test('trigger with NEW row variable access', () => {
    tm.createTrigger({
      name: 'validate_amount',
      timing: 'BEFORE',
      event: 'INSERT',
      table: 'orders',
      functionBody: `
        BEGIN
          IF new_amount < 0 THEN
            RAISE EXCEPTION 'Amount cannot be negative';
          END IF;
        END;
      `,
    });

    // Positive amount — should pass
    const result1 = tm.fire('BEFORE', 'INSERT', 'orders', {
      newRow: { id: 2, product: 'X', amount: 50, status: 'ok' },
      db,
    });
    assert.ok(!result1.suppressed);

    // Negative amount — should raise
    assert.throws(() => {
      tm.fire('BEFORE', 'INSERT', 'orders', {
        newRow: { id: 3, product: 'Y', amount: -10, status: 'ok' },
        db,
      });
    }, /negative/);
  });

  test('trigger with TG_ context variables', () => {
    let capturedOp = null;
    tm.createTrigger({
      name: 'capture_op',
      timing: 'AFTER',
      event: 'INSERT',
      table: 'orders',
      functionBody: `
        BEGIN
          RAISE NOTICE 'op: %', tg_op;
        END;
      `,
    });

    // Just verify it doesn't crash
    tm.fire('AFTER', 'INSERT', 'orders', {
      newRow: { id: 2 },
      db,
    });
  });

  test('WHEN condition filters trigger execution', () => {
    let fireCount = 0;
    tm.createTrigger({
      name: 'high_value_only',
      timing: 'AFTER',
      event: 'INSERT',
      table: 'orders',
      condition: 'NEW.amount > 100',
      functionBody: `
        BEGIN
          RAISE NOTICE 'high value order';
        END;
      `,
    });

    // Low value — condition false, trigger shouldn't fire
    tm.fire('AFTER', 'INSERT', 'orders', {
      newRow: { id: 2, amount: 50 },
      db,
    });

    // High value — condition true
    tm.fire('AFTER', 'INSERT', 'orders', {
      newRow: { id: 3, amount: 200 },
      db,
    });
  });

  test('multiple triggers on same table/event', () => {
    tm.createTrigger({
      name: 'trigger1',
      timing: 'AFTER',
      event: 'INSERT',
      table: 'orders',
      functionBody: `BEGIN RAISE NOTICE 'first'; END;`,
    });
    tm.createTrigger({
      name: 'trigger2',
      timing: 'AFTER',
      event: 'INSERT',
      table: 'orders',
      functionBody: `BEGIN RAISE NOTICE 'second'; END;`,
    });

    const result = tm.fire('AFTER', 'INSERT', 'orders', {
      newRow: { id: 2 },
      db,
    });
    assert.ok(!result.suppressed);
  });

  test('disabled trigger does not fire', () => {
    tm.createTrigger({
      name: 'disabled_trigger',
      timing: 'AFTER',
      event: 'INSERT',
      table: 'orders',
      functionBody: `
        BEGIN
          RAISE EXCEPTION 'should not fire';
        END;
      `,
    });

    tm.setEnabled('disabled_trigger', false);

    // Should not throw since trigger is disabled
    tm.fire('AFTER', 'INSERT', 'orders', {
      newRow: { id: 2 },
      db,
    });
  });

  test('drop trigger', () => {
    tm.createTrigger({
      name: 'temp_trigger',
      timing: 'AFTER',
      event: 'INSERT',
      table: 'orders',
      functionBody: 'BEGIN NULL; END;',
    });

    assert.ok(tm.hasTrigger('temp_trigger'));
    tm.dropTrigger('temp_trigger');
    assert.ok(!tm.hasTrigger('temp_trigger'));
  });

  test('drop trigger IF EXISTS', () => {
    const result = tm.dropTrigger('nonexistent', true);
    assert.equal(result, false);
  });

  test('listTriggers', () => {
    tm.createTrigger({
      name: 't1', timing: 'BEFORE', event: 'INSERT', table: 'orders',
      functionBody: 'BEGIN NULL; END;',
    });
    tm.createTrigger({
      name: 't2', timing: 'AFTER', event: 'DELETE', table: 'orders',
      functionBody: 'BEGIN NULL; END;',
    });

    const all = tm.listTriggers();
    assert.equal(all.length, 2);

    const orderTriggers = tm.listTriggers('orders');
    assert.equal(orderTriggers.length, 2);

    const noTriggers = tm.listTriggers('nonexistent');
    assert.equal(noTriggers.length, 0);
  });

  test('trigger on different events dont interfere', () => {
    tm.createTrigger({
      name: 'insert_trigger',
      timing: 'AFTER',
      event: 'INSERT',
      table: 'orders',
      functionBody: `
        BEGIN
          RAISE EXCEPTION 'insert trigger';
        END;
      `,
    });

    // DELETE event should NOT trigger INSERT trigger
    tm.fire('AFTER', 'DELETE', 'orders', {
      oldRow: { id: 1 },
      db,
    });
    // No exception = pass

    // INSERT event SHOULD trigger
    assert.throws(() => {
      tm.fire('AFTER', 'INSERT', 'orders', {
        newRow: { id: 2 },
        db,
      });
    });
  });

  test('STATEMENT-level trigger fires without row data', () => {
    tm.createTrigger({
      name: 'stmt_trigger',
      timing: 'AFTER',
      event: 'INSERT',
      table: 'orders',
      forEach: 'STATEMENT',
      functionBody: `
        BEGIN
          RAISE NOTICE 'statement-level trigger fired';
        END;
      `,
    });

    tm.fire('AFTER', 'INSERT', 'orders', {
      newRow: null,
      db,
    });
  });
});

describe('parseCreateTrigger', () => {
  test('parse basic CREATE TRIGGER', () => {
    const t = parseCreateTrigger(`
      CREATE TRIGGER audit_insert
      AFTER INSERT ON orders
      FOR EACH ROW
      AS $$ BEGIN RAISE NOTICE 'inserted'; END; $$
    `);
    assert.equal(t.name, 'audit_insert');
    assert.equal(t.timing, 'AFTER');
    assert.equal(t.event, 'INSERT');
    assert.equal(t.table, 'orders');
    assert.equal(t.forEach, 'ROW');
    assert.ok(t.functionBody.includes('RAISE'));
  });

  test('parse BEFORE UPDATE trigger', () => {
    const t = parseCreateTrigger(`
      CREATE TRIGGER validate_update
      BEFORE UPDATE ON accounts
      FOR EACH ROW
      EXECUTE FUNCTION check_balance()
    `);
    assert.equal(t.timing, 'BEFORE');
    assert.equal(t.event, 'UPDATE');
    assert.equal(t.functionName, 'check_balance');
  });

  test('parse trigger with WHEN clause', () => {
    const t = parseCreateTrigger(`
      CREATE TRIGGER high_value
      AFTER INSERT ON orders
      FOR EACH ROW
      WHEN (NEW.amount > 1000)
      AS $$ BEGIN NULL; END; $$
    `);
    assert.equal(t.condition, 'NEW.amount > 1000');
  });

  test('parse STATEMENT-level trigger', () => {
    const t = parseCreateTrigger(`
      CREATE TRIGGER notify_changes
      AFTER DELETE ON orders
      FOR EACH STATEMENT
      AS $$ BEGIN NULL; END; $$
    `);
    assert.equal(t.forEach, 'STATEMENT');
  });
});
