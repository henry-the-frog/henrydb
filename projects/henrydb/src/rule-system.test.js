// rule-system.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { RuleManager } from './rule-system.js';

let rm;

describe('RuleManager', () => {
  beforeEach(() => { rm = new RuleManager(); });

  test('create rule', () => {
    const info = rm.create('log_inserts', {
      table: 'orders',
      event: 'INSERT',
      type: 'ALSO',
      actions: [{ type: 'INSERT', table: 'order_log' }],
    });
    assert.equal(info.name, 'log_inserts');
    assert.equal(info.event, 'INSERT');
  });

  test('ALSO rule adds operations', () => {
    rm.create('audit', {
      table: 'accounts',
      event: 'UPDATE',
      type: 'ALSO',
      actions: [(op) => ({ type: 'INSERT', table: 'audit_log', data: { action: 'update' } })],
    });

    const result = rm.applyRules('accounts', 'UPDATE', { type: 'UPDATE', table: 'accounts' });
    assert.ok(result.modified);
    assert.equal(result.operations.length, 2); // Original + audit log
  });

  test('INSTEAD rule replaces operation', () => {
    rm.create('redirect_insert', {
      table: 'old_table',
      event: 'INSERT',
      type: 'INSTEAD',
      actions: [(op) => ({ ...op, table: 'new_table' })],
    });

    const result = rm.applyRules('old_table', 'INSERT', { type: 'INSERT', table: 'old_table', data: { id: 1 } });
    assert.ok(result.modified);
    assert.equal(result.operations.length, 1);
    assert.equal(result.operations[0].table, 'new_table');
  });

  test('no matching rules returns unmodified', () => {
    const result = rm.applyRules('orders', 'INSERT', { type: 'INSERT' });
    assert.ok(!result.modified);
    assert.equal(result.operations.length, 1);
  });

  test('conditional rule', () => {
    rm.create('high_value', {
      table: 'orders',
      event: 'INSERT',
      type: 'ALSO',
      condition: (row) => row.amount > 1000,
      actions: [() => ({ type: 'NOTIFY', channel: 'high_value_order' })],
    });

    const noMatch = rm.getRules('orders', 'INSERT', { amount: 500 });
    assert.equal(noMatch.length, 0);

    const match = rm.getRules('orders', 'INSERT', { amount: 2000 });
    assert.equal(match.length, 1);
  });

  test('DROP rule', () => {
    rm.create('temp_rule', { table: 'x', event: 'INSERT', actions: [] });
    rm.drop('temp_rule');
    assert.ok(!rm.has('temp_rule'));
  });

  test('DROP IF EXISTS', () => {
    assert.equal(rm.drop('nonexistent', true), false);
  });

  test('enable/disable rule', () => {
    rm.create('my_rule', { table: 'orders', event: 'INSERT', actions: [] });
    rm.setEnabled('my_rule', false);
    
    const rules = rm.getRules('orders', 'INSERT');
    assert.equal(rules.length, 0);
    
    rm.setEnabled('my_rule', true);
    assert.equal(rm.getRules('orders', 'INSERT').length, 1);
  });

  test('multiple rules on same table', () => {
    rm.create('rule1', { table: 'orders', event: 'INSERT', type: 'ALSO', actions: [() => ({ a: 1 })] });
    rm.create('rule2', { table: 'orders', event: 'INSERT', type: 'ALSO', actions: [() => ({ b: 2 })] });

    const result = rm.applyRules('orders', 'INSERT', { type: 'INSERT' });
    assert.equal(result.operations.length, 3); // original + 2 also
  });

  test('OR REPLACE', () => {
    rm.create('my_rule', { table: 'orders', event: 'INSERT', actions: [] });
    rm.create('my_rule', { table: 'orders', event: 'UPDATE', actions: [], orReplace: true });
    
    const rules = rm.listForTable('orders');
    assert.equal(rules.length, 1);
    assert.equal(rules[0].event, 'UPDATE');
  });

  test('listForTable', () => {
    rm.create('r1', { table: 'orders', event: 'INSERT', actions: [] });
    rm.create('r2', { table: 'orders', event: 'DELETE', actions: [] });
    rm.create('r3', { table: 'users', event: 'UPDATE', actions: [] });

    assert.equal(rm.listForTable('orders').length, 2);
    assert.equal(rm.listForTable('users').length, 1);
  });

  test('list all rules', () => {
    rm.create('r1', { table: 'a', event: 'INSERT', actions: [] });
    rm.create('r2', { table: 'b', event: 'DELETE', actions: [] });
    assert.equal(rm.list().length, 2);
  });

  test('event filtering', () => {
    rm.create('ins', { table: 'orders', event: 'INSERT', actions: [] });
    rm.create('upd', { table: 'orders', event: 'UPDATE', actions: [] });
    
    assert.equal(rm.getRules('orders', 'INSERT').length, 1);
    assert.equal(rm.getRules('orders', 'DELETE').length, 0);
  });

  test('case-insensitive', () => {
    rm.create('MyRule', { table: 'Orders', event: 'INSERT', actions: [] });
    assert.ok(rm.has('myrule'));
    assert.equal(rm.getRules('orders', 'INSERT').length, 1);
  });
});
