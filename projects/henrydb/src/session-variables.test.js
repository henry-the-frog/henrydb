// session-variables.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { SessionVariables } from './session-variables.js';

let sv;

describe('SessionVariables', () => {
  beforeEach(() => { sv = new SessionVariables(); });

  test('SHOW default value', () => {
    assert.equal(sv.show('client_encoding'), 'UTF8');
    assert.equal(sv.show('timezone'), 'UTC');
  });

  test('SET and SHOW', () => {
    sv.set('timezone', 'America/Denver');
    assert.equal(sv.show('timezone'), 'America/Denver');
  });

  test('SET is case-insensitive', () => {
    sv.set('TimeZone', 'US/Pacific');
    assert.equal(sv.show('timezone'), 'US/Pacific');
  });

  test('RESET restores default', () => {
    sv.set('timezone', 'US/Eastern');
    sv.reset('timezone');
    assert.equal(sv.show('timezone'), 'UTC');
  });

  test('RESET ALL', () => {
    sv.set('timezone', 'X');
    sv.set('work_mem', '16MB');
    sv.reset('all');
    assert.equal(sv.show('timezone'), 'UTC');
    assert.equal(sv.show('work_mem'), '4MB');
  });

  test('SET LOCAL overrides session', () => {
    sv.set('work_mem', '8MB');
    sv.setLocal('work_mem', '16MB');
    assert.equal(sv.show('work_mem'), '16MB');
  });

  test('SET LOCAL discarded on commit', () => {
    sv.set('work_mem', '8MB');
    sv.setLocal('work_mem', '16MB');
    sv.commitTransaction();
    assert.equal(sv.show('work_mem'), '8MB');
  });

  test('SET LOCAL discarded on rollback', () => {
    sv.setLocal('work_mem', '16MB');
    sv.rollbackTransaction();
    assert.equal(sv.show('work_mem'), '4MB');
  });

  test('savepoint/rollback restores local', () => {
    sv.setLocal('work_mem', '8MB');
    sv.savepoint();
    sv.setLocal('work_mem', '16MB');
    assert.equal(sv.show('work_mem'), '16MB');
    sv.rollbackToSavepoint();
    assert.equal(sv.show('work_mem'), '8MB');
  });

  test('SHOW ALL returns all params', () => {
    const all = sv.showAll();
    assert.ok('search_path' in all);
    assert.ok('timezone' in all);
    assert.ok('work_mem' in all);
  });

  test('SHOW unknown param throws', () => {
    assert.throws(() => sv.show('nonexistent_param'), /Unrecognized/);
  });

  test('getBoolean', () => {
    assert.ok(sv.getBoolean('enable_seqscan'));
    sv.set('enable_seqscan', 'off');
    assert.ok(!sv.getBoolean('enable_seqscan'));
  });

  test('getFloat', () => {
    assert.equal(sv.getFloat('random_page_cost'), 4.0);
    assert.equal(sv.getFloat('seq_page_cost'), 1.0);
  });

  test('getMemoryBytes', () => {
    assert.equal(sv.getMemoryBytes('work_mem'), 4 * 1024 * 1024);
    assert.equal(sv.getMemoryBytes('maintenance_work_mem'), 64 * 1024 * 1024);
    assert.equal(sv.getMemoryBytes('effective_cache_size'), 4 * 1024 ** 3);
  });

  test('custom server defaults', () => {
    const sv2 = new SessionVariables({ timezone: 'US/Mountain', custom_param: 'hello' });
    assert.equal(sv2.show('timezone'), 'US/Mountain');
    assert.equal(sv2.show('custom_param'), 'hello');
  });

  test('search_path default', () => {
    assert.equal(sv.show('search_path'), '"$user", public');
  });
});
