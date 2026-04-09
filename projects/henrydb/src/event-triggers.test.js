// event-triggers.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { EventTriggerManager } from './event-triggers.js';

let etm;

describe('EventTriggerManager', () => {
  beforeEach(() => { etm = new EventTriggerManager(); });

  test('create event trigger', () => {
    const info = etm.create('log_ddl', {
      event: 'ddl_command_end',
      handler: () => 'logged',
    });
    assert.equal(info.name, 'log_ddl');
    assert.equal(info.event, 'ddl_command_end');
  });

  test('fire event trigger', () => {
    let captured = null;
    etm.create('capture', {
      event: 'ddl_command_end',
      handler: (ctx) => { captured = ctx; return 'ok'; },
    });

    const results = etm.fire('ddl_command_end', {
      commandTag: 'CREATE TABLE',
      objectType: 'table',
      objectName: 'users',
    });

    assert.equal(results.length, 1);
    assert.equal(captured.commandTag, 'CREATE TABLE');
    assert.equal(captured.objectName, 'users');
  });

  test('tag filtering', () => {
    let fired = false;
    etm.create('only_drops', {
      event: 'ddl_command_end',
      tags: ['DROP TABLE', 'DROP INDEX'],
      handler: () => { fired = true; },
    });

    etm.fire('ddl_command_end', { commandTag: 'CREATE TABLE' });
    assert.ok(!fired);

    etm.fire('ddl_command_end', { commandTag: 'DROP TABLE' });
    assert.ok(fired);
  });

  test('ddl_command_start can abort', () => {
    etm.create('no_drops', {
      event: 'ddl_command_start',
      tags: ['DROP TABLE'],
      handler: () => { throw new Error('DROP not allowed'); },
    });

    assert.throws(
      () => etm.fire('ddl_command_start', { commandTag: 'DROP TABLE' }),
      /DROP not allowed/
    );
  });

  test('disabled trigger does not fire', () => {
    let fired = false;
    etm.create('t1', {
      event: 'ddl_command_end',
      handler: () => { fired = true; },
      enabled: false,
    });

    etm.fire('ddl_command_end', { commandTag: 'CREATE TABLE' });
    assert.ok(!fired);
  });

  test('ALTER enable/disable', () => {
    let count = 0;
    etm.create('t1', {
      event: 'ddl_command_end',
      handler: () => { count++; },
    });

    etm.fire('ddl_command_end', { commandTag: 'X' });
    assert.equal(count, 1);

    etm.alter('t1', { enabled: false });
    etm.fire('ddl_command_end', { commandTag: 'X' });
    assert.equal(count, 1); // Still 1
  });

  test('fire count tracked', () => {
    etm.create('counter', {
      event: 'ddl_command_end',
      handler: () => {},
    });

    etm.fire('ddl_command_end', { commandTag: 'A' });
    etm.fire('ddl_command_end', { commandTag: 'B' });

    const info = etm.list().find(t => t.name === 'counter');
    assert.equal(info.fireCount, 2);
  });

  test('multiple triggers on same event', () => {
    const log = [];
    etm.create('t1', { event: 'ddl_command_end', handler: () => log.push('t1') });
    etm.create('t2', { event: 'ddl_command_end', handler: () => log.push('t2') });

    etm.fire('ddl_command_end', { commandTag: 'X' });
    assert.deepEqual(log, ['t1', 't2']);
  });

  test('DROP event trigger', () => {
    etm.create('temp', { event: 'ddl_command_end', handler: () => {} });
    etm.drop('temp');
    assert.ok(!etm.has('temp'));
  });

  test('DROP IF EXISTS', () => {
    assert.equal(etm.drop('nonexistent', true), false);
  });

  test('invalid event throws', () => {
    assert.throws(() => etm.create('bad', { event: 'invalid_event', handler: () => {} }), /Unknown event/);
  });

  test('sql_drop event', () => {
    let dropped = null;
    etm.create('track_drops', {
      event: 'sql_drop',
      handler: (ctx) => { dropped = ctx.objectName; },
    });

    etm.fire('sql_drop', { commandTag: 'DROP TABLE', objectName: 'old_table' });
    assert.equal(dropped, 'old_table');
  });

  test('table_rewrite event', () => {
    let rewritten = null;
    etm.create('track_rewrite', {
      event: 'table_rewrite',
      handler: (ctx) => { rewritten = ctx.objectName; },
    });

    etm.fire('table_rewrite', { commandTag: 'ALTER TABLE', objectName: 'users' });
    assert.equal(rewritten, 'users');
  });

  test('list triggers', () => {
    etm.create('a', { event: 'ddl_command_start', handler: () => {} });
    etm.create('b', { event: 'ddl_command_end', handler: () => {} });
    assert.equal(etm.list().length, 2);
  });
});
