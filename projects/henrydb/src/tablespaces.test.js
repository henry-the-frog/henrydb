// tablespaces.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { TablespaceManager } from './tablespaces.js';

let tsm;

describe('TablespaceManager', () => {
  beforeEach(() => { tsm = new TablespaceManager(); });

  test('built-in tablespaces exist', () => {
    assert.ok(tsm.has('pg_default'));
    assert.ok(tsm.has('pg_global'));
  });

  test('CREATE TABLESPACE', () => {
    const info = tsm.create('fast_ssd', '/mnt/ssd/data', { owner: 'henry' });
    assert.equal(info.name, 'fast_ssd');
    assert.equal(info.location, '/mnt/ssd/data');
    assert.equal(info.owner, 'henry');
  });

  test('duplicate create throws', () => {
    tsm.create('ts1', '/data/ts1');
    assert.throws(() => tsm.create('ts1', '/data/ts1b'), /already exists/);
  });

  test('SET TABLESPACE moves object', () => {
    tsm.create('fast', '/mnt/ssd');
    tsm.setTablespace('users', 'fast');
    assert.equal(tsm.getTablespace('users'), 'fast');
  });

  test('default tablespace is pg_default', () => {
    assert.equal(tsm.getTablespace('some_table'), 'pg_default');
  });

  test('move between tablespaces', () => {
    tsm.create('ts1', '/data/1');
    tsm.create('ts2', '/data/2');
    tsm.setTablespace('orders', 'ts1');
    tsm.setTablespace('orders', 'ts2');
    assert.equal(tsm.getTablespace('orders'), 'ts2');
    
    const ts1Info = tsm.getInfo('ts1');
    assert.equal(ts1Info.objectCount, 0);
  });

  test('DROP empty tablespace', () => {
    tsm.create('temp', '/tmp/data');
    tsm.drop('temp');
    assert.ok(!tsm.has('temp'));
  });

  test('DROP non-empty tablespace throws', () => {
    tsm.create('ts1', '/data/1');
    tsm.setTablespace('my_table', 'ts1');
    assert.throws(() => tsm.drop('ts1'), /not empty/);
  });

  test('DROP built-in tablespace throws', () => {
    assert.throws(() => tsm.drop('pg_default'), /Cannot drop built-in/);
  });

  test('DROP IF EXISTS', () => {
    assert.equal(tsm.drop('nonexistent', true), false);
  });

  test('ALTER TABLESPACE owner', () => {
    tsm.create('ts1', '/data/1');
    tsm.alter('ts1', { owner: 'admin' });
    const info = tsm.getInfo('ts1');
    assert.equal(info.owner, 'admin');
  });

  test('getInfo', () => {
    tsm.create('ts1', '/data/fast');
    tsm.setTablespace('tbl1', 'ts1');
    tsm.setTablespace('tbl2', 'ts1');
    tsm.updateSize('ts1', 1024000);
    
    const info = tsm.getInfo('ts1');
    assert.equal(info.objectCount, 2);
    assert.equal(info.sizeBytes, 1024000);
  });

  test('list tablespaces', () => {
    const initial = tsm.list().length;
    tsm.create('ts1', '/data/1');
    tsm.create('ts2', '/data/2');
    assert.equal(tsm.list().length, initial + 2);
  });

  test('case-insensitive', () => {
    tsm.create('MySpace', '/data/myspace');
    assert.ok(tsm.has('myspace'));
  });
});
