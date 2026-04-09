// audit-log.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { AuditLog } from './audit-log.js';

let log;

describe('AuditLog', () => {
  beforeEach(() => {
    log = new AuditLog({ maxEntries: 100 });
  });

  test('log connection event', () => {
    const entry = log.logConnect('s1', 'alice', '10.0.0.1');
    assert.ok(entry);
    assert.equal(entry.eventType, 'CONNECT');
    assert.equal(entry.user, 'alice');
  });

  test('log auth failure', () => {
    log.logAuthFailure('bob', '10.0.0.2', 'bad password');
    const events = log.query({ eventType: 'AUTH_FAILURE' });
    assert.equal(events.length, 1);
    assert.equal(events[0].success, false);
  });

  test('log DDL', () => {
    log.logDDL('s1', 'alice', 'CREATE TABLE users', 'TABLE', 'users');
    const events = log.query({ eventType: 'DDL' });
    assert.equal(events.length, 1);
    assert.equal(events[0].objectName, 'users');
  });

  test('log query with duration', () => {
    log.logQuery('s1', 'alice', 'SELECT * FROM users', { duration: 5.5, rowsAffected: 100 });
    const events = log.query({ eventType: 'QUERY' });
    assert.equal(events[0].duration, 5.5);
    assert.equal(events[0].rowsAffected, 100);
  });

  test('log error', () => {
    log.logError('s1', 'alice', 'SELECT * FROM nonexistent', 'table does not exist');
    const events = log.query({ success: false });
    assert.equal(events.length, 1);
    assert.ok(events[0].errorMessage.includes('table'));
  });

  test('query by user', () => {
    log.logConnect('s1', 'alice', '10.0.0.1');
    log.logConnect('s2', 'bob', '10.0.0.2');
    log.logConnect('s3', 'alice', '10.0.0.1');

    const aliceEvents = log.query({ user: 'alice' });
    assert.equal(aliceEvents.length, 2);
  });

  test('query by session', () => {
    log.logQuery('s1', 'alice', 'q1');
    log.logQuery('s2', 'bob', 'q2');
    log.logQuery('s1', 'alice', 'q3');

    const s1Events = log.query({ sessionId: 's1' });
    assert.equal(s1Events.length, 2);
  });

  test('query with limit', () => {
    for (let i = 0; i < 20; i++) {
      log.logQuery('s1', 'alice', `q${i}`);
    }
    const events = log.query({ limit: 5 });
    assert.equal(events.length, 5);
  });

  test('recent returns last N', () => {
    log.logConnect('s1', 'alice', '10.0.0.1');
    log.logQuery('s1', 'alice', 'SELECT 1');
    log.logDisconnect('s1', 'alice');

    const recent = log.recent(2);
    assert.equal(recent.length, 2);
    assert.equal(recent[1].eventType, 'DISCONNECT');
  });

  test('log rotation at capacity', () => {
    const small = new AuditLog({ maxEntries: 10 });
    for (let i = 0; i < 20; i++) {
      small.logQuery('s1', 'alice', `q${i}`);
    }
    assert.ok(small.getStats().currentEntries <= 10);
    assert.ok(small.getStats().rotations > 0);
  });

  test('disabled log skips entries', () => {
    const disabled = new AuditLog({ enabled: false });
    const entry = disabled.logConnect('s1', 'alice', '10.0.0.1');
    assert.equal(entry, null);
    assert.equal(disabled.getStats().totalEvents, 0);
  });

  test('log level filtering: ddl only', () => {
    const ddlOnly = new AuditLog({ logLevel: 'ddl' });
    ddlOnly.logConnect('s1', 'alice', '10.0.0.1');
    ddlOnly.logDDL('s1', 'alice', 'CREATE TABLE t', 'TABLE', 't');
    ddlOnly.logQuery('s1', 'alice', 'SELECT 1');

    assert.equal(ddlOnly.getStats().currentEntries, 1);
    assert.equal(ddlOnly.query({})[0].eventType, 'DDL');
  });

  test('exclude patterns', () => {
    const filtered = new AuditLog({ excludePatterns: [/^SELECT 1/] });
    filtered.logQuery('s1', 'alice', 'SELECT 1');
    filtered.logQuery('s1', 'alice', 'SELECT * FROM users');

    assert.equal(filtered.getStats().currentEntries, 1);
  });

  test('external sink receives events', () => {
    const received = [];
    log.addSink(entry => received.push(entry));

    log.logConnect('s1', 'alice', '10.0.0.1');
    assert.equal(received.length, 1);
  });

  test('clear removes all entries', () => {
    log.logConnect('s1', 'alice', '10.0.0.1');
    log.logConnect('s2', 'bob', '10.0.0.2');
    const cleared = log.clear();
    assert.equal(cleared, 2);
    assert.equal(log.getStats().currentEntries, 0);
  });

  test('stats by type', () => {
    log.logConnect('s1', 'alice', '10.0.0.1');
    log.logQuery('s1', 'alice', 'q1');
    log.logQuery('s1', 'alice', 'q2');
    log.logAuthFailure('bad', '10.0.0.3', 'fail');

    const stats = log.getStats();
    assert.equal(stats.byType.CONNECT, 1);
    assert.equal(stats.byType.QUERY, 2);
    assert.equal(stats.byType.AUTH_FAILURE, 1);
    assert.equal(stats.failedEvents, 1);
  });
});
