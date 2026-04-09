// sequences.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { SequenceManager } from './sequences.js';

let sm;

describe('SequenceManager', () => {
  beforeEach(() => { sm = new SequenceManager(); });

  test('create and nextval', () => {
    sm.create('user_id_seq');
    assert.equal(sm.nextval('user_id_seq'), 1);
    assert.equal(sm.nextval('user_id_seq'), 2);
    assert.equal(sm.nextval('user_id_seq'), 3);
  });

  test('currval after nextval', () => {
    sm.create('seq1');
    sm.nextval('seq1');
    assert.equal(sm.currval('seq1'), 1);
  });

  test('currval before nextval throws', () => {
    sm.create('seq1');
    assert.throws(() => sm.currval('seq1'), /not been called/);
  });

  test('custom start value', () => {
    sm.create('seq1', { start: 100 });
    assert.equal(sm.nextval('seq1'), 100);
    assert.equal(sm.nextval('seq1'), 101);
  });

  test('custom increment', () => {
    sm.create('seq1', { start: 0, increment: 5 });
    assert.equal(sm.nextval('seq1'), 0);
    assert.equal(sm.nextval('seq1'), 5);
    assert.equal(sm.nextval('seq1'), 10);
  });

  test('negative increment (descending)', () => {
    sm.create('countdown', { start: 10, increment: -1, minValue: 1, maxValue: 10 });
    assert.equal(sm.nextval('countdown'), 10);
    assert.equal(sm.nextval('countdown'), 9);
  });

  test('setval changes current value', () => {
    sm.create('seq1');
    sm.setval('seq1', 50);
    assert.equal(sm.nextval('seq1'), 51);
  });

  test('setval with isCalled=false', () => {
    sm.create('seq1');
    sm.setval('seq1', 50, false);
    assert.equal(sm.nextval('seq1'), 50); // Next call returns 50, not 51
  });

  test('CYCLE wraps around', () => {
    sm.create('cyc', { start: 1, maxValue: 3, cycle: true });
    assert.equal(sm.nextval('cyc'), 1);
    assert.equal(sm.nextval('cyc'), 2);
    assert.equal(sm.nextval('cyc'), 3);
    assert.equal(sm.nextval('cyc'), 1); // Wraps
  });

  test('no CYCLE throws at max', () => {
    sm.create('seq', { start: 1, maxValue: 2, cycle: false });
    sm.nextval('seq'); // 1
    sm.nextval('seq'); // 2
    assert.throws(() => sm.nextval('seq'), /maximum/);
  });

  test('drop sequence', () => {
    sm.create('temp');
    sm.drop('temp');
    assert.ok(!sm.has('temp'));
  });

  test('drop IF EXISTS', () => {
    assert.equal(sm.drop('nonexistent', true), false);
  });

  test('duplicate create throws', () => {
    sm.create('seq1');
    assert.throws(() => sm.create('seq1'), /already exists/);
  });

  test('IF NOT EXISTS', () => {
    sm.create('seq1');
    const info = sm.create('seq1', { ifNotExists: true });
    assert.ok(info);
  });

  test('restart resets sequence', () => {
    sm.create('seq1');
    sm.nextval('seq1');
    sm.nextval('seq1');
    sm.restart('seq1');
    assert.equal(sm.nextval('seq1'), 1);
  });

  test('createSerial', () => {
    const info = sm.createSerial('users', 'id');
    assert.ok(sm.has('users_id_seq'));
    assert.equal(info.ownedBy, 'users.id');
    assert.equal(sm.nextval('users_id_seq'), 1);
  });

  test('list sequences', () => {
    sm.create('a');
    sm.create('b');
    assert.equal(sm.list().length, 2);
  });

  test('case-insensitive', () => {
    sm.create('MySeq');
    assert.ok(sm.has('myseq'));
    assert.equal(sm.nextval('MYSEQ'), 1);
  });
});
