// savepoints.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { SavepointManager } from './savepoints.js';

let spm;
let state;

describe('SavepointManager', () => {
  beforeEach(() => {
    state = { counter: 0, items: [] };
    spm = new SavepointManager({
      snapshotFn: () => ({ counter: state.counter, items: [...state.items] }),
      restoreFn: (snapshot) => {
        state.counter = snapshot.counter;
        state.items = [...snapshot.items];
      },
    });
  });

  test('SAVEPOINT creates a savepoint', () => {
    const result = spm.savepoint('sp1');
    assert.equal(result.name, 'sp1');
    assert.equal(result.depth, 1);
    assert.ok(spm.has('sp1'));
  });

  test('nested savepoints increase depth', () => {
    spm.savepoint('sp1');
    spm.savepoint('sp2');
    spm.savepoint('sp3');
    assert.equal(spm.depth, 3);
  });

  test('ROLLBACK TO restores state', () => {
    state.counter = 10;
    state.items = ['a', 'b'];
    spm.savepoint('sp1');
    
    state.counter = 99;
    state.items = ['a', 'b', 'c', 'd'];
    
    spm.rollbackTo('sp1');
    assert.equal(state.counter, 10);
    assert.deepEqual(state.items, ['a', 'b']);
  });

  test('ROLLBACK TO removes newer savepoints', () => {
    spm.savepoint('sp1');
    spm.savepoint('sp2');
    spm.savepoint('sp3');
    
    spm.rollbackTo('sp1');
    assert.equal(spm.depth, 1);
    assert.ok(spm.has('sp1'));
    assert.ok(!spm.has('sp2'));
    assert.ok(!spm.has('sp3'));
  });

  test('ROLLBACK TO keeps the target savepoint', () => {
    state.counter = 5;
    spm.savepoint('sp1');
    state.counter = 20;
    
    spm.rollbackTo('sp1');
    assert.equal(state.counter, 5);
    
    // Can rollback to sp1 again
    state.counter = 30;
    spm.rollbackTo('sp1');
    assert.equal(state.counter, 5);
  });

  test('RELEASE SAVEPOINT removes it and newer', () => {
    spm.savepoint('sp1');
    spm.savepoint('sp2');
    spm.savepoint('sp3');
    
    spm.release('sp2');
    assert.equal(spm.depth, 1);
    assert.ok(spm.has('sp1'));
    assert.ok(!spm.has('sp2'));
    assert.ok(!spm.has('sp3'));
  });

  test('RELEASE keeps older savepoints', () => {
    spm.savepoint('sp1');
    spm.savepoint('sp2');
    
    spm.release('sp2');
    assert.ok(spm.has('sp1'));
    assert.equal(spm.depth, 1);
  });

  test('duplicate savepoint name replaces', () => {
    state.counter = 1;
    spm.savepoint('sp1');
    
    state.counter = 2;
    spm.savepoint('sp1'); // Replace
    
    state.counter = 3;
    spm.rollbackTo('sp1');
    assert.equal(state.counter, 2); // Restored to second savepoint
  });

  test('non-existent savepoint throws on ROLLBACK TO', () => {
    assert.throws(() => spm.rollbackTo('nonexistent'), /does not exist/);
  });

  test('non-existent savepoint throws on RELEASE', () => {
    assert.throws(() => spm.release('nonexistent'), /does not exist/);
  });

  test('clear removes all savepoints', () => {
    spm.savepoint('sp1');
    spm.savepoint('sp2');
    const count = spm.clear();
    assert.equal(count, 2);
    assert.equal(spm.depth, 0);
  });

  test('list returns all active savepoints', () => {
    spm.savepoint('sp1');
    spm.savepoint('sp2');
    
    const list = spm.list();
    assert.equal(list.length, 2);
    assert.equal(list[0].name, 'sp1');
    assert.equal(list[0].depth, 1);
    assert.equal(list[1].name, 'sp2');
    assert.equal(list[1].depth, 2);
  });

  test('case-insensitive names', () => {
    spm.savepoint('MyPoint');
    assert.ok(spm.has('mypoint'));
    spm.rollbackTo('MYPOINT');
  });

  test('complex nested scenario', () => {
    state.counter = 0;
    state.items = [];

    spm.savepoint('a');
    state.counter = 1;
    state.items.push('x');

    spm.savepoint('b');
    state.counter = 2;
    state.items.push('y');

    spm.savepoint('c');
    state.counter = 3;
    state.items.push('z');

    // Rollback to b: should restore to state when b was created
    // At b creation: counter=1, items=['x']
    spm.rollbackTo('b');
    assert.equal(state.counter, 1);
    assert.deepEqual(state.items, ['x']);
    assert.equal(spm.depth, 2); // a and b remain

    // Now rollback to a
    spm.rollbackTo('a');
    assert.equal(state.counter, 0); // Original state at savepoint a
    assert.deepEqual(state.items, []);
    assert.equal(spm.depth, 1);
  });

  test('stats tracking', () => {
    spm.savepoint('sp1');
    spm.savepoint('sp2');
    spm.rollbackTo('sp1');
    spm.release('sp1');
    
    const stats = spm.getStats();
    assert.equal(stats.created, 2);
    assert.equal(stats.rolledBack, 1);
    assert.equal(stats.released, 1);
  });
});
