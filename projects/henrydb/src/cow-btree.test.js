// cow-btree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { COWBTree } from './cow-btree.js';

describe('COW B-Tree', () => {
  it('basic set/get', () => {
    const tree = new COWBTree(4);
    tree.set(1, 'a'); tree.set(2, 'b'); tree.set(3, 'c');
    assert.equal(tree.get(1), 'a');
    assert.equal(tree.get(2), 'b');
  });

  it('update existing', () => {
    const tree = new COWBTree(4);
    tree.set(1, 'old'); tree.set(1, 'new');
    assert.equal(tree.get(1), 'new');
  });

  it('snapshot preserves old state', () => {
    const tree = new COWBTree(4);
    tree.set('x', 100);
    const snap = tree.snapshot();
    
    tree.set('x', 200); // New write
    
    assert.equal(tree.get('x'), 200); // Current: 200
    assert.equal(tree.getFromSnapshot(snap, 'x'), 100); // Snapshot: still 100
  });

  it('multiple snapshots', () => {
    const tree = new COWBTree(4);
    tree.set('a', 1);
    const s1 = tree.snapshot();
    
    tree.set('a', 2);
    const s2 = tree.snapshot();
    
    tree.set('a', 3);
    
    assert.equal(tree.getFromSnapshot(s1, 'a'), 1);
    assert.equal(tree.getFromSnapshot(s2, 'a'), 2);
    assert.equal(tree.get('a'), 3);
  });

  it('snapshot sees only data at snapshot time', () => {
    const tree = new COWBTree(4);
    tree.set('x', 10);
    const snap = tree.snapshot();
    
    tree.set('y', 20); // Added after snapshot
    
    assert.equal(tree.getFromSnapshot(snap, 'x'), 10);
    assert.equal(tree.getFromSnapshot(snap, 'y'), undefined); // Not in snapshot
  });

  it('many inserts with splits', () => {
    const tree = new COWBTree(4);
    for (let i = 0; i < 50; i++) tree.set(i, i * 10);
    
    assert.equal(tree.size, 50);
    for (let i = 0; i < 50; i++) assert.equal(tree.get(i), i * 10);
  });

  it('snapshot count', () => {
    const tree = new COWBTree(4);
    tree.snapshot();
    tree.snapshot();
    assert.equal(tree.snapshotCount, 2);
  });
});
