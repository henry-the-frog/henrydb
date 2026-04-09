// snapshot.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SnapshotStore } from './snapshot.js';

describe('Snapshot', () => {
  it('snapshot preserves state at creation time', () => {
    const store = new SnapshotStore();
    store.set('key', 'v1');
    const snap = store.snapshot();
    store.set('key', 'v2');
    
    assert.equal(snap.get('key'), 'v1');
    assert.equal(store.get('key'), 'v2');
  });

  it('multiple snapshots', () => {
    const store = new SnapshotStore();
    store.set('x', 1);
    const s1 = store.snapshot();
    store.set('x', 2);
    const s2 = store.snapshot();
    
    assert.equal(s1.get('x'), 1);
    assert.equal(s2.get('x'), 2);
  });
});
