// table-heap.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { DataPage, TableHeap } from './table-heap.js';

describe('DataPage', () => {
  it('pin and dirty', () => {
    const p = new DataPage(0);
    p.pin();
    p.markDirty();
    assert.equal(p.isPinned, true);
    assert.equal(p.dirty, true);
    p.unpin();
    assert.equal(p.isPinned, false);
  });
});

describe('TableHeap', () => {
  it('insert and scan', () => {
    const heap = new TableHeap();
    heap.insert({ id: 1 }); heap.insert({ id: 2 });
    assert.equal([...heap.scan()].length, 2);
  });
});
