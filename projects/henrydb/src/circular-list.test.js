// circular-list.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CircularList } from './circular-list.js';

describe('CircularList', () => {
  it('add and iterate', () => {
    const cl = new CircularList();
    cl.add(1); cl.add(2); cl.add(3);
    assert.deepEqual([...cl], [1, 2, 3]);
  });

  it('remove', () => {
    const cl = new CircularList();
    const n = cl.add(1); cl.add(2);
    cl.remove(n);
    assert.deepEqual([...cl], [2]);
  });
});
