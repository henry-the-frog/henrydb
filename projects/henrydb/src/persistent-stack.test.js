// persistent-stack.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PersistentStack } from './persistent-stack.js';

describe('PersistentStack', () => {
  it('push and pop', () => {
    const s0 = PersistentStack.empty();
    const s1 = s0.push(1);
    const s2 = s1.push(2);
    const [val, s3] = s2.pop();
    assert.equal(val, 2);
    assert.equal(s3.peek(), 1);
  });

  it('persistence: old versions preserved', () => {
    const v1 = PersistentStack.empty().push('a');
    const v2 = v1.push('b');
    const v3 = v2.push('c');
    
    // All versions still valid
    assert.equal(v1.peek(), 'a');
    assert.equal(v2.peek(), 'b');
    assert.equal(v3.peek(), 'c');
    assert.equal(v1.size, 1);
    assert.equal(v3.size, 3);
  });

  it('structural sharing', () => {
    const base = PersistentStack.empty().push(1).push(2);
    const branch1 = base.push(3);
    const branch2 = base.push(4);
    
    // Both branches share the base (1, 2) nodes
    assert.equal(branch1.peek(), 3);
    assert.equal(branch2.peek(), 4);
    // Both still have base values
    assert.deepEqual(branch1.toArray(), [3, 2, 1]);
    assert.deepEqual(branch2.toArray(), [4, 2, 1]);
  });

  it('immutable: push returns new instance', () => {
    const s1 = PersistentStack.empty().push(1);
    const s2 = s1.push(2);
    assert.notEqual(s1, s2);
    assert.equal(s1.size, 1);
    assert.equal(s2.size, 2);
  });

  it('iterator', () => {
    const s = PersistentStack.empty().push(1).push(2).push(3);
    assert.deepEqual([...s], [3, 2, 1]);
  });

  it('reverse', () => {
    const s = PersistentStack.empty().push(1).push(2).push(3);
    const r = s.reverse();
    assert.deepEqual([...r], [1, 2, 3]);
  });

  it('stress: 10K pushes', () => {
    let s = PersistentStack.empty();
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) s = s.push(i);
    const elapsed = performance.now() - t0;
    assert.equal(s.size, 10000);
    console.log(`  10K push: ${elapsed.toFixed(1)}ms`);
  });

  it('use case: undo/redo', () => {
    let history = [PersistentStack.empty()]; // Stack of stacks
    let idx = 0;
    
    // Actions
    history.push(history[idx].push('type A')); idx++;
    history.push(history[idx].push('type B')); idx++;
    history.push(history[idx].push('type C')); idx++;
    
    // Undo
    idx--;
    assert.equal(history[idx].peek(), 'type B');
    
    // Undo again
    idx--;
    assert.equal(history[idx].peek(), 'type A');
    
    // Redo
    idx++;
    assert.equal(history[idx].peek(), 'type B');
  });
});
