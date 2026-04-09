// splay-tree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SplayTree } from './splay-tree.js';

describe('SplayTree', () => {
  it('insert and get', () => {
    const st = new SplayTree();
    st.insert(5, 'e'); st.insert(3, 'c'); st.insert(7, 'g');
    assert.equal(st.get(5), 'e');
    assert.equal(st.get(3), 'c');
    assert.equal(st.get(99), undefined);
  });

  it('recently accessed at root', () => {
    const st = new SplayTree();
    st.insert(1, 'a'); st.insert(2, 'b'); st.insert(3, 'c');
    st.get(1); // Splay 1 to root
    assert.equal(st._root.key, 1);
  });

  it('delete', () => {
    const st = new SplayTree();
    st.insert(1, 'a'); st.insert(2, 'b');
    assert.equal(st.delete(1), true);
    assert.equal(st.has(1), false);
    assert.equal(st.size, 1);
  });

  it('min and max', () => {
    const st = new SplayTree();
    [5, 3, 7, 1, 9].forEach(k => st.insert(k, k));
    assert.equal(st.min().key, 1);
    assert.equal(st.max().key, 9);
  });

  it('sorted iteration', () => {
    const st = new SplayTree();
    [5, 3, 7, 1, 9].forEach(k => st.insert(k, k));
    assert.deepEqual([...st].map(e => e.key), [1, 3, 5, 7, 9]);
  });

  it('working set: repeated access is fast', () => {
    const st = new SplayTree();
    for (let i = 0; i < 10000; i++) st.insert(i, i);
    
    // Access same 10 keys repeatedly (should splay to top)
    const hot = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000];
    const t0 = performance.now();
    for (let r = 0; r < 10000; r++) {
      st.get(hot[r % hot.length]);
    }
    const hotMs = performance.now() - t0;
    
    // Random access
    const t1 = performance.now();
    for (let r = 0; r < 10000; r++) {
      st.get(Math.floor(Math.random() * 10000));
    }
    const coldMs = performance.now() - t1;
    
    console.log(`  10K hot access: ${hotMs.toFixed(1)}ms, 10K random: ${coldMs.toFixed(1)}ms`);
    console.log(`  Working set speedup: ${(coldMs/hotMs).toFixed(1)}x`);
  });

  it('stress: 10K sequential', () => {
    const st = new SplayTree();
    for (let i = 0; i < 10000; i++) st.insert(i, i);
    assert.equal(st.size, 10000);
    assert.equal(st.get(5000), 5000);
  });
});
