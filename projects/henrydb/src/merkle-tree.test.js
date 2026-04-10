// merkle-tree.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MerkleTree } from './merkle-tree.js';

describe('MerkleTree — Construction', () => {
  it('builds tree from data', () => {
    const tree = new MerkleTree(['a', 'b', 'c', 'd']);
    assert.ok(tree.root);
    assert.equal(tree.leafCount, 4);
  });

  it('root changes when any data changes', () => {
    const t1 = new MerkleTree(['a', 'b', 'c', 'd']);
    const t2 = new MerkleTree(['a', 'b', 'c', 'e']); // One difference
    assert.notEqual(t1.root, t2.root);
  });

  it('identical data produces identical root', () => {
    const t1 = new MerkleTree(['x', 'y', 'z']);
    const t2 = new MerkleTree(['x', 'y', 'z']);
    assert.equal(t1.root, t2.root);
  });
});

describe('MerkleTree — Proof Verification', () => {
  it('generates valid proof for each leaf', () => {
    const data = ['alpha', 'beta', 'gamma', 'delta'];
    const tree = new MerkleTree(data);
    
    for (let i = 0; i < data.length; i++) {
      const proof = tree.getProof(i);
      assert.ok(MerkleTree.verifyProof(data[i], i, proof, tree.root), `Proof should verify for leaf ${i}`);
    }
  });

  it('proof fails for wrong data', () => {
    const tree = new MerkleTree(['a', 'b', 'c', 'd']);
    const proof = tree.getProof(0);
    assert.ok(!MerkleTree.verifyProof('z', 0, proof, tree.root), 'Wrong data should not verify');
  });

  it('proof fails for wrong root', () => {
    const tree = new MerkleTree(['a', 'b', 'c', 'd']);
    const proof = tree.getProof(0);
    assert.ok(!MerkleTree.verifyProof('a', 0, proof, 'fake-root'));
  });
});

describe('MerkleTree — Diff (Anti-Entropy)', () => {
  it('no diff for identical trees', () => {
    const t1 = new MerkleTree(['a', 'b', 'c', 'd']);
    const t2 = new MerkleTree(['a', 'b', 'c', 'd']);
    assert.deepEqual(t1.diff(t2), []);
  });

  it('finds single differing leaf', () => {
    const t1 = new MerkleTree(['a', 'b', 'c', 'd']);
    const t2 = new MerkleTree(['a', 'b', 'X', 'd']);
    const diffs = t1.diff(t2);
    assert.ok(diffs.includes(2), `Should find diff at index 2, got ${diffs}`);
  });

  it('finds multiple differing leaves', () => {
    const t1 = new MerkleTree(['a', 'b', 'c', 'd']);
    const t2 = new MerkleTree(['X', 'b', 'c', 'Y']);
    const diffs = t1.diff(t2);
    assert.ok(diffs.includes(0));
    assert.ok(diffs.includes(3));
  });

  it('diff is efficient (only traverses differing branches)', () => {
    // 1024 leaves, only 1 difference
    const data1 = Array.from({ length: 1024 }, (_, i) => `item-${i}`);
    const data2 = [...data1];
    data2[512] = 'CHANGED';
    
    const t1 = new MerkleTree(data1);
    const t2 = new MerkleTree(data2);
    
    const diffs = t1.diff(t2);
    assert.ok(diffs.includes(512));
    assert.equal(diffs.length, 1);
  });
});

describe('MerkleTree — Large Scale', () => {
  it('handles 10K items', () => {
    const data = Array.from({ length: 10000 }, (_, i) => `record-${i}`);
    const t0 = performance.now();
    const tree = new MerkleTree(data);
    const buildMs = performance.now() - t0;
    
    assert.ok(tree.root);
    console.log(`    10K items: build=${buildMs.toFixed(1)}ms, root=${tree.root}`);
    
    // Verify a proof
    const proof = tree.getProof(5000);
    assert.ok(MerkleTree.verifyProof('record-5000', 5000, proof, tree.root));
  });

  it('diff of 10K items with 5 changes', () => {
    const data1 = Array.from({ length: 10000 }, (_, i) => `record-${i}`);
    const data2 = [...data1];
    data2[100] = 'X'; data2[2500] = 'Y'; data2[5000] = 'Z';
    data2[7500] = 'W'; data2[9999] = 'V';
    
    const t1 = new MerkleTree(data1);
    const t2 = new MerkleTree(data2);
    
    const t0 = performance.now();
    const diffs = t1.diff(t2);
    const diffMs = performance.now() - t0;
    
    console.log(`    10K items, 5 changes: diff=${diffMs.toFixed(1)}ms, found ${diffs.length} diffs`);
    assert.equal(diffs.length, 5);
  });
});
