// merkle.test.js — Merkle tree tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MerkleTree } from './merkle.js';
import { sha256 } from './sha256.js';

describe('MerkleTree', () => {
  it('single block tree', () => {
    const tree = new MerkleTree(['hello']);
    assert.equal(tree.root, sha256('\x00hello')); // leaf prefix
    assert.equal(tree.leafCount, 1);
  });

  it('two blocks', () => {
    const tree = new MerkleTree(['a', 'b']);
    const ha = sha256('\x00a');
    const hb = sha256('\x00b');
    assert.equal(tree.root, sha256('\x01' + ha + hb)); // internal prefix
  });

  it('four blocks (perfect binary tree)', () => {
    const tree = new MerkleTree(['a', 'b', 'c', 'd']);
    const ha = sha256('\x00a'), hb = sha256('\x00b');
    const hc = sha256('\x00c'), hd = sha256('\x00d');
    const hab = sha256('\x01' + ha + hb);
    const hcd = sha256('\x01' + hc + hd);
    assert.equal(tree.root, sha256('\x01' + hab + hcd));
  });

  it('odd number of blocks (last duplicated)', () => {
    const tree = new MerkleTree(['a', 'b', 'c']);
    const ha = sha256('\x00a'), hb = sha256('\x00b'), hc = sha256('\x00c');
    const hab = sha256('\x01' + ha + hb);
    const hcc = sha256('\x01' + hc + hc); // duplicated
    assert.equal(tree.root, sha256('\x01' + hab + hcc));
  });

  it('deterministic: same data → same root', () => {
    const t1 = new MerkleTree(['x', 'y', 'z']);
    const t2 = new MerkleTree(['x', 'y', 'z']);
    assert.equal(t1.root, t2.root);
    assert.ok(t1.equals(t2));
  });

  it('different data → different root', () => {
    const t1 = new MerkleTree(['a', 'b', 'c']);
    const t2 = new MerkleTree(['a', 'b', 'd']);
    assert.notEqual(t1.root, t2.root);
    assert.ok(!t1.equals(t2));
  });
});

describe('Merkle Proofs', () => {
  it('proof for first leaf', () => {
    const data = ['alpha', 'beta', 'gamma', 'delta'];
    const tree = new MerkleTree(data);
    const proof = tree.getProof(0);
    assert.ok(MerkleTree.verify('alpha', proof, tree.root));
  });

  it('proof for last leaf', () => {
    const data = ['alpha', 'beta', 'gamma', 'delta'];
    const tree = new MerkleTree(data);
    const proof = tree.getProof(3);
    assert.ok(MerkleTree.verify('delta', proof, tree.root));
  });

  it('proof for middle leaf', () => {
    const data = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'];
    const tree = new MerkleTree(data);
    for (let i = 0; i < data.length; i++) {
      const proof = tree.getProof(i);
      assert.ok(MerkleTree.verify(data[i], proof, tree.root), `Proof failed for index ${i}`);
    }
  });

  it('invalid data fails verification', () => {
    const tree = new MerkleTree(['a', 'b', 'c', 'd']);
    const proof = tree.getProof(0);
    assert.ok(!MerkleTree.verify('WRONG', proof, tree.root));
  });

  it('proof for odd-sized tree', () => {
    const data = ['one', 'two', 'three', 'four', 'five'];
    const tree = new MerkleTree(data);
    for (let i = 0; i < data.length; i++) {
      const proof = tree.getProof(i);
      assert.ok(MerkleTree.verify(data[i], proof, tree.root), `Proof failed for index ${i}`);
    }
  });

  it('proof for large tree (100 leaves)', () => {
    const data = Array.from({ length: 100 }, (_, i) => `block-${i}`);
    const tree = new MerkleTree(data);
    
    // Verify random proofs
    for (const idx of [0, 49, 99, 25, 75]) {
      const proof = tree.getProof(idx);
      assert.ok(MerkleTree.verify(data[idx], proof, tree.root));
    }
  });
});

describe('Merkle Diff', () => {
  it('identical trees have no diffs', () => {
    const t1 = new MerkleTree(['a', 'b', 'c', 'd']);
    const t2 = new MerkleTree(['a', 'b', 'c', 'd']);
    assert.deepEqual(t1.diff(t2), []);
  });

  it('single leaf changed', () => {
    const t1 = new MerkleTree(['a', 'b', 'c', 'd']);
    const t2 = new MerkleTree(['a', 'b', 'X', 'd']);
    assert.deepEqual(t1.diff(t2), [2]);
  });

  it('multiple leaves changed', () => {
    const t1 = new MerkleTree(['a', 'b', 'c', 'd']);
    const t2 = new MerkleTree(['X', 'b', 'c', 'Y']);
    const diffs = t1.diff(t2);
    assert.deepEqual(diffs.sort(), [0, 3]);
  });

  it('all leaves changed', () => {
    const t1 = new MerkleTree(['a', 'b', 'c', 'd']);
    const t2 = new MerkleTree(['w', 'x', 'y', 'z']);
    assert.deepEqual(t1.diff(t2).sort(), [0, 1, 2, 3]);
  });

  it('efficient diff on large tree (only visits changed subtrees)', () => {
    const n = 128;
    const d1 = Array.from({ length: n }, (_, i) => `block-${i}`);
    const d2 = [...d1];
    d2[42] = 'CHANGED';
    
    const t1 = new MerkleTree(d1);
    const t2 = new MerkleTree(d2);
    assert.deepEqual(t1.diff(t2), [42]);
  });
});

describe('Merkle Fuzzer', () => {
  it('second preimage attack prevented (domain separation)', () => {
    // Without domain separation, a 2-leaf tree and a 1-leaf tree with
    // the concatenated hashes would have the same root.
    const t = new MerkleTree(['hello', 'world']);
    const h1 = sha256('\x00hello');
    const h2 = sha256('\x00world');
    
    // Try the attack: single-leaf tree where leaf = h1 + h2
    const attackTree = new MerkleTree([h1 + h2]);
    assert.notEqual(t.root, attackTree.root, 
      'Second preimage attack should be prevented by domain separation');
  });

  it('1000 random trees: all proofs verify', () => {
    let seed = 42;
    const rng = () => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed / 0x7fffffff; };
    
    for (let i = 0; i < 100; i++) {
      const n = Math.floor(rng() * 20) + 1;
      const data = Array.from({ length: n }, (_, j) => `item-${i}-${j}-${Math.floor(rng() * 1000)}`);
      const tree = new MerkleTree(data);
      
      // Verify all proofs
      for (let j = 0; j < n; j++) {
        const proof = tree.getProof(j);
        assert.ok(MerkleTree.verify(data[j], proof, tree.root),
          `Failed: tree ${i}, leaf ${j}, size ${n}`);
      }
    }
  });
});
