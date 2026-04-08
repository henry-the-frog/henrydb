// hashing2.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { jumpHash, RendezvousHashing, MaglevHashing } from './hashing2.js';

describe('Jump Consistent Hashing', () => {
  it('maps to valid bucket', () => {
    for (let i = 0; i < 100; i++) {
      const b = jumpHash(i, 10);
      assert.ok(b >= 0 && b < 10);
    }
  });

  it('distributes across buckets', () => {
    const counts = new Array(5).fill(0);
    for (let i = 0; i < 10000; i++) counts[jumpHash(i, 5)]++;
    assert.ok(counts.every(c => c > 1000)); // Roughly uniform
  });

  it('minimal disruption when adding bucket', () => {
    let moved = 0;
    for (let i = 0; i < 1000; i++) {
      if (jumpHash(i, 10) !== jumpHash(i, 11)) moved++;
    }
    console.log(`    Jump hash: ${moved}/1000 moved on 10→11 buckets`);
    assert.ok(moved < 200); // Roughly 1/11 should move
  });
});

describe('RendezvousHashing', () => {
  it('returns node', () => {
    const rh = new RendezvousHashing(['A', 'B', 'C']);
    const node = rh.getNode('key1');
    assert.ok(['A', 'B', 'C'].includes(node));
  });

  it('consistent mapping', () => {
    const rh = new RendezvousHashing(['A', 'B', 'C']);
    assert.equal(rh.getNode('key1'), rh.getNode('key1'));
  });

  it('minimal disruption on removal', () => {
    const rh1 = new RendezvousHashing(['A', 'B', 'C']);
    const rh2 = new RendezvousHashing(['A', 'B']); // C removed
    let moved = 0;
    for (let i = 0; i < 100; i++) {
      const k = `key_${i}`;
      const n1 = rh1.getNode(k);
      const n2 = rh2.getNode(k);
      if (n1 !== n2 && n1 !== 'C') moved++;
    }
    assert.ok(moved < 10); // Only keys on C should move
  });

  it('getNodes for replication', () => {
    const rh = new RendezvousHashing(['A', 'B', 'C', 'D']);
    const nodes = rh.getNodes('key1', 2);
    assert.equal(nodes.length, 2);
    assert.notEqual(nodes[0], nodes[1]);
  });
});

describe('MaglevHashing', () => {
  it('maps keys to nodes', () => {
    const mh = new MaglevHashing(['A', 'B', 'C'], 13);
    const node = mh.lookup('key1');
    assert.ok(['A', 'B', 'C'].includes(node));
  });

  it('consistent mapping', () => {
    const mh = new MaglevHashing(['A', 'B', 'C'], 13);
    assert.equal(mh.lookup('key1'), mh.lookup('key1'));
  });

  it('distributes across nodes', () => {
    const mh = new MaglevHashing(['A', 'B', 'C'], 997);
    const counts = { A: 0, B: 0, C: 0 };
    for (let i = 0; i < 1000; i++) counts[mh.lookup(`key_${i}`)]++;
    assert.ok(counts.A > 200 && counts.B > 200 && counts.C > 200);
  });

  it('empty nodes', () => {
    const mh = new MaglevHashing([]);
    assert.equal(mh.lookup('key'), null);
  });
});
