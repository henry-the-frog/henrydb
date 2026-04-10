// crdt.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { GCounter, PNCounter, ORSet, LWWRegister } from './crdt.js';

describe('GCounter', () => {
  it('increments and reads value', () => {
    const c = new GCounter('A');
    c.increment();
    c.increment();
    c.increment();
    assert.equal(c.value(), 3);
  });

  it('merge takes max per node', () => {
    const a = new GCounter('A');
    const b = new GCounter('B');
    a.increment(5);
    b.increment(3);
    
    // Simulate: both replicas diverge
    const a2 = a.clone();
    a2.increment(2); // A: 7
    b.increment(1);  // B: 4
    
    // Merge
    a2.merge(b);
    assert.equal(a2.value(), 7 + 4); // A:7 + B:4 = 11
  });

  it('merge is commutative and idempotent', () => {
    const a = new GCounter('A');
    const b = new GCounter('B');
    a.increment(3);
    b.increment(5);
    
    const ab = a.clone(); ab.merge(b);
    const ba = b.clone(); ba.merge(a);
    assert.equal(ab.value(), ba.value()); // Commutative
    
    ab.merge(b); // Merge again
    assert.equal(ab.value(), ba.value()); // Idempotent
  });
});

describe('PNCounter', () => {
  it('supports increment and decrement', () => {
    const c = new PNCounter('A');
    c.increment(10);
    c.decrement(3);
    assert.equal(c.value(), 7);
  });

  it('merge works across replicas', () => {
    const a = new PNCounter('A');
    const b = new PNCounter('B');
    
    a.increment(10);
    b.decrement(3);
    
    a.merge(b);
    assert.equal(a.value(), 7);
  });

  it('handles concurrent inc/dec', () => {
    const a = new PNCounter('A');
    const b = new PNCounter('B');
    
    a.increment(5);
    b.increment(3);
    a.decrement(2);
    b.decrement(1);
    
    a.merge(b);
    b.merge(a);
    
    // Both should agree: (5+3) - (2+1) = 5
    assert.equal(a.value(), 5);
    assert.equal(b.value(), 5);
  });
});

describe('ORSet', () => {
  it('add and remove', () => {
    const s = new ORSet('A');
    s.add('apple');
    s.add('banana');
    assert.ok(s.has('apple'));
    assert.ok(s.has('banana'));
    assert.equal(s.size, 2);
    
    s.remove('apple');
    assert.ok(!s.has('apple'));
    assert.equal(s.size, 1);
  });

  it('concurrent add wins over remove', () => {
    const a = new ORSet('A');
    const b = new ORSet('B');
    
    a.add('item');
    b.merge(a.clone()); // B now has 'item'
    
    // Concurrent: A removes, B adds again
    a.remove('item');
    b.add('item'); // New tag from B
    
    // Merge
    a.merge(b);
    
    // B's add should win (its tag wasn't observed by A's remove)
    assert.ok(a.has('item'), 'Concurrent add should win over remove');
  });

  it('merge is commutative', () => {
    const a = new ORSet('A');
    const b = new ORSet('B');
    
    a.add('x');
    a.add('y');
    b.add('y');
    b.add('z');
    
    const ab = a.clone(); ab.merge(b);
    const ba = b.clone(); ba.merge(a);
    
    assert.deepEqual(ab.values().sort(), ba.values().sort());
  });
});

describe('LWWRegister', () => {
  it('last write wins', () => {
    const a = new LWWRegister('A');
    a.set('first', 100);
    a.set('second', 200);
    assert.equal(a.get(), 'second');
  });

  it('merge picks higher timestamp', () => {
    const a = new LWWRegister('A');
    const b = new LWWRegister('B');
    
    a.set('from-A', 100);
    b.set('from-B', 200);
    
    a.merge(b);
    assert.equal(a.get(), 'from-B');
  });

  it('ignores older writes', () => {
    const a = new LWWRegister('A');
    a.set('new', 200);
    a.set('old', 100); // Older timestamp
    assert.equal(a.get(), 'new');
  });
});

describe('CRDTs — Distributed Scenario', () => {
  it('simulates 3-node replicated counter', () => {
    const nodes = [new PNCounter('A'), new PNCounter('B'), new PNCounter('C')];
    
    // Each node does local operations
    nodes[0].increment(10);
    nodes[1].increment(5);
    nodes[2].decrement(3);
    
    // Gossip: each node merges with all others
    for (let round = 0; round < 2; round++) {
      for (let i = 0; i < 3; i++) {
        for (let j = 0; j < 3; j++) {
          if (i !== j) nodes[i].merge(nodes[j]);
        }
      }
    }
    
    // All nodes should agree
    assert.equal(nodes[0].value(), 12); // 10 + 5 - 3
    assert.equal(nodes[1].value(), 12);
    assert.equal(nodes[2].value(), 12);
  });
});
