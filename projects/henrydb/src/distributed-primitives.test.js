// distributed-primitives.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LamportClock, VectorClock, GCounter, PNCounter, GossipProtocol } from './distributed-primitives.js';

describe('LamportClock', () => {
  it('tick increments', () => {
    const c = new LamportClock();
    assert.equal(c.tick(), 1);
    assert.equal(c.tick(), 2);
  });

  it('update takes max', () => {
    const c = new LamportClock();
    c.tick(); // 1
    c.update(5); // max(1,5)+1 = 6
    assert.equal(c.time, 6);
  });
});

describe('VectorClock', () => {
  it('tick increments own node', () => {
    const vc = new VectorClock('A');
    vc.tick();
    assert.equal(vc.clock.A, 1);
  });

  it('merge takes max per node', () => {
    const a = new VectorClock('A');
    const b = new VectorClock('B');
    a.tick(); a.tick(); // A:2
    b.tick(); // B:1
    
    a.merge(b.clock); // A:3, B:1
    assert.equal(a.clock.A, 3);
    assert.equal(a.clock.B, 1);
  });

  it('compare: before/after/concurrent', () => {
    const vc = new VectorClock('A');
    assert.equal(vc.compare({ A: 1 }, { A: 2 }), 'before');
    assert.equal(vc.compare({ A: 2 }, { A: 1 }), 'after');
    assert.equal(vc.compare({ A: 1, B: 2 }, { A: 2, B: 1 }), 'concurrent');
    assert.equal(vc.compare({ A: 1 }, { A: 1 }), 'equal');
  });
});

describe('GCounter', () => {
  it('increment and value', () => {
    const c = new GCounter('n1');
    c.increment(5);
    c.increment(3);
    assert.equal(c.value, 8);
  });

  it('merge from multiple nodes', () => {
    const c1 = new GCounter('n1');
    const c2 = new GCounter('n2');
    c1.increment(5);
    c2.increment(3);
    
    c1.merge(c2);
    assert.equal(c1.value, 8);
  });

  it('idempotent merge', () => {
    const c1 = new GCounter('n1');
    const c2 = new GCounter('n2');
    c1.increment(5);
    c2.increment(3);
    
    c1.merge(c2);
    c1.merge(c2); // Idempotent
    assert.equal(c1.value, 8);
  });
});

describe('PNCounter', () => {
  it('increment and decrement', () => {
    const c = new PNCounter('n1');
    c.increment(10);
    c.decrement(3);
    assert.equal(c.value, 7);
  });

  it('merge with decrements', () => {
    const c1 = new PNCounter('n1');
    const c2 = new PNCounter('n2');
    c1.increment(10);
    c2.decrement(3);
    
    c1.merge(c2);
    assert.equal(c1.value, 7);
  });
});

describe('GossipProtocol', () => {
  it('set and get', () => {
    const n1 = new GossipProtocol('n1', ['n2']);
    n1.set('x', 42);
    assert.equal(n1.get('x'), 42);
  });

  it('gossip propagates data', () => {
    const n1 = new GossipProtocol('n1', ['n2']);
    const n2 = new GossipProtocol('n2', ['n1']);
    
    n1.set('key1', 'value1');
    const msg = n1.createGossipMessage();
    n2.receiveGossipMessage(msg);
    
    assert.equal(n2.get('key1'), 'value1');
  });

  it('newer version wins', () => {
    const n1 = new GossipProtocol('n1', ['n2']);
    const n2 = new GossipProtocol('n2', ['n1']);
    
    n1.set('x', 'old');
    const msg1 = n1.createGossipMessage();
    n2.receiveGossipMessage(msg1);
    
    n1.set('x', 'new'); // Newer version
    const msg2 = n1.createGossipMessage();
    n2.receiveGossipMessage(msg2);
    
    assert.equal(n2.get('x'), 'new');
  });

  it('bidirectional gossip', () => {
    const n1 = new GossipProtocol('n1');
    const n2 = new GossipProtocol('n2');
    
    n1.set('a', 1);
    n2.set('b', 2);
    
    n1.receiveGossipMessage(n2.createGossipMessage());
    n2.receiveGossipMessage(n1.createGossipMessage());
    
    assert.equal(n1.get('b'), 2);
    assert.equal(n2.get('a'), 1);
  });
});
