// vector-clock.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { VectorClock, DistributedNode, CausalHistory } from './vector-clock.js';

describe('VectorClock — Comparison', () => {
  it('detects happened-before', () => {
    const a = new VectorClock('A');
    a.increment();
    const b = a.clone();
    b.nodeId = 'B';
    b.increment();
    
    assert.equal(a.compare(b), 'before');
    assert.ok(a.happenedBefore(b));
  });

  it('detects concurrent events', () => {
    const a = new VectorClock('A');
    a.increment(); // A:1
    
    const b = new VectorClock('B');
    b.increment(); // B:1
    
    assert.equal(a.compare(b), 'concurrent');
    assert.ok(a.isConcurrent(b));
  });

  it('detects equal clocks', () => {
    const a = new VectorClock('A');
    a.increment();
    const b = a.clone();
    assert.equal(a.compare(b), 'equal');
  });

  it('merge takes pointwise max', () => {
    const a = new VectorClock('A');
    a._clock.set('A', 3);
    a._clock.set('B', 1);
    
    const b = new VectorClock('B');
    b._clock.set('A', 1);
    b._clock.set('B', 4);
    b._clock.set('C', 2);
    
    a.merge(b);
    assert.equal(a.get('A'), 3); // max(3,1)
    assert.equal(a.get('B'), 4); // max(1,4)
    assert.equal(a.get('C'), 2); // max(0,2)
  });
});

describe('DistributedNode — Message Passing', () => {
  it('send/receive maintains causality', () => {
    const alice = new DistributedNode('Alice');
    const bob = new DistributedNode('Bob');
    
    // Alice sends a message
    const msgClock = alice.send('hello');
    
    // Bob receives it
    bob.receive(msgClock, 'hello');
    
    // Bob's clock should reflect Alice's time
    assert.ok(bob.clock.get('Alice') >= 1);
    assert.ok(bob.clock.get('Bob') >= 1);
    
    // Alice's send happened-before Bob's receive
    assert.ok(alice.events[0].clock.happenedBefore(bob.events[0].clock));
  });

  it('concurrent events are detected', () => {
    const alice = new DistributedNode('Alice');
    const bob = new DistributedNode('Bob');
    
    // Both do local events without communicating
    const a1 = alice.localEvent('alice-work');
    const b1 = bob.localEvent('bob-work');
    
    assert.ok(a1.isConcurrent(b1));
  });

  it('causal chain: A → B → C', () => {
    const a = new DistributedNode('A');
    const b = new DistributedNode('B');
    const c = new DistributedNode('C');
    
    // A sends to B
    const msg1 = a.send('msg1');
    b.receive(msg1, 'msg1');
    
    // B sends to C
    const msg2 = b.send('msg2');
    c.receive(msg2, 'msg2');
    
    // A's send should happened-before C's receive
    assert.ok(a.events[0].clock.happenedBefore(c.events[0].clock));
  });
});

describe('CausalHistory — Conflict Detection', () => {
  it('no conflict on sequential writes', () => {
    const history = new CausalHistory();
    const a = new VectorClock('A');
    
    a.increment();
    const r1 = history.write('key', 'v1', a);
    assert.ok(!r1.conflict);
    
    a.increment();
    const r2 = history.write('key', 'v2', a);
    assert.ok(!r2.conflict);
    
    const versions = history.read('key');
    assert.equal(versions.length, 1);
    assert.equal(versions[0].value, 'v2');
  });

  it('detects concurrent write conflict', () => {
    const history = new CausalHistory();
    
    const a = new VectorClock('A');
    a.increment();
    history.write('key', 'value-from-A', a);
    
    const b = new VectorClock('B');
    b.increment();
    const result = history.write('key', 'value-from-B', b);
    
    assert.ok(result.conflict, 'Should detect conflict');
    assert.equal(result.siblings, 2);
    
    const versions = history.read('key');
    assert.equal(versions.length, 2);
    console.log(`    Conflict: ${versions.map(v => v.value).join(' vs ')}`);
  });

  it('resolve conflict with merged clock', () => {
    const history = new CausalHistory();
    
    const a = new VectorClock('A');
    a.increment();
    history.write('key', 'A-value', a);
    
    const b = new VectorClock('B');
    b.increment();
    history.write('key', 'B-value', b);
    
    // Resolve by merging both clocks
    const merged = a.clone().merge(b);
    merged.nodeId = 'A';
    merged.increment();
    history.resolve('key', 'merged-value', merged);
    
    const versions = history.read('key');
    assert.equal(versions.length, 1);
    assert.equal(versions[0].value, 'merged-value');
  });

  it('DynamoDB-style shopping cart conflict', () => {
    const history = new CausalHistory();
    
    // Initial cart
    const clock0 = new VectorClock('server');
    clock0.increment();
    history.write('cart:123', ['item-A'], clock0);
    
    // Two concurrent updates from different devices
    const phone = clock0.clone();
    phone.nodeId = 'phone';
    phone.increment();
    history.write('cart:123', ['item-A', 'item-B'], phone);
    
    const laptop = clock0.clone();
    laptop.nodeId = 'laptop';
    laptop.increment();
    const result = history.write('cart:123', ['item-A', 'item-C'], laptop);
    
    assert.ok(result.conflict);
    const versions = history.read('cart:123');
    assert.equal(versions.length, 2);
    console.log('    Cart conflict:');
    for (const v of versions) {
      console.log(`      ${v.clock.toString()} → [${v.value.join(', ')}]`);
    }
  });
});
