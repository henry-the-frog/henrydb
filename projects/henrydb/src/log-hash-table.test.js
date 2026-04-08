// log-hash-table.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LogHashTable } from './log-hash-table.js';

describe('LogHashTable', () => {
  it('set/get', () => {
    const t = new LogHashTable();
    t.set('a', 1); t.set('b', 2);
    assert.equal(t.get('a'), 1);
    assert.equal(t.get('b'), 2);
  });

  it('update overwrites', () => {
    const t = new LogHashTable();
    t.set('k', 'old'); t.set('k', 'new');
    assert.equal(t.get('k'), 'new');
    assert.equal(t.size, 1);
    assert.equal(t.logSize, 2); // Both entries in log
  });

  it('delete with tombstone', () => {
    const t = new LogHashTable();
    t.set('x', 100);
    assert.ok(t.delete('x'));
    assert.equal(t.get('x'), undefined);
    assert.equal(t.size, 0);
    assert.equal(t.logSize, 2); // Original + tombstone
  });

  it('compact removes old entries', () => {
    const t = new LogHashTable();
    for (let i = 0; i < 100; i++) t.set('k', i); // 100 writes to same key
    assert.equal(t.logSize, 100);
    
    const result = t.compact();
    assert.equal(t.logSize, 1); // Only latest value
    assert.equal(result.reclaimed, 99);
    assert.equal(t.get('k'), 99);
  });

  it('compact removes deleted entries', () => {
    const t = new LogHashTable();
    t.set('a', 1); t.set('b', 2); t.set('c', 3);
    t.delete('b');
    
    t.compact();
    assert.equal(t.logSize, 2); // a and c
    assert.equal(t.get('a'), 1);
    assert.equal(t.get('b'), undefined);
  });

  it('1000 ops', () => {
    const t = new LogHashTable();
    for (let i = 0; i < 1000; i++) t.set(i, i * 10);
    for (let i = 0; i < 1000; i++) assert.equal(t.get(i), i * 10);
  });

  it('waste ratio tracking', () => {
    const t = new LogHashTable();
    for (let i = 0; i < 10; i++) t.set('x', i); // 10 writes, 1 live
    const stats = t.getStats();
    assert.ok(stats.wasteRatio.includes('90')); // ~90% waste
  });
});
