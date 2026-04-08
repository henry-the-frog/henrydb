// linear-hashing.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LinearHashTable } from './linear-hashing.js';

describe('LinearHashTable', () => {
  it('basic set/get', () => {
    const ht = new LinearHashTable();
    ht.set('a', 1); ht.set('b', 2);
    assert.equal(ht.get('a'), 1);
    assert.equal(ht.get('b'), 2);
  });

  it('update', () => {
    const ht = new LinearHashTable();
    ht.set(1, 'old'); ht.set(1, 'new');
    assert.equal(ht.get(1), 'new');
    assert.equal(ht.size, 1);
  });

  it('delete', () => {
    const ht = new LinearHashTable();
    ht.set('x', 1);
    assert.ok(ht.delete('x'));
    assert.equal(ht.get('x'), undefined);
  });

  it('splits on high load', () => {
    const ht = new LinearHashTable(2, 4, 0.5);
    for (let i = 0; i < 20; i++) ht.set(i, i);
    assert.equal(ht.size, 20);
    assert.ok(ht.getStats().buckets > 2);
    for (let i = 0; i < 20; i++) assert.equal(ht.get(i), i);
  });

  it('1000 inserts', () => {
    const ht = new LinearHashTable();
    for (let i = 0; i < 1000; i++) ht.set(i, i * 7);
    assert.equal(ht.size, 1000);
    for (let i = 0; i < 1000; i++) assert.equal(ht.get(i), i * 7);
  });

  it('stats', () => {
    const ht = new LinearHashTable();
    for (let i = 0; i < 100; i++) ht.set(i, i);
    const s = ht.getStats();
    assert.equal(s.size, 100);
    assert.ok(s.buckets > 4);
    assert.ok(parseFloat(s.avgChain) < 10);
  });
});
