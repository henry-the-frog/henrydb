// heaps.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BinomialHeap, PairingHeap, LeftistHeap } from './heaps.js';

describe('BinomialHeap', () => {
  it('insert and findMin', () => {
    const h = new BinomialHeap();
    h.insert(5, 'five').insert(3, 'three').insert(7, 'seven');
    assert.equal(h.findMin().key, 3);
  });

  it('extractMin in order', () => {
    const h = new BinomialHeap();
    [5, 3, 7, 1, 9, 2].forEach(k => h.insert(k, k));
    const order = [];
    while (h.size > 0) order.push(h.extractMin().key);
    assert.deepEqual(order, [1, 2, 3, 5, 7, 9]);
  });

  it('empty extractMin', () => {
    assert.equal(new BinomialHeap().extractMin(), null);
  });

  it('benchmark: 10K operations', () => {
    const h = new BinomialHeap();
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) h.insert(Math.random() * 10000, i);
    for (let i = 0; i < 10000; i++) h.extractMin();
    console.log(`    Binomial heap 10K: ${Date.now() - t0}ms`);
    assert.equal(h.size, 0);
  });
});

describe('PairingHeap', () => {
  it('insert and findMin', () => {
    const h = new PairingHeap();
    h.insert(5, 'five').insert(3, 'three').insert(7, 'seven');
    assert.equal(h.findMin().key, 3);
  });

  it('extractMin in order', () => {
    const h = new PairingHeap();
    [5, 3, 7, 1, 9, 2].forEach(k => h.insert(k, k));
    const order = [];
    while (h.size > 0) order.push(h.extractMin().key);
    assert.deepEqual(order, [1, 2, 3, 5, 7, 9]);
  });

  it('empty', () => {
    assert.equal(new PairingHeap().findMin(), null);
  });

  it('benchmark: 10K operations', () => {
    const h = new PairingHeap();
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) h.insert(Math.random() * 10000, i);
    for (let i = 0; i < 10000; i++) h.extractMin();
    console.log(`    Pairing heap 10K: ${Date.now() - t0}ms`);
    assert.equal(h.size, 0);
  });
});

describe('LeftistHeap', () => {
  it('insert and findMin', () => {
    const h = new LeftistHeap();
    h.insert(5, 'five').insert(3, 'three').insert(7, 'seven');
    assert.equal(h.findMin().key, 3);
  });

  it('extractMin in order', () => {
    const h = new LeftistHeap();
    [5, 3, 7, 1, 9, 2].forEach(k => h.insert(k, k));
    const order = [];
    while (h.size > 0) order.push(h.extractMin().key);
    assert.deepEqual(order, [1, 2, 3, 5, 7, 9]);
  });

  it('empty', () => {
    assert.equal(new LeftistHeap().extractMin(), null);
  });

  it('benchmark: 10K operations', () => {
    const h = new LeftistHeap();
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) h.insert(Math.random() * 10000, i);
    for (let i = 0; i < 10000; i++) h.extractMin();
    console.log(`    Leftist heap 10K: ${Date.now() - t0}ms`);
    assert.equal(h.size, 0);
  });
});
