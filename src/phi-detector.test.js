// phi-detector.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PhiAccrualFailureDetector, BLinkTree } from './phi-detector.js';

describe('PhiAccrualFailureDetector', () => {
  it('healthy node has low phi', () => {
    const fd = new PhiAccrualFailureDetector();
    let t = 1000;
    for (let i = 0; i < 20; i++) { fd.heartbeat(t); t += 100; }
    assert.ok(fd.phi(t + 50) < 3); // Shortly after expected heartbeat
    assert.ok(fd.isAlive(t + 50));
  });

  it('missed heartbeats increase phi', () => {
    const fd = new PhiAccrualFailureDetector();
    let t = 1000;
    for (let i = 0; i < 20; i++) { fd.heartbeat(t); t += 100; }
    // Miss several heartbeats
    const phi1 = fd.phi(t + 500);
    const phi2 = fd.phi(t + 2000);
    assert.ok(phi2 > phi1);
  });

  it('eventually marks as failed', () => {
    const fd = new PhiAccrualFailureDetector(8);
    let t = 1000;
    for (let i = 0; i < 20; i++) { fd.heartbeat(t); t += 100; }
    // Long delay → should be considered failed
    assert.ok(!fd.isAlive(t + 10000));
  });

  it('adapts to different intervals', () => {
    const fd = new PhiAccrualFailureDetector();
    let t = 1000;
    for (let i = 0; i < 20; i++) { fd.heartbeat(t); t += 500; } // 500ms intervals
    assert.ok(fd.isAlive(t + 200)); // Within expected range
  });

  it('sample size tracking', () => {
    const fd = new PhiAccrualFailureDetector(8, 10);
    for (let i = 0; i < 20; i++) fd.heartbeat(1000 + i * 100);
    assert.equal(fd.sampleSize, 10); // Capped at maxSampleSize
  });
});

describe('BLinkTree', () => {
  it('insert and search', () => {
    const tree = new BLinkTree(4);
    tree.insert(5, 'five');
    tree.insert(3, 'three');
    tree.insert(7, 'seven');
    assert.equal(tree.search(5), 'five');
    assert.equal(tree.search(3), 'three');
    assert.equal(tree.search(99), undefined);
  });

  it('range scan via leaf links', () => {
    const tree = new BLinkTree(4);
    for (let i = 0; i < 20; i++) tree.insert(i, i * 10);
    const range = tree.range(5, 15);
    assert.equal(range.length, 11);
    assert.equal(range[0][0], 5);
    assert.equal(range[10][0], 15);
  });

  it('update existing key', () => {
    const tree = new BLinkTree(4);
    tree.insert(5, 'old');
    tree.insert(5, 'new');
    assert.equal(tree.search(5), 'new');
  });

  it('handles splits', () => {
    const tree = new BLinkTree(4);
    for (let i = 0; i < 100; i++) tree.insert(i, i);
    for (let i = 0; i < 100; i++) assert.equal(tree.search(i), i);
  });

  it('reverse insertion order', () => {
    const tree = new BLinkTree(4);
    for (let i = 99; i >= 0; i--) tree.insert(i, i);
    for (let i = 0; i < 100; i++) assert.equal(tree.search(i), i);
  });

  it('benchmark: 10K inserts + lookups', () => {
    const tree = new BLinkTree(16);
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) tree.insert(i, i);
    const t1 = Date.now();
    for (let i = 0; i < 10000; i++) tree.search(i);
    console.log(`    B-link tree 10K: insert=${t1 - t0}ms, search=${Date.now() - t1}ms`);
  });
});
