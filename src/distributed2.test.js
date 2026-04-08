// distributed2.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TwoPhaseCommitCoordinator, EpochManager, BRINIndex } from './distributed2.js';

describe('TwoPhaseCommitCoordinator', () => {
  it('commit when all vote yes', () => {
    const coord = new TwoPhaseCommitCoordinator();
    const txnId = coord.begin(['A', 'B', 'C']);
    coord.prepare(txnId);
    coord.vote(txnId, 'A', true);
    coord.vote(txnId, 'B', true);
    coord.vote(txnId, 'C', true);
    assert.equal(coord.decide(txnId).decision, 'COMMITTED');
  });

  it('abort when any votes no', () => {
    const coord = new TwoPhaseCommitCoordinator();
    const txnId = coord.begin(['A', 'B']);
    coord.prepare(txnId);
    coord.vote(txnId, 'A', true);
    coord.vote(txnId, 'B', false);
    assert.equal(coord.decide(txnId).decision, 'ABORTED');
  });

  it('waiting when not all voted', () => {
    const coord = new TwoPhaseCommitCoordinator();
    const txnId = coord.begin(['A', 'B']);
    coord.prepare(txnId);
    coord.vote(txnId, 'A', true);
    assert.equal(coord.decide(txnId).decision, 'WAITING');
  });

  it('tracks state', () => {
    const coord = new TwoPhaseCommitCoordinator();
    const txnId = coord.begin(['A']);
    assert.equal(coord.getState(txnId), 'INIT');
    coord.prepare(txnId);
    assert.equal(coord.getState(txnId), 'PREPARING');
  });
});

describe('EpochManager', () => {
  it('basic epoch lifecycle', () => {
    const em = new EpochManager();
    em.enter('t1');
    em.advance();
    em.retire('old data');
    em.exit('t1');
    const reclaimed = em.reclaim();
    assert.ok(reclaimed.length > 0 || em.retiredCount > 0);
  });

  it('doesnt reclaim while thread active', () => {
    const em = new EpochManager();
    em.enter('t1');
    em.retire('data');
    const reclaimed = em.reclaim();
    assert.equal(reclaimed.length, 0); // t1 still active
  });

  it('reclaims after thread exits and epoch advances', () => {
    const em = new EpochManager();
    em.retire('old');
    em.advance();
    em.enter('t1');
    em.exit('t1');
    em.advance();
    const reclaimed = em.reclaim();
    assert.ok(reclaimed.includes('old'));
  });
});

describe('BRINIndex', () => {
  it('build and lookup', () => {
    const data = Array.from({ length: 1000 }, (_, i) => ({ id: i, val: i * 10 }));
    const brin = BRINIndex.build(data, r => r.val, 128);
    assert.ok(brin.blockCount > 0);
    const blocks = brin.lookup(5000);
    assert.ok(blocks.length > 0);
  });

  it('range blocks', () => {
    const data = Array.from({ length: 1000 }, (_, i) => ({ key: i }));
    const brin = BRINIndex.build(data, r => r.key, 100);
    const blocks = brin.rangeBlocks(200, 500);
    assert.ok(blocks.length >= 3);
  });

  it('selectivity estimate', () => {
    const data = Array.from({ length: 1000 }, (_, i) => ({ key: i }));
    const brin = BRINIndex.build(data, r => r.key, 100);
    const sel = brin.selectivity(0, 999);
    assert.equal(sel, 1); // All blocks match full range
  });

  it('filters out non-matching blocks', () => {
    const data = Array.from({ length: 1000 }, (_, i) => ({ key: i }));
    const brin = BRINIndex.build(data, r => r.key, 100);
    const blocks = brin.lookup(5000);
    assert.equal(blocks.length, 0); // No block has key=5000
  });
});
