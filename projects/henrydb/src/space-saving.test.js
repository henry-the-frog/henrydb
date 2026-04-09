// space-saving.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SpaceSaving } from './space-saving.js';

describe('SpaceSaving', () => {
  it('finds obvious heavy hitters', () => {
    const ss = new SpaceSaving(3);
    for (let i = 0; i < 100; i++) ss.add('hot');
    for (let i = 0; i < 10; i++) ss.add('warm');
    for (let i = 0; i < 1; i++) ss.add('cold');
    
    const top = ss.getTop(1);
    assert.equal(top[0].item, 'hot');
  });

  it('maintains k counters', () => {
    const ss = new SpaceSaving(5);
    for (let i = 0; i < 100; i++) ss.add(`item-${i % 10}`);
    assert.equal(ss.size, 5);
  });

  it('Zipf distribution: finds true heavy hitters', () => {
    const ss = new SpaceSaving(10);
    // Zipf: item i appears 10000/i times
    for (let rank = 1; rank <= 100; rank++) {
      const freq = Math.floor(1000 / rank);
      for (let j = 0; j < freq; j++) ss.add(`item-${rank}`);
    }
    
    const top = ss.getTop(5);
    // Top items should include item-1 (highest frequency)
    assert.ok(top.some(t => t.item === 'item-1'));
  });
});
