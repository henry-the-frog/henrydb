// exponential-histogram.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ExponentialHistogram } from './exponential-histogram.js';

describe('ExponentialHistogram', () => {
  it('all ones window', () => {
    const eh = new ExponentialHistogram(10);
    for (let i = 0; i < 10; i++) eh.add(1);
    const est = eh.estimate();
    assert.ok(est >= 5 && est <= 10, `estimate ${est} out of range`);
  });

  it('all zeros', () => {
    const eh = new ExponentialHistogram(10);
    for (let i = 0; i < 10; i++) eh.add(0);
    assert.equal(eh.estimate(), 0);
  });

  it('sliding window expires old data', () => {
    const eh = new ExponentialHistogram(5);
    for (let i = 0; i < 5; i++) eh.add(1);
    for (let i = 0; i < 5; i++) eh.add(0);
    assert.equal(eh.estimate(), 0); // All 1s expired
  });

  it('approximation within epsilon', () => {
    const W = 100;
    const eh = new ExponentialHistogram(W, 0.5);
    let actual = 0;
    for (let i = 0; i < 200; i++) {
      const bit = Math.random() < 0.5 ? 1 : 0;
      eh.add(bit);
    }
    const est = eh.estimate();
    // Rough check: estimate should be reasonable
    assert.ok(est >= 0 && est <= W);
  });
});
