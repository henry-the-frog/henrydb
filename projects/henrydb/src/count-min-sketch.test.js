// count-min-sketch.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CountMinSketch } from './count-min-sketch.js';

describe('CountMinSketch', () => {
  it('basic add and estimate', () => {
    const cms = new CountMinSketch(1024, 5);
    cms.add('hello', 10);
    cms.add('world', 5);
    assert.ok(cms.estimate('hello') >= 10);
    assert.ok(cms.estimate('world') >= 5);
    assert.equal(cms.estimate('missing'), 0);
  });

  it('accuracy on skewed distribution', () => {
    const cms = new CountMinSketch(2048, 7);
    const trueCounts = {};
    for (let i = 0; i < 100000; i++) {
      const key = `key_${Math.floor(Math.pow(Math.random(), 2) * 100)}`; // Zipf-like
      cms.add(key);
      trueCounts[key] = (trueCounts[key] || 0) + 1;
    }
    let totalError = 0, checks = 0;
    for (const [key, trueCount] of Object.entries(trueCounts)) {
      const est = cms.estimate(key);
      assert.ok(est >= trueCount, `Underestimate: ${est} < ${trueCount}`);
      totalError += (est - trueCount) / trueCount;
      checks++;
    }
    const avgError = totalError / checks;
    console.log(`    Avg relative error: ${(avgError * 100).toFixed(1)}%`);
  });

  it('merge two sketches', () => {
    const a = new CountMinSketch(512, 4);
    const b = new CountMinSketch(512, 4);
    a.add('x', 10);
    b.add('x', 20);
    a.merge(b);
    assert.ok(a.estimate('x') >= 30);
  });

  it('frequency estimation', () => {
    const cms = new CountMinSketch(1024, 5);
    for (let i = 0; i < 1000; i++) cms.add('common');
    for (let i = 0; i < 10; i++) cms.add('rare');
    assert.ok(cms.estimateFrequency('common') > 0.9);
    assert.ok(cms.estimateFrequency('rare') < 0.05);
  });

  it('memory stats', () => {
    const cms = new CountMinSketch(1024, 5);
    assert.equal(cms.getStats().memoryBytes, 1024 * 5 * 4);
  });
});
