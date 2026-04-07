// bitmap-index.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BitmapIndex } from './bitmap-index.js';

describe('BitmapIndex', () => {
  it('finds rows by value', () => {
    const idx = new BitmapIndex('idx', 'status');
    idx.addRow(0, 'active');
    idx.addRow(1, 'inactive');
    idx.addRow(2, 'active');
    idx.addRow(3, 'active');
    
    assert.deepEqual(idx.findEqual('active'), [0, 2, 3]);
    assert.deepEqual(idx.findEqual('inactive'), [1]);
    assert.deepEqual(idx.findEqual('unknown'), []);
  });

  it('counts values efficiently', () => {
    const idx = new BitmapIndex('idx', 'gender');
    for (let i = 0; i < 100; i++) {
      idx.addRow(i, i % 3 === 0 ? 'M' : 'F');
    }
    assert.equal(idx.count('M'), 34);
    assert.equal(idx.count('F'), 66);
  });

  it('NOT returns complement', () => {
    const idx = new BitmapIndex('idx', 'type');
    idx.addRow(0, 'A');
    idx.addRow(1, 'B');
    idx.addRow(2, 'A');
    idx.addRow(3, 'C');
    
    const notA = idx.not('A');
    assert.deepEqual(notA, [1, 3]);
  });

  it('AND combines two result sets', () => {
    const bm1 = [1, 0, 1, 1, 0];
    const bm2 = [1, 1, 0, 1, 0];
    const result = BitmapIndex.and(bm1, bm2);
    assert.deepEqual(result, [0, 3]);
  });

  it('OR combines two result sets', () => {
    const bm1 = [1, 0, 0, 1, 0];
    const bm2 = [0, 1, 0, 0, 1];
    const result = BitmapIndex.or(bm1, bm2);
    assert.deepEqual(result, [0, 1, 3, 4]);
  });

  it('distinct values', () => {
    const idx = new BitmapIndex('idx', 'color');
    idx.addRow(0, 'red');
    idx.addRow(1, 'blue');
    idx.addRow(2, 'red');
    idx.addRow(3, 'green');
    
    const values = idx.distinctValues().sort();
    assert.deepEqual(values, ['blue', 'green', 'red']);
  });

  it('stats reports value distribution', () => {
    const idx = new BitmapIndex('idx', 'status');
    idx.addRow(0, 'A');
    idx.addRow(1, 'B');
    idx.addRow(2, 'A');
    
    const stats = idx.stats();
    assert.equal(stats.distinctValues, 2);
    assert.equal(stats.totalRows, 3);
    assert.equal(stats.valueCounts['A'], 2);
  });
});
