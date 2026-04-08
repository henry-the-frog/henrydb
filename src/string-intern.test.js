// string-intern.test.js — Tests for string interning and dictionary encoding
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { StringInternPool, DictionaryEncodedColumn } from './string-intern.js';

describe('StringInternPool', () => {
  
  it('interns unique strings with sequential IDs', () => {
    const pool = new StringInternPool();
    assert.equal(pool.intern('hello'), 0);
    assert.equal(pool.intern('world'), 1);
    assert.equal(pool.intern('hello'), 0); // Same ID
    assert.equal(pool.size, 2);
  });

  it('lookup returns original string', () => {
    const pool = new StringInternPool();
    pool.intern('foo');
    pool.intern('bar');
    assert.equal(pool.lookup(0), 'foo');
    assert.equal(pool.lookup(1), 'bar');
    assert.equal(pool.lookup(999), null);
  });

  it('handles null/undefined', () => {
    const pool = new StringInternPool();
    assert.equal(pool.intern(null), -1);
    assert.equal(pool.intern(undefined), -1);
  });

  it('tracks hit/miss stats', () => {
    const pool = new StringInternPool();
    pool.intern('a'); // miss
    pool.intern('a'); // hit
    pool.intern('b'); // miss
    pool.intern('a'); // hit
    assert.equal(pool.stats.interns, 2);
    assert.equal(pool.stats.hits, 2);
    assert.equal(pool.stats.misses, 2);
  });

  it('estimates memory savings', () => {
    const pool = new StringInternPool();
    pool.intern('ABCDEFGHIJ'); // 10 chars
    const savings = pool.memorySavings(1000); // 1000 references
    assert.ok(savings.savedBytes > 0);
    assert.ok(savings.savedPercent > 0);
  });
});

describe('DictionaryEncodedColumn', () => {
  
  it('push and get', () => {
    const col = new DictionaryEncodedColumn();
    col.push('US');
    col.push('EU');
    col.push('US');
    assert.equal(col.get(0), 'US');
    assert.equal(col.get(1), 'EU');
    assert.equal(col.get(2), 'US');
    assert.equal(col.length, 3);
    assert.equal(col.cardinality, 2);
  });

  it('filterEquals returns matching indices', () => {
    const col = new DictionaryEncodedColumn();
    const regions = ['US', 'EU', 'APAC', 'US', 'EU', 'US'];
    for (const r of regions) col.push(r);

    const usIndices = col.filterEquals('US');
    assert.deepEqual(usIndices, [0, 3, 5]);

    const euIndices = col.filterEquals('EU');
    assert.deepEqual(euIndices, [1, 4]);

    const missing = col.filterEquals('LATAM');
    assert.deepEqual(missing, []);
  });

  it('filterEqualsBatch returns Uint32Array', () => {
    const col = new DictionaryEncodedColumn();
    for (let i = 0; i < 1000; i++) col.push(['US', 'EU', 'APAC'][i % 3]);

    const batch = col.filterEqualsBatch('US');
    assert.ok(batch instanceof Uint32Array);
    assert.equal(batch.length, 334); // ceil(1000/3)
  });

  it('filterIn returns union of matches', () => {
    const col = new DictionaryEncodedColumn();
    const values = ['US', 'EU', 'APAC', 'LATAM'];
    for (let i = 0; i < 100; i++) col.push(values[i % 4]);

    const result = col.filterIn(['US', 'LATAM']);
    assert.equal(result.length, 50); // 25 + 25
  });

  it('groupBy returns correct groups', () => {
    const col = new DictionaryEncodedColumn();
    col.push('A'); col.push('B'); col.push('A'); col.push('C'); col.push('B');

    const groups = col.groupBy();
    assert.equal(groups.size, 3);
    assert.deepEqual(groups.get('A'), [0, 2]);
    assert.deepEqual(groups.get('B'), [1, 4]);
    assert.deepEqual(groups.get('C'), [3]);
  });

  it('handles null values', () => {
    const col = new DictionaryEncodedColumn();
    col.push('US');
    col.push(null);
    col.push('EU');
    col.push(null);

    assert.equal(col.get(0), 'US');
    assert.equal(col.get(1), null);
    assert.equal(col.get(3), null);
  });

  it('benchmark: dictionary filter vs string comparison on 1M rows', () => {
    const col = new DictionaryEncodedColumn();
    const rawValues = [];
    const regions = ['US', 'EU', 'APAC', 'LATAM', 'AFRICA'];
    for (let i = 0; i < 1000000; i++) {
      const val = regions[i % 5];
      col.push(val);
      rawValues.push(val);
    }

    // Dictionary filter (integer comparison)
    const t0 = Date.now();
    const dictResult = col.filterEquals('US');
    const dictMs = Date.now() - t0;

    // String comparison
    const t1 = Date.now();
    const strResult = [];
    for (let i = 0; i < rawValues.length; i++) {
      if (rawValues[i] === 'US') strResult.push(i);
    }
    const strMs = Date.now() - t1;

    console.log(`    Dictionary: ${dictMs}ms vs String: ${strMs}ms (${(strMs / Math.max(dictMs, 0.1)).toFixed(1)}x)`);
    assert.equal(dictResult.length, 200000);
    assert.equal(strResult.length, 200000);
    assert.equal(col.cardinality, 5);
  });

  it('stats report compression', () => {
    const col = new DictionaryEncodedColumn();
    for (let i = 0; i < 10000; i++) col.push(`Region_${i % 4}`);

    const stats = col.getStats();
    assert.equal(stats.length, 10000);
    assert.equal(stats.cardinality, 4);
    assert.ok(parseFloat(stats.compressionRatio) > 100); // 10000/4 = 2500x
    assert.ok(stats.savedBytes > 0);
  });
});
