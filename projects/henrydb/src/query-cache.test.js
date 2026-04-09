// query-cache.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { QueryCache } from './query-cache.js';

describe('QueryCache', () => {
  it('cache hit and miss', () => {
    const qc = new QueryCache();
    qc.set('SELECT * FROM users', [], { rows: [1] });
    const hit = qc.get('SELECT * FROM users', []);
    assert.ok(hit);
    const miss = qc.get('SELECT * FROM orders', []);
    assert.equal(miss, undefined);
  });

  it('invalidate by table', () => {
    const qc = new QueryCache();
    qc.set('SELECT * FROM users', [], { rows: [] });
    qc.invalidate('users');
    assert.equal(qc.size, 0);
  });

  it('hit rate', () => {
    const qc = new QueryCache();
    qc.set('q1', [], {}); 
    qc.get('q1', []); // hit
    qc.get('q2', []); // miss
    assert.equal(qc.hitRate, 0.5);
  });
});
