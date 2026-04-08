// query-profiler.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { QueryProfiler, IndexAdvisor } from './query-profiler.js';

describe('QueryProfiler', () => {
  it('profiles operator timing', () => {
    const profiler = new QueryProfiler();
    
    const data = profiler.profileOp('SeqScan(users)', () => {
      return Array.from({ length: 1000 }, (_, i) => ({ id: i, name: `user_${i}` }));
    });
    
    const filtered = profiler.profileOp('Filter(age > 25)', () => {
      return data.filter(r => r.id > 500);
    });
    
    const profile = profiler.getProfile();
    assert.equal(profile.operators.length, 2);
    assert.equal(profile.operators[0].rows, 1000);
    assert.ok(profile.totalMs >= 0);
  });

  it('toText format', () => {
    const profiler = new QueryProfiler();
    profiler.profileOp('Scan', () => [1, 2, 3]);
    profiler.profileOp('Filter', () => [1]);
    
    const text = profiler.toText();
    assert.ok(text.includes('Scan'));
    assert.ok(text.includes('Filter'));
    assert.ok(text.includes('TOTAL'));
  });

  it('reset clears state', () => {
    const profiler = new QueryProfiler();
    profiler.profileOp('Op', () => []);
    profiler.reset();
    assert.equal(profiler.getProfile().operators.length, 0);
  });

  it('nested profiling', () => {
    const profiler = new QueryProfiler();
    profiler.profileOp('Outer', () => {
      const inner = profiler.profileOp('Inner', () => [1, 2, 3]);
      return inner.map(x => x * 2);
    });
    assert.equal(profiler.getProfile().operators.length, 2);
  });
});

describe('IndexAdvisor', () => {
  it('suggests index for frequently queried column', () => {
    const advisor = new IndexAdvisor();
    advisor.recordQuery({ table: 'users', predicates: [{ column: 'email', type: 'EQ' }] });
    advisor.recordQuery({ table: 'users', predicates: [{ column: 'email', type: 'EQ' }] });
    advisor.recordQuery({ table: 'users', predicates: [{ column: 'email', type: 'EQ' }] });
    
    const suggestions = advisor.suggest();
    assert.ok(suggestions.length > 0);
    assert.equal(suggestions[0].column, 'email');
    assert.equal(suggestions[0].indexType, 'HASH');
  });

  it('suggests B-TREE for range queries', () => {
    const advisor = new IndexAdvisor();
    advisor.recordQuery({ table: 'orders', predicates: [{ column: 'date', type: 'RANGE' }] });
    advisor.recordQuery({ table: 'orders', predicates: [{ column: 'date', type: 'RANGE' }] });
    
    const suggestions = advisor.suggest();
    assert.ok(suggestions.some(s => s.column === 'date' && s.indexType === 'B-TREE'));
  });

  it('ranks by frequency', () => {
    const advisor = new IndexAdvisor();
    for (let i = 0; i < 5; i++) advisor.recordQuery({ table: 'a', predicates: [{ column: 'hot', type: 'EQ' }] });
    for (let i = 0; i < 2; i++) advisor.recordQuery({ table: 'a', predicates: [{ column: 'cold', type: 'EQ' }] });
    
    const suggestions = advisor.suggest();
    assert.equal(suggestions[0].column, 'hot');
  });

  it('no suggestions for rarely used columns', () => {
    const advisor = new IndexAdvisor();
    advisor.recordQuery({ table: 'a', predicates: [{ column: 'rare', type: 'EQ' }] });
    assert.equal(advisor.suggest().length, 0); // Only 1 access, need >= 2
  });
});
