// predicate.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Predicate } from './predicate.js';

describe('Predicate', () => {
  const rows = [
    { name: 'Alice', age: 30, dept: 'eng' },
    { name: 'Bob', age: 25, dept: 'sales' },
    { name: 'Charlie', age: 35, dept: 'eng' },
  ];

  it('eq and gt', () => {
    const p = Predicate.eq('dept', 'eng').and(Predicate.gt('age', 28));
    const filtered = rows.filter(r => p.test(r));
    assert.deepEqual(filtered.map(r => r.name), ['Alice', 'Charlie']);
  });

  it('or and not', () => {
    const p = Predicate.eq('name', 'Alice').or(Predicate.eq('name', 'Bob'));
    assert.equal(rows.filter(r => p.test(r)).length, 2);
    
    const notP = p.not();
    assert.equal(rows.filter(r => notP.test(r)).length, 1);
  });

  it('like', () => {
    const p = Predicate.like('name', 'Al%');
    assert.equal(p.test(rows[0]), true);
    assert.equal(p.test(rows[1]), false);
  });

  it('in', () => {
    const p = Predicate.in('dept', ['eng', 'hr']);
    assert.equal(rows.filter(r => p.test(r)).length, 2);
  });

  it('between', () => {
    const p = Predicate.between('age', 26, 34);
    assert.equal(rows.filter(r => p.test(r)).length, 1);
  });
});
