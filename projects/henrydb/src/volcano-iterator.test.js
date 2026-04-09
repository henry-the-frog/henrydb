// volcano-iterator.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SeqScan, Filter, Project, Limit, collect } from './volcano-iterator.js';

describe('Volcano Iterator Model', () => {
  const data = [
    { id: 1, name: 'Alice', age: 30 },
    { id: 2, name: 'Bob', age: 25 },
    { id: 3, name: 'Charlie', age: 35 },
  ];

  it('SeqScan → Filter → Project → Limit', () => {
    const plan =
      new Limit(
        new Project(
          new Filter(
            new SeqScan(data),
            row => row.age > 26
          ),
          ['name']
        ),
        1
      );

    const result = collect(plan);
    assert.equal(result.length, 1);
    assert.equal(result[0].name, 'Alice');
    assert.equal(result[0].age, undefined); // Projected away
  });

  it('full scan', () => {
    assert.equal(collect(new SeqScan(data)).length, 3);
  });

  it('filter only', () => {
    const plan = new Filter(new SeqScan(data), r => r.name === 'Bob');
    assert.deepEqual(collect(plan).map(r => r.name), ['Bob']);
  });
});
