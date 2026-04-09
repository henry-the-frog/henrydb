// tuple.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Tuple } from './tuple.js';

describe('Tuple', () => {
  it('get by name and index', () => {
    const t = new Tuple(['id', 'name', 'age'], [1, 'Alice', 30]);
    assert.equal(t.get('name'), 'Alice');
    assert.equal(t.getByIndex(0), 1);
  });

  it('toObject', () => {
    const t = new Tuple(['a', 'b'], [1, 2]);
    assert.deepEqual(t.toObject(), { a: 1, b: 2 });
  });

  it('project', () => {
    const t = new Tuple(['id', 'name', 'age'], [1, 'Alice', 30]);
    const p = t.project(['name', 'age']);
    assert.deepEqual(p.toObject(), { name: 'Alice', age: 30 });
  });

  it('immutable', () => {
    const t = new Tuple(['x'], [1]);
    assert.throws(() => { t._values[0] = 2; });
  });
});
