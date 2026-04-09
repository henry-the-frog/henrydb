// type-system.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { inferType, coerce, isCompatible, Types } from './type-system.js';

describe('TypeSystem', () => {
  it('inferType', () => {
    assert.equal(inferType(42), Types.INT);
    assert.equal(inferType(3.14), Types.FLOAT);
    assert.equal(inferType('hello'), Types.VARCHAR);
    assert.equal(inferType(null), Types.NULL);
  });

  it('coerce', () => {
    assert.equal(coerce('42', Types.INT), 42);
    assert.equal(coerce(42, Types.VARCHAR), '42');
  });

  it('isCompatible', () => {
    assert.equal(isCompatible(Types.INT, Types.FLOAT), true);
    assert.equal(isCompatible(Types.INT, Types.VARCHAR), false);
    assert.equal(isCompatible(Types.NULL, Types.INT), true);
  });
});
