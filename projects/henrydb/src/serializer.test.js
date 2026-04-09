// serializer.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { serialize, deserialize } from './serializer.js';

describe('Serializer', () => {
  it('roundtrip all types', () => {
    for (const val of [null, 42, 3.14, true, false, 'hello world']) {
      const buf = serialize(val);
      const { value } = deserialize(buf);
      assert.deepEqual(value, val);
    }
  });

  it('compact encoding', () => {
    assert.equal(serialize(null).length, 1);
    assert.equal(serialize(42).length, 9);
    assert.equal(serialize(true).length, 2);
  });
});
