// varint.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { encodeVarint, decodeVarint, varintSize } from './varint.js';

describe('Varint', () => {
  it('encode/decode roundtrip', () => {
    for (const v of [0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 0xFFFFFFFF]) {
      const buf = encodeVarint(v);
      const { value } = decodeVarint(buf);
      assert.equal(value, v >>> 0, `Failed for ${v}`);
    }
  });

  it('small values use 1 byte', () => {
    assert.equal(encodeVarint(42).length, 1);
    assert.equal(encodeVarint(127).length, 1);
    assert.equal(encodeVarint(128).length, 2);
  });

  it('varintSize', () => {
    assert.equal(varintSize(0), 1);
    assert.equal(varintSize(127), 1);
    assert.equal(varintSize(128), 2);
  });
});
