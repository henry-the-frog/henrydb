// checksum.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { adler32, fletcher16, xorChecksum } from './checksum.js';

describe('Checksums', () => {
  it('adler32', () => {
    const a = adler32(Buffer.from('hello'));
    const b = adler32(Buffer.from('hello'));
    const c = adler32(Buffer.from('world'));
    assert.equal(a, b);
    assert.notEqual(a, c);
  });

  it('fletcher16', () => {
    assert.ok(fletcher16(Buffer.from('test')) > 0);
  });

  it('xor', () => {
    assert.equal(xorChecksum(Buffer.from([0xFF, 0xFF])), 0);
  });
});
