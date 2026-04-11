// sha256.test.js — SHA-256 and HMAC-SHA256 tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { sha256, hmacSha256 } from './sha256.js';
import crypto from 'node:crypto';

describe('SHA-256', () => {
  // NIST test vectors
  it('empty string', () => {
    assert.equal(sha256(''), 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855');
  });

  it('"abc"', () => {
    assert.equal(sha256('abc'), 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
  });

  it('"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"', () => {
    assert.equal(sha256('abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq'),
      '248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1');
  });

  // Cross-check against Node crypto for various inputs
  const testStrings = [
    'hello', 'Hello, World!', '0123456789',
    'The quick brown fox jumps over the lazy dog',
    'a', 'aa', 'aaa', 'a'.repeat(55), 'a'.repeat(56),  // padding boundary
    'a'.repeat(64), 'a'.repeat(100), 'a'.repeat(1000),
    '\x00\x01\x02\x03', // binary data
    '日本語テスト', // Unicode
  ];

  for (const str of testStrings) {
    it(`matches crypto for "${str.slice(0, 30)}${str.length > 30 ? '...' : ''}" (${str.length} chars)`, () => {
      const expected = crypto.createHash('sha256').update(str).digest('hex');
      assert.equal(sha256(str), expected);
    });
  }

  it('handles Uint8Array input', () => {
    const data = new Uint8Array([0, 1, 2, 3, 255, 254, 253]);
    const expected = crypto.createHash('sha256').update(data).digest('hex');
    assert.equal(sha256(data), expected);
  });

  it('handles Buffer input', () => {
    const data = Buffer.from('test buffer input');
    const expected = crypto.createHash('sha256').update(data).digest('hex');
    assert.equal(sha256(data), expected);
  });

  // Deterministic: same input always gives same output
  it('deterministic', () => {
    const h1 = sha256('determinism');
    const h2 = sha256('determinism');
    assert.equal(h1, h2);
  });

  // Avalanche: small change → very different hash
  it('avalanche effect', () => {
    const h1 = sha256('test1');
    const h2 = sha256('test2');
    assert.notEqual(h1, h2);
    // Count different hex chars — should be many
    let diffs = 0;
    for (let i = 0; i < 64; i++) if (h1[i] !== h2[i]) diffs++;
    assert.ok(diffs > 20, `Only ${diffs}/64 hex chars differ — poor avalanche`);
  });

  // Output format
  it('always returns 64 hex chars', () => {
    for (let i = 0; i < 100; i++) {
      const hash = sha256('test' + i);
      assert.equal(hash.length, 64);
      assert.ok(/^[0-9a-f]{64}$/.test(hash));
    }
  });
});

describe('HMAC-SHA256', () => {
  it('RFC 4231 test vector 1', () => {
    // Key: 0x0b repeated 20 times, data: "Hi There"
    const key = Buffer.alloc(20, 0x0b);
    const expected = 'b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7';
    assert.equal(hmacSha256(key, 'Hi There'), expected);
  });

  it('RFC 4231 test vector 2', () => {
    // Key: "Jefe", data: "what do ya want for nothing?"
    const expected = '5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843';
    assert.equal(hmacSha256('Jefe', 'what do ya want for nothing?'), expected);
  });

  it('cross-check with Node crypto', () => {
    const testCases = [
      { key: 'secret', msg: 'hello' },
      { key: 'key', msg: 'The quick brown fox' },
      { key: 'a'.repeat(100), msg: 'long key test' },
    ];
    
    for (const { key, msg } of testCases) {
      const expected = crypto.createHmac('sha256', key).update(msg).digest('hex');
      assert.equal(hmacSha256(key, msg), expected);
    }
  });
});

describe('SHA-256 Differential Fuzzer', () => {
  it('1000 random strings match Node crypto', () => {
    let seed = 42;
    const rng = () => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed / 0x7fffffff; };
    
    for (let i = 0; i < 1000; i++) {
      const len = Math.floor(rng() * 200);
      let str = '';
      for (let j = 0; j < len; j++) str += String.fromCharCode(Math.floor(rng() * 128));
      
      const expected = crypto.createHash('sha256').update(str).digest('hex');
      assert.equal(sha256(str), expected, `Mismatch at iteration ${i}, len=${len}`);
    }
  });
});
