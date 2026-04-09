// siphash.js — SipHash: cryptographic hash for hash tables (DoS-resistant)
// Used in Python dict, Rust HashMap, Redis.
// Short-input optimized: fast for small keys while resistant to hash-flooding attacks.

export function siphash(key, k0 = 0x0706050403020100n, k1 = 0x0f0e0d0c0b0a0908n) {
  const buf = typeof key === 'string' ? Buffer.from(key) : key;
  let v0 = k0 ^ 0x736f6d6570736575n;
  let v1 = k1 ^ 0x646f72616e646f6dn;
  let v2 = k0 ^ 0x6c7967656e657261n;
  let v3 = k1 ^ 0x7465646279746573n;

  const len = buf.length;
  const blocks = Math.floor(len / 8);
  
  for (let i = 0; i < blocks; i++) {
    let m = 0n;
    for (let j = 0; j < 8; j++) m |= BigInt(buf[i * 8 + j]) << BigInt(j * 8);
    v3 ^= m;
    for (let r = 0; r < 2; r++) { v0 += v1; v2 += v3; v1 = rotl(v1, 13n); v3 = rotl(v3, 16n); v1 ^= v0; v3 ^= v2; v0 = rotl(v0, 32n); v2 += v1; v0 += v3; v1 = rotl(v1, 17n); v3 = rotl(v3, 21n); v1 ^= v2; v3 ^= v0; v2 = rotl(v2, 32n); }
    v0 ^= m;
  }

  let last = BigInt(len) << 56n;
  for (let i = blocks * 8; i < len; i++) last |= BigInt(buf[i]) << BigInt((i - blocks * 8) * 8);
  
  v3 ^= last;
  for (let r = 0; r < 2; r++) { v0 += v1; v2 += v3; v1 = rotl(v1, 13n); v3 = rotl(v3, 16n); v1 ^= v0; v3 ^= v2; v0 = rotl(v0, 32n); v2 += v1; v0 += v3; v1 = rotl(v1, 17n); v3 = rotl(v3, 21n); v1 ^= v2; v3 ^= v0; v2 = rotl(v2, 32n); }
  v0 ^= last;
  v2 ^= 0xffn;
  for (let r = 0; r < 4; r++) { v0 += v1; v2 += v3; v1 = rotl(v1, 13n); v3 = rotl(v3, 16n); v1 ^= v0; v3 ^= v2; v0 = rotl(v0, 32n); v2 += v1; v0 += v3; v1 = rotl(v1, 17n); v3 = rotl(v3, 21n); v1 ^= v2; v3 ^= v0; v2 = rotl(v2, 32n); }

  return (v0 ^ v1 ^ v2 ^ v3) & ((1n << 64n) - 1n);
}

function rotl(v, n) { return ((v << n) | (v >> (64n - n))) & ((1n << 64n) - 1n); }
