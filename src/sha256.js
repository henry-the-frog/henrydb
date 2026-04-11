// sha256.js — SHA-256 implementation from scratch
// Reference: FIPS 180-4 (Secure Hash Standard)

// Initial hash values (first 32 bits of fractional parts of square roots of first 8 primes)
const H0 = [
  0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
  0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

// Round constants (first 32 bits of fractional parts of cube roots of first 64 primes)
const K = [
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

// Bitwise operations (all operate on 32-bit unsigned integers)
function rotr(x, n) { return ((x >>> n) | (x << (32 - n))) >>> 0; }
function shr(x, n) { return (x >>> n) >>> 0; }
function ch(x, y, z) { return ((x & y) ^ (~x & z)) >>> 0; }
function maj(x, y, z) { return ((x & y) ^ (x & z) ^ (y & z)) >>> 0; }
function sigma0(x) { return (rotr(x, 2) ^ rotr(x, 13) ^ rotr(x, 22)) >>> 0; }
function sigma1(x) { return (rotr(x, 6) ^ rotr(x, 11) ^ rotr(x, 25)) >>> 0; }
function gamma0(x) { return (rotr(x, 7) ^ rotr(x, 18) ^ shr(x, 3)) >>> 0; }
function gamma1(x) { return (rotr(x, 17) ^ rotr(x, 19) ^ shr(x, 10)) >>> 0; }

/**
 * Pad the message per SHA-256 spec:
 * 1. Append bit '1' (0x80 byte)
 * 2. Append zeros until message length ≡ 448 mod 512 (in bits)
 * 3. Append original message length as 64-bit big-endian
 * Returns a Uint8Array whose length is a multiple of 64 bytes (512 bits).
 */
function pad(message) {
  const msgLen = message.length;
  const bitLen = msgLen * 8;
  
  // Calculate padded length: need room for 1 byte (0x80) + zeros + 8 bytes (length)
  let paddedLen = msgLen + 1 + 8;
  while (paddedLen % 64 !== 0) paddedLen++;
  
  const padded = new Uint8Array(paddedLen);
  padded.set(message);
  padded[msgLen] = 0x80;
  
  // Append length as 64-bit big-endian (we only support messages up to 2^53 bits)
  const view = new DataView(padded.buffer);
  // High 32 bits (for messages < 2^32 bytes, this is 0 or very small)
  view.setUint32(paddedLen - 8, Math.floor(bitLen / 0x100000000), false);
  // Low 32 bits
  view.setUint32(paddedLen - 4, bitLen >>> 0, false);
  
  return padded;
}

/**
 * SHA-256 hash function.
 * @param {string|Uint8Array|Buffer} input — the message to hash
 * @returns {string} — 64-character hex string
 */
export function sha256(input) {
  // Convert input to Uint8Array
  let message;
  if (typeof input === 'string') {
    message = new TextEncoder().encode(input);
  } else if (input instanceof Uint8Array) {
    message = input;
  } else if (Buffer.isBuffer(input)) {
    message = new Uint8Array(input);
  } else {
    throw new Error('Input must be string, Uint8Array, or Buffer');
  }
  
  // Pad message
  const padded = pad(message);
  
  // Initialize hash values
  let [h0, h1, h2, h3, h4, h5, h6, h7] = H0;
  
  // Process each 512-bit (64-byte) block
  const numBlocks = padded.length / 64;
  const view = new DataView(padded.buffer);
  
  for (let block = 0; block < numBlocks; block++) {
    const offset = block * 64;
    
    // Prepare message schedule (W)
    const W = new Uint32Array(64);
    
    // First 16 words: directly from the block (big-endian)
    for (let t = 0; t < 16; t++) {
      W[t] = view.getUint32(offset + t * 4, false);
    }
    
    // Remaining 48 words: computed from previous words
    for (let t = 16; t < 64; t++) {
      W[t] = (gamma1(W[t - 2]) + W[t - 7] + gamma0(W[t - 15]) + W[t - 16]) >>> 0;
    }
    
    // Initialize working variables
    let a = h0, b = h1, c = h2, d = h3;
    let e = h4, f = h5, g = h6, h = h7;
    
    // 64 rounds of compression
    for (let t = 0; t < 64; t++) {
      const T1 = (h + sigma1(e) + ch(e, f, g) + K[t] + W[t]) >>> 0;
      const T2 = (sigma0(a) + maj(a, b, c)) >>> 0;
      
      h = g;
      g = f;
      f = e;
      e = (d + T1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (T1 + T2) >>> 0;
    }
    
    // Add compressed chunk to hash value
    h0 = (h0 + a) >>> 0;
    h1 = (h1 + b) >>> 0;
    h2 = (h2 + c) >>> 0;
    h3 = (h3 + d) >>> 0;
    h4 = (h4 + e) >>> 0;
    h5 = (h5 + f) >>> 0;
    h6 = (h6 + g) >>> 0;
    h7 = (h7 + h) >>> 0;
  }
  
  // Produce the final hash (concatenate h0-h7 as big-endian hex)
  return [h0, h1, h2, h3, h4, h5, h6, h7]
    .map(h => h.toString(16).padStart(8, '0'))
    .join('');
}

/**
 * HMAC-SHA256: keyed hash for message authentication.
 * @param {string|Uint8Array} key
 * @param {string|Uint8Array} message
 * @returns {string} — hex digest
 */
export function hmacSha256(key, message) {
  const encoder = new TextEncoder();
  let keyBytes = typeof key === 'string' ? encoder.encode(key) : new Uint8Array(key);
  const msgBytes = typeof message === 'string' ? encoder.encode(message) : new Uint8Array(message);
  
  // If key > 64 bytes, hash it
  if (keyBytes.length > 64) {
    const hashed = sha256(keyBytes);
    keyBytes = new Uint8Array(hashed.match(/.{2}/g).map(b => parseInt(b, 16)));
  }
  
  // Pad key to 64 bytes
  const paddedKey = new Uint8Array(64);
  paddedKey.set(keyBytes);
  
  // Inner and outer padding
  const ipad = new Uint8Array(64);
  const opad = new Uint8Array(64);
  for (let i = 0; i < 64; i++) {
    ipad[i] = paddedKey[i] ^ 0x36;
    opad[i] = paddedKey[i] ^ 0x5c;
  }
  
  // HMAC = H(opad || H(ipad || message))
  const innerInput = new Uint8Array(64 + msgBytes.length);
  innerInput.set(ipad);
  innerInput.set(msgBytes, 64);
  const innerHash = sha256(innerInput);
  
  const innerHashBytes = new Uint8Array(innerHash.match(/.{2}/g).map(b => parseInt(b, 16)));
  const outerInput = new Uint8Array(64 + 32);
  outerInput.set(opad);
  outerInput.set(innerHashBytes, 64);
  
  return sha256(outerInput);
}
