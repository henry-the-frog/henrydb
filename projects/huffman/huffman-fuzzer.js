// Huffman coding roundtrip fuzzer
import { compress, decompress, compressionRatio, encode, decode, buildTree, buildFrequencyTable, generateCodes } from './huffman.js';

function seeded(seed) {
  let s = seed;
  return () => { s = (s * 1103515245 + 12345) & 0x7fffffff; return s / 0x7fffffff; };
}

function randomInt(rng, min, max) { return Math.floor(rng() * (max - min + 1)) + min; }

function randomString(rng, maxLen) {
  const len = randomInt(rng, 0, maxLen);
  let s = '';
  const charSets = [
    'abcdefghijklmnopqrstuvwxyz',                          // lowercase
    'ABCDEFGHIJKLMNOPQRSTUVWXYZ',                          // uppercase
    '0123456789',                                           // digits
    ' !@#$%^&*()_+-=[]{}|;:,.<>?/~`',                     // symbols
    '\n\t\r',                                               // whitespace
  ];
  const charset = charSets[randomInt(rng, 0, charSets.length - 1)];
  for (let i = 0; i < len; i++) {
    s += charset[Math.floor(rng() * charset.length)];
  }
  return s;
}

// Test different distributions
function skewedString(rng, len) {
  // Heavily skewed toward one char (good compression)
  const dominant = String.fromCharCode(randomInt(rng, 97, 122));
  let s = '';
  for (let i = 0; i < len; i++) {
    s += rng() < 0.8 ? dominant : String.fromCharCode(randomInt(rng, 32, 126));
  }
  return s;
}

function uniformString(rng, len) {
  // Uniform distribution (poor compression)
  let s = '';
  for (let i = 0; i < len; i++) {
    s += String.fromCharCode(randomInt(rng, 32, 126));
  }
  return s;
}

let passed = 0, failed = 0, crashes = 0;
const failures = [];
const startTime = Date.now();

for (let seed = 1; seed <= 3000; seed++) {
  const rng = seeded(seed);
  const r = rng();
  
  let input;
  if (r < 0.3) {
    input = randomString(rng, 200);
  } else if (r < 0.5) {
    input = skewedString(rng, randomInt(rng, 50, 500));
  } else if (r < 0.7) {
    input = uniformString(rng, randomInt(rng, 10, 300));
  } else if (r < 0.8) {
    // Single char repeated
    input = 'a'.repeat(randomInt(rng, 1, 200));
  } else if (r < 0.9) {
    // Binary-ish (just 2 chars)
    const len = randomInt(rng, 5, 100);
    input = '';
    for (let i = 0; i < len; i++) input += rng() < 0.6 ? '0' : '1';
  } else {
    // Empty or single char
    input = rng() < 0.5 ? '' : String.fromCharCode(randomInt(rng, 32, 126));
  }
  
  try {
    if (input.length === 0) {
      passed++; // skip empty
      continue;
    }
    
    const compressed = compress(input);
    const decompressed = decompress(compressed);
    
    if (decompressed !== input) {
      failed++;
      if (failures.length < 10) {
        failures.push({ seed, len: input.length, expected: input.slice(0, 50), got: decompressed.slice(0, 50) });
      }
    } else {
      // Verify compression properties
      const ratio = compressionRatio(input, compressed);
      if (typeof ratio !== 'number' || isNaN(ratio)) {
        failed++;
        if (failures.length < 10) failures.push({ seed, issue: 'bad ratio: ' + ratio });
      } else {
        passed++;
      }
    }
  } catch (e) {
    crashes++;
    if (failures.length < 10) failures.push({ seed, len: input?.length, error: e.message });
  }
  
  if (seed % 500 === 0) process.stderr.write(`${seed}/3000...\n`);
}

const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
console.log(`\n=== Huffman Roundtrip Fuzzer ===`);
console.log(`Passed: ${passed}, Failed: ${failed}, Crashes: ${crashes}`);
console.log(`Time: ${elapsed}s`);

if (failures.length > 0) {
  console.log(`\n=== Failures ===`);
  for (const f of failures) {
    if (f.error) console.log(`  seed=${f.seed} len=${f.len}: ${f.error}`);
    else if (f.issue) console.log(`  seed=${f.seed}: ${f.issue}`);
    else console.log(`  seed=${f.seed} len=${f.len}: roundtrip mismatch`);
  }
}

console.log(failed + crashes === 0 ? '\nALL PASS ✓' : '\nFAILURES DETECTED');
process.exit(failed + crashes > 0 ? 1 : 0);
