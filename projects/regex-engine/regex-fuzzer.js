// Regex Differential Fuzzer: our engine vs native JS RegExp
import { Regex } from './regex.js';

function seeded(seed) {
  let s = seed;
  return () => { s = (s * 1103515245 + 12345) & 0x7fffffff; return s / 0x7fffffff; };
}

function randomInt(rng, min, max) { return Math.floor(rng() * (max - min + 1)) + min; }

function randomChar(rng) {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789 .';
  return chars[Math.floor(rng() * chars.length)];
}

function randomString(rng, maxLen) {
  const len = randomInt(rng, 0, maxLen);
  let s = '';
  for (let i = 0; i < len; i++) s += randomChar(rng);
  return s;
}

// Generate random regex patterns (simple subset safe for both engines)
function randomPattern(rng) {
  const atoms = 'abcdefghijklmnopqrstuvwxyz0123456789.';
  const depth = randomInt(rng, 1, 4);
  
  function gen(d) {
    if (d <= 0) return atoms[Math.floor(rng() * atoms.length)];
    
    const r = rng();
    if (r < 0.3) {
      // literal
      return atoms[Math.floor(rng() * atoms.length)];
    } else if (r < 0.45) {
      // char class
      const chars = [];
      for (let i = 0; i < randomInt(rng, 2, 5); i++) {
        chars.push(atoms[Math.floor(rng() * 26)]); // just letters
      }
      return '[' + [...new Set(chars)].join('') + ']';
    } else if (r < 0.6) {
      // concatenation
      return gen(d-1) + gen(d-1);
    } else if (r < 0.72) {
      // alternation
      return '(?:' + gen(d-1) + '|' + gen(d-1) + ')';
    } else if (r < 0.82) {
      // star
      return gen(d-1) + '*';
    } else if (r < 0.9) {
      // plus
      return gen(d-1) + '+';
    } else {
      // optional
      return gen(d-1) + '?';
    }
  }
  
  return gen(depth);
}

let passed = 0, failed = 0, errors = 0, skipped = 0;
const mismatches = [];
const startTime = Date.now();

for (let seed = 1; seed <= 2000; seed++) {
  const rng = seeded(seed);
  const pattern = randomPattern(rng);
  
  // Generate test strings: some random, some derived from pattern
  const testStrings = [];
  for (let i = 0; i < 5; i++) testStrings.push(randomString(rng, 20));
  // Add pattern-derived strings (likely to partially match)
  const patLiterals = pattern.replace(/[\[\]\(\)\?\*\+\.\|\{\}\^$:]/g, '');
  if (patLiterals.length > 0) {
    testStrings.push(patLiterals);
    testStrings.push(patLiterals + randomString(rng, 5));
    testStrings.push(randomString(rng, 3) + patLiterals + randomString(rng, 3));
  }
  testStrings.push(''); // empty string
  
  let ourRegex, nativeRegex;
  try {
    ourRegex = new Regex(pattern);
    nativeRegex = new RegExp(pattern);
  } catch (e) {
    skipped++;
    continue;
  }
  
  for (const str of testStrings) {
    try {
      // Compare full-string match: our test() vs native with anchors
      const anchoredNative = new RegExp('^(?:' + pattern + ')$');
      const ourResult = ourRegex.test(str);
      const nativeResult = anchoredNative.test(str);
      
      if (ourResult !== nativeResult) {
        failed++;
        if (mismatches.length < 20) {
          mismatches.push({ seed, pattern, str, ours: ourResult, native: nativeResult });
        }
      } else {
        passed++;
      }
      
      // Also compare search behavior
      const ourSearch = ourRegex.search(str);
      const nativeSearch = nativeRegex.exec(str);
      
      const ourFound = ourSearch !== null;
      const nativeFound = nativeSearch !== null;
      
      if (ourFound !== nativeFound) {
        failed++;
        if (mismatches.length < 20) {
          mismatches.push({ seed, pattern, str, mode: 'search', ours: ourFound, native: nativeFound });
        }
      } else {
        passed++;
      }
    } catch (e) {
      errors++;
    }
  }
  
  if (seed % 500 === 0) process.stderr.write(`${seed}/2000...\n`);
}

const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
console.log(`\n=== Regex Differential Fuzzer Results ===`);
console.log(`Passed: ${passed}, Failed: ${failed}, Errors: ${errors}, Skipped patterns: ${skipped}`);
console.log(`Time: ${elapsed}s`);

if (mismatches.length > 0) {
  console.log(`\n=== First ${mismatches.length} mismatches ===`);
  for (const m of mismatches) {
    console.log(`  seed=${m.seed} pattern=/${m.pattern}/ str="${m.str}" ${m.mode || 'test'}: ours=${m.ours} native=${m.native}`);
  }
}

console.log(failed + errors === 0 ? '\nALL PASS ✓' : '\nFAILURES DETECTED');
process.exit(failed + errors > 0 ? 1 : 0);
