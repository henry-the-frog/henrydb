// Forth interpreter fuzzer: random Forth programs, verify no crashes
import { Forth } from './forth.js';

function seeded(seed) {
  let s = seed;
  return () => { s = (s * 1103515245 + 12345) & 0x7fffffff; return s / 0x7fffffff; };
}

function randomInt(rng, min, max) { return Math.floor(rng() * (max - min + 1)) + min; }
function randomChoice(rng, arr) { return arr[Math.floor(rng() * arr.length)]; }

// Generate random Forth programs
function randomProgram(rng) {
  const lines = [];
  const numStatements = randomInt(rng, 1, 8);
  const definedWords = [];
  
  for (let s = 0; s < numStatements; s++) {
    const r = rng();
    
    if (r < 0.3) {
      // Arithmetic: push numbers and compute
      const a = randomInt(rng, -100, 100);
      const b = randomInt(rng, 1, 100); // avoid division by zero
      const op = randomChoice(rng, ['+', '-', '*']);
      lines.push(`${a} ${b} ${op}`);
    } else if (r < 0.45) {
      // Stack ops
      const n = randomInt(rng, 1, 50);
      const op = randomChoice(rng, ['dup', 'drop', 'swap', 'over', 'rot']);
      lines.push(`${n} ${n + 1} ${op}`);
    } else if (r < 0.55) {
      // Word definition
      const name = 'w' + randomInt(rng, 0, 100);
      const ops = [];
      for (let i = 0; i < randomInt(rng, 1, 4); i++) {
        ops.push(randomChoice(rng, ['dup', '+', '*', '1+', '1-']));
      }
      lines.push(`: ${name} ${ops.join(' ')} ;`);
      definedWords.push(name);
    } else if (r < 0.65 && definedWords.length > 0) {
      // Use defined word
      const n = randomInt(rng, 1, 50);
      const word = randomChoice(rng, definedWords);
      lines.push(`${n} ${word}`);
    } else if (r < 0.75) {
      // Comparison and print
      const a = randomInt(rng, 0, 50);
      const b = randomInt(rng, 0, 50);
      lines.push(`${a} ${b} = .`);
    } else if (r < 0.85) {
      // Do loop (small iterations)
      const limit = randomInt(rng, 1, 5);
      lines.push(`${limit} 0 do i loop`);
    } else {
      // Simple print
      lines.push(`${randomInt(rng, -100, 100)} .`);
    }
  }
  
  return lines.join(' ');
}

// Verify properties
function verify(f) {
  // Stack should have reasonable size
  if (f.stack.length > 1000) return { ok: false, issue: 'stack overflow' };
  // Output should be strings
  for (const o of f.output) {
    if (typeof o !== 'string' && typeof o !== 'number') {
      return { ok: false, issue: `bad output: ${typeof o}` };
    }
  }
  return { ok: true };
}

let passed = 0, expectedErrors = 0, crashes = 0;
const crashPrograms = [];
const startTime = Date.now();

for (let seed = 1; seed <= 5000; seed++) {
  const rng = seeded(seed);
  const program = randomProgram(rng);
  
  try {
    const f = new Forth();
    f.eval(program);
    
    const check = verify(f);
    if (!check.ok) {
      crashes++;
      if (crashPrograms.length < 10) crashPrograms.push({ seed, program, issue: check.issue });
    } else {
      passed++;
    }
  } catch (e) {
    // Expected errors: stack underflow, unknown word, etc.
    if (e.message.includes('underflow') || e.message.includes('Unknown') ||
        e.message.includes('overflow') || e.message.includes('empty') ||
        e.message.includes('undefined') || e.message.includes('Unexpected')) {
      expectedErrors++;
    } else {
      crashes++;
      if (crashPrograms.length < 10) crashPrograms.push({ seed, program, error: e.message });
    }
  }
  
  if (seed % 1000 === 0) process.stderr.write(`${seed}/5000...\n`);
}

const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
console.log(`\n=== Forth Fuzzer Results ===`);
console.log(`Passed: ${passed}, Expected errors: ${expectedErrors}, Crashes: ${crashes}`);
console.log(`Time: ${elapsed}s`);

if (crashPrograms.length > 0) {
  console.log(`\n=== Crashes ===`);
  for (const c of crashPrograms) {
    console.log(`  seed=${c.seed}: ${c.error || c.issue}`);
    console.log(`    program: ${c.program.slice(0, 100)}`);
  }
}

console.log(crashes === 0 ? '\nNO CRASHES ✓' : '\nCRASHES DETECTED');
process.exit(crashes > 0 ? 1 : 0);
