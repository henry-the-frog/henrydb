import { strict as assert } from 'assert';
import {
  LType, LFun, LPair, LResource,
  LINEAR, AFFINE, RELEVANT, UNRESTRICTED,
  linearCheck, UsageMap,
  lvar, llam, lapp, llet, lint, lbool, lstr, lunit, lpair, lletpair, lnew, luse, lclose,
  linear, affine, relevant, unrestricted
} from './linear.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

function expectOk(expr, msg = '') {
  const result = linearCheck(expr);
  assert.deepStrictEqual(result.errors, [], `Expected no errors${msg ? ': ' + msg : ''}\nGot: ${result.errors.join(', ')}`);
  return result;
}

function expectError(expr, pattern) {
  const result = linearCheck(expr);
  const found = result.errors.some(e => e.includes(pattern));
  assert.ok(found, `Expected error matching "${pattern}"\nGot: ${result.errors.join(', ') || '(no errors)'}`);
  return result;
}

// ============================================================
// Unrestricted (normal) types
// ============================================================

test('literal int', () => {
  expectOk(lint(42));
});

test('unrestricted variable used once', () => {
  const expr = llam('x', unrestricted('Int'), lvar('x'));
  expectOk(expr);
});

test('unrestricted variable used twice', () => {
  const expr = llam('x', unrestricted('Int'), lpair(lvar('x'), lvar('x')));
  expectOk(expr);
});

test('unrestricted variable not used', () => {
  const expr = llam('x', unrestricted('Int'), lint(42));
  expectOk(expr);
});

// ============================================================
// Linear types (exactly once)
// ============================================================

test('linear variable used once: OK', () => {
  const expr = llam('x', linear('Handle'), lvar('x'));
  expectOk(expr);
});

test('linear variable used zero times: ERROR', () => {
  const expr = llam('x', linear('Handle'), lint(42));
  expectError(expr, 'Linear variable');
});

test('linear variable used twice: ERROR', () => {
  const expr = llam('x', linear('Handle'), lpair(lvar('x'), lvar('x')));
  expectError(expr, 'Linear variable');
});

test('linear variable in let: used once', () => {
  const expr = llam('h', linear('Handle'),
    llet('result', luse(lvar('h'), lint(42)),
      lvar('result')));
  expectOk(expr);
});

// ============================================================
// Affine types (at most once — Rust's model)
// ============================================================

test('affine variable used once: OK', () => {
  const expr = llam('x', affine('Box'), lvar('x'));
  expectOk(expr);
});

test('affine variable not used: OK (can drop)', () => {
  const expr = llam('x', affine('Box'), lint(42));
  expectOk(expr);
});

test('affine variable used twice: ERROR', () => {
  const expr = llam('x', affine('Box'), lpair(lvar('x'), lvar('x')));
  expectError(expr, 'Affine variable');
});

// ============================================================
// Relevant types (at least once)
// ============================================================

test('relevant variable used once: OK', () => {
  const expr = llam('x', relevant('Log'), lvar('x'));
  expectOk(expr);
});

test('relevant variable used twice: OK (can duplicate)', () => {
  const expr = llam('x', relevant('Log'), lpair(lvar('x'), lvar('x')));
  expectOk(expr);
});

test('relevant variable not used: ERROR', () => {
  const expr = llam('x', relevant('Log'), lint(42));
  expectError(expr, 'Relevant variable');
});

// ============================================================
// Resource management patterns
// ============================================================

test('file handle: open, use, close pattern', () => {
  // open returns a linear handle, must close it
  const expr = llet('file', lnew('File'),
    llet('data', luse(lvar('file'), lstr('contents')),
      lvar('data')));
  expectOk(expr);
});

test('pair destructuring preserves linearity', () => {
  const expr = llam('p', new LPair(linear('A'), linear('B')),
    lletpair('a', 'b', lvar('p'),
      lpair(lvar('a'), lvar('b'))));
  expectOk(expr);
});

test('pair destructuring: drop linear component: ERROR', () => {
  const expr = llam('p', new LPair(linear('A'), linear('B')),
    lletpair('a', 'b', lvar('p'),
      lvar('a'))); // 'b' is dropped!
  expectError(expr, 'Linear variable');
});

// ============================================================
// Function types
// ============================================================

test('linear function: applied exactly once', () => {
  const fnType = new LFun('x', linear('Int'), new LType('Int'));
  const expr = llam('f', new LType('Fn', LINEAR),
    lapp(lvar('f'), lint(5)));
  expectOk(expr);
});

test('identity function on linear type', () => {
  const expr = llam('x', linear('Handle'), lvar('x'));
  const result = expectOk(expr);
  assert.ok(result.type instanceof LFun);
});

// ============================================================
// Complex programs
// ============================================================

test('swap: uses both components exactly once', () => {
  const pairType = new LPair(linear('A'), linear('B'));
  const expr = llam('p', pairType,
    lletpair('a', 'b', lvar('p'),
      lpair(lvar('b'), lvar('a'))));
  expectOk(expr);
});

test('nested let with linear: each used once', () => {
  const expr = llet('a', lint(1),
    llet('b', lint(2),
      lpair(lvar('a'), lvar('b'))));
  expectOk(expr);
});

test('Rust-like ownership: move semantics', () => {
  // After moving an affine value, it cannot be used again
  const expr = llam('x', affine('Vec'),
    llet('y', lvar('x'),  // move x into y
      lvar('y')));         // use y (x is consumed)
  expectOk(expr);
});

test('Rust-like: use after move ERROR', () => {
  // Using x after moving it to y
  const expr = llam('x', affine('Vec'),
    llet('y', lvar('x'),  // move x into y
      lpair(lvar('x'), lvar('y')))); // use x again! ERROR
  expectError(expr, 'Affine variable');
});

// ============================================================
// Usage tracking
// ============================================================

test('UsageMap tracks usage counts', () => {
  const usg = new UsageMap();
  usg.bind('x', linear('Int'));
  assert.equal(usg.getUsage('x'), 0);
  usg.use('x');
  assert.equal(usg.getUsage('x'), 1);
  usg.use('x');
  assert.equal(usg.getUsage('x'), 2);
});

test('UsageMap constraints: linear used 0', () => {
  const usg = new UsageMap();
  usg.bind('x', linear('Int'));
  const errors = usg.checkConstraints();
  assert.ok(errors.some(e => e.includes('Linear')));
});

test('UsageMap constraints: linear used 1', () => {
  const usg = new UsageMap();
  usg.bind('x', linear('Int'));
  usg.use('x');
  const errors = usg.checkConstraints();
  assert.deepStrictEqual(errors, []);
});

// ============================================================
// Report
// ============================================================

console.log(`\nLinear type tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
