import { strict as assert } from 'assert';
import {
  quote, splice, liftCode,
  WorldSystem, publicVal, secretVal
} from './modal-types.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// Staged computation
test('quote/splice roundtrip', () => {
  assert.equal(splice(quote(42)), 42);
});

test('quote string', () => {
  assert.equal(splice(quote('hello')), 'hello');
});

test('liftCode: generates function application', () => {
  const code = liftCode('add', quote(1), quote(2));
  assert.ok(code.generate().includes('add'));
});

// Kripke worlds
test('box: holds at all accessible worlds', () => {
  const sys = new WorldSystem();
  const w0 = sys.addWorld('w0');
  const w1 = sys.addWorld('w1');
  const w2 = sys.addWorld('w2');
  sys.worlds.get('w1').values.set('x', 1);
  sys.worlds.get('w2').values.set('x', 2);
  sys.addEdge('w0', 'w1');
  sys.addEdge('w0', 'w2');
  
  assert.ok(sys.checkBox('w0', w => w.values.has('x'))); // x exists in all accessible
});

test('box: fails when not all satisfy', () => {
  const sys = new WorldSystem();
  sys.addWorld('w0');
  sys.addWorld('w1');
  sys.addWorld('w2');
  sys.worlds.get('w1').values.set('x', 1);
  // w2 doesn't have x
  sys.addEdge('w0', 'w1');
  sys.addEdge('w0', 'w2');
  
  assert.ok(!sys.checkBox('w0', w => w.values.has('x')));
});

test('diamond: holds at some accessible world', () => {
  const sys = new WorldSystem();
  sys.addWorld('w0');
  sys.addWorld('w1');
  sys.addWorld('w2');
  sys.worlds.get('w2').values.set('secret', true);
  sys.addEdge('w0', 'w1');
  sys.addEdge('w0', 'w2');
  
  assert.ok(sys.checkDiamond('w0', w => w.values.has('secret')));
});

test('diamond: fails when none satisfy', () => {
  const sys = new WorldSystem();
  sys.addWorld('w0');
  sys.addWorld('w1');
  sys.addEdge('w0', 'w1');
  
  assert.ok(!sys.checkDiamond('w0', w => w.values.has('x')));
});

// Information flow
test('public map: allowed', () => {
  const r = publicVal(21).map(x => x * 2);
  assert.equal(r.value, 42);
});

test('secret bind to secret: allowed', () => {
  const r = secretVal(42).bind(x => secretVal(x + 1));
  assert.equal(r.value, 43);
});

test('secret bind to public: BLOCKED', () => {
  assert.throws(() => secretVal(42).bind(x => publicVal(x)), /Information flow/);
});

test('public bind to secret: allowed (upgrade)', () => {
  const r = publicVal(42).bind(x => secretVal(x));
  assert.equal(r.value, 42);
});

console.log(`\nModal types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
