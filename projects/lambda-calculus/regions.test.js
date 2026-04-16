import { strict as assert } from 'assert';
import { Region, deref, assign, letregion, RegionStack, annotateRegions, usedRegions, RNum, RLet, RLetRegion } from './regions.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('region: alloc and deref', () => {
  const r = new Region('r1');
  const ref = r.alloc(42);
  assert.equal(deref(ref), 42);
});

test('region: dealloc frees all', () => {
  const r = new Region('r1');
  r.alloc(1); r.alloc(2); r.alloc(3);
  assert.equal(r.size, 3);
  const freed = r.dealloc();
  assert.equal(freed, 3);
});

test('region: deref after dealloc → error', () => {
  const r = new Region('r1');
  const ref = r.alloc(42);
  r.dealloc();
  assert.throws(() => deref(ref), /dead/i);
});

test('region: assign and read back', () => {
  const r = new Region('r1');
  const ref = r.alloc(1);
  assign(ref, 99);
  assert.equal(deref(ref), 99);
});

test('letregion: creates and destroys', () => {
  const { result, freed } = letregion('r', r => {
    r.alloc(1); r.alloc(2);
    return 'done';
  });
  assert.equal(result, 'done');
  assert.equal(freed, 2);
});

test('letregion: nested regions', () => {
  const { result } = letregion('outer', outer => {
    const ref1 = outer.alloc(10);
    const { result: inner } = letregion('inner', inner => {
      const ref2 = inner.alloc(20);
      return deref(ref1) + deref(ref2);
    });
    return inner;
  });
  assert.equal(result, 30);
});

test('letregion: inner region dead after return', () => {
  let capturedRef;
  letregion('r', r => {
    capturedRef = r.alloc(42);
    return null;
  });
  assert.throws(() => deref(capturedRef), /dead/i);
});

// Region stack
test('region stack: push, alloc, pop', () => {
  const stack = new RegionStack();
  stack.push('r1');
  const ref = stack.alloc(42);
  assert.equal(deref(ref), 42);
  stack.pop();
  assert.throws(() => deref(ref), /dead/i);
});

test('region stack: nested', () => {
  const stack = new RegionStack();
  stack.push('r1');
  const ref1 = stack.alloc(1);
  stack.push('r2');
  const ref2 = stack.alloc(2);
  assert.equal(stack.depth, 2);
  stack.pop(); // kills r2
  assert.throws(() => deref(ref2), /dead/i);
  assert.equal(deref(ref1), 1); // r1 still alive
  stack.pop();
});

// Region annotation
test('annotateRegions: assigns current region', () => {
  const expr = new RNum(42);
  const annotated = annotateRegions(expr, 'ρ1');
  assert.equal(annotated.region, 'ρ1');
});

test('annotateRegions: letregion changes region', () => {
  const expr = new RLetRegion('ρ2', new RNum(42));
  const annotated = annotateRegions(expr);
  assert.equal(annotated.body.region, 'ρ2');
});

test('usedRegions: collects all', () => {
  const expr = { tag: 'RLet', name: 'x', region: 'ρ1',
    init: { tag: 'RNum', n: 1, region: 'ρ1' },
    body: { tag: 'RNum', n: 2, region: 'ρ2' }
  };
  const regions = usedRegions(expr);
  assert.ok(regions.has('ρ1'));
  assert.ok(regions.has('ρ2'));
});

console.log(`\nRegion-based memory tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
