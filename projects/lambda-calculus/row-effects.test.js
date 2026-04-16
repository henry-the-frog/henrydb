import { strict as assert } from 'assert';
import {
  EffectRow, effect, EffectSystem,
  stateEffect, ioEffect, exceptionEffect,
  isSubRow, rowUnion, rowDifference
} from './row-effects.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// Effect rows
test('row: has effect', () => {
  const row = new EffectRow([stateEffect, ioEffect]);
  assert.ok(row.has('State'));
  assert.ok(!row.has('Exception'));
});

test('row: add effect', () => {
  const row = new EffectRow([stateEffect]).add(ioEffect);
  assert.ok(row.has('IO'));
});

test('row: remove effect', () => {
  const row = new EffectRow([stateEffect, ioEffect]).remove('IO');
  assert.ok(!row.has('IO'));
  assert.ok(row.has('State'));
});

test('row: isEmpty', () => {
  assert.ok(new EffectRow().isEmpty());
  assert.ok(!new EffectRow([stateEffect]).isEmpty());
});

// Row subtyping
test('subRow: {State} <: {State, IO}', () => {
  const r1 = new EffectRow([stateEffect]);
  const r2 = new EffectRow([stateEffect, ioEffect]);
  assert.ok(isSubRow(r1, r2));
});

test('subRow: {State, IO} !<: {State}', () => {
  const r1 = new EffectRow([stateEffect, ioEffect]);
  const r2 = new EffectRow([stateEffect]);
  assert.ok(!isSubRow(r1, r2));
});

// Row operations
test('rowUnion: combines effects', () => {
  const u = rowUnion(new EffectRow([stateEffect]), new EffectRow([ioEffect]));
  assert.ok(u.has('State') && u.has('IO'));
});

test('rowDifference: removes handled effects', () => {
  const d = rowDifference(new EffectRow([stateEffect, ioEffect]), new EffectRow([stateEffect]));
  assert.ok(!d.has('State'));
  assert.ok(d.has('IO'));
});

// Effect system
test('EffectSystem: handle State', () => {
  const sys = new EffectSystem();
  let state = 0;
  sys.handle('State', new Map([
    ['get', () => state],
    ['put', ([v]) => { state = v; }],
  ]));
  sys.perform('State', 'put', 42);
  assert.equal(sys.perform('State', 'get'), 42);
});

test('EffectSystem: unhandled → error', () => {
  const sys = new EffectSystem();
  assert.throws(() => sys.perform('IO', 'print', 'hello'), /Unhandled/);
});

test('EffectSystem: multiple handlers', () => {
  const sys = new EffectSystem();
  let state = 0;
  const logs = [];
  sys.handle('State', new Map([['get', () => state], ['put', ([v]) => { state = v; }]]));
  sys.handle('Log', new Map([['log', ([msg]) => logs.push(msg)]]));
  
  sys.perform('State', 'put', 10);
  sys.perform('Log', 'log', `state is ${sys.perform('State', 'get')}`);
  assert.deepStrictEqual(logs, ['state is 10']);
});

console.log(`\nRow-level effects tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
