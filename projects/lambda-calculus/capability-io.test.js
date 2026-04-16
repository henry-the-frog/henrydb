import { strict as assert } from 'assert';
import {
  CAP_FILE_READ, CAP_FILE_WRITE, CAP_NETWORK, CAP_CONSOLE,
  CapabilitySet, pure, readFile, writeFile, httpGet, log,
  attenuate, fullCaps, readOnlyCaps, sandboxCaps
} from './capability-io.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

test('pure: no capabilities needed', () => {
  const r = pure(42).run(new CapabilitySet());
  assert.equal(r, 42);
});

test('readFile: needs FileRead', () => {
  const r = readFile('/etc/hosts').run(fullCaps);
  assert.ok(r.includes('/etc/hosts'));
});

test('readFile: fails without FileRead', () => {
  assert.throws(() => readFile('/etc/hosts').run(sandboxCaps), /Missing.*FileRead/);
});

test('writeFile: needs FileWrite', () => {
  const r = writeFile('/tmp/test', 'hello').run(fullCaps);
  assert.ok(r.includes('wrote'));
});

test('writeFile: fails with readOnly caps', () => {
  assert.throws(() => writeFile('/tmp/test', 'hello').run(readOnlyCaps), /Missing.*FileWrite/);
});

test('httpGet: needs Network', () => {
  const r = httpGet('https://example.com').run(fullCaps);
  assert.ok(r.includes('example.com'));
});

test('map: transform result', () => {
  const r = pure(21).map(x => x * 2).run(new CapabilitySet());
  assert.equal(r, 42);
});

test('then: chain computations', () => {
  const comp = readFile('/a').then(content => log(`Read: ${content}`));
  const r = comp.run(new CapabilitySet([CAP_FILE_READ, CAP_CONSOLE]));
  assert.ok(r.includes('Read:'));
});

test('then: chain fails if missing cap', () => {
  const comp = readFile('/a').then(() => httpGet('http://evil.com'));
  assert.throws(() => comp.run(readOnlyCaps), /Missing/);
});

// Capability set operations
test('capSet: subset check', () => {
  assert.ok(readOnlyCaps.isSubsetOf(fullCaps));
  assert.ok(!fullCaps.isSubsetOf(readOnlyCaps));
});

test('capSet: union', () => {
  const combined = readOnlyCaps.union(sandboxCaps);
  assert.ok(combined.has(CAP_FILE_READ));
  assert.ok(combined.has(CAP_CONSOLE));
});

test('attenuate: valid', () => {
  const comp = readFile('/a');
  attenuate(comp, fullCaps); // Should not throw
});

test('attenuate: invalid → error', () => {
  assert.throws(() => attenuate(httpGet('http://x'), readOnlyCaps), /Cannot attenuate/);
});

console.log(`\nCapability-safe IO tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
