import { strict as assert } from 'assert';
import { Capability, attenuate, combine, revocable, guard, membrane } from './capabilities.js';

let passed = 0, failed = 0, total = 0;
function test(name, fn) { total++; try { fn(); passed++; } catch (e) { failed++; console.log(`  FAIL: ${name}\n    ${e.message}`); } }

test('can: allowed action', () => assert.ok(new Capability('fs', ['read', 'write']).can('read')));
test('can: denied action', () => assert.ok(!new Capability('fs', ['read']).can('write')));
test('attenuate: restrict', () => {
  const full = new Capability('fs', ['read', 'write', 'delete']);
  const ro = attenuate(full, ['read']);
  assert.ok(ro.can('read'));
  assert.ok(!ro.can('write'));
});
test('combine: merge caps', () => {
  const r = combine([new Capability('a', ['x']), new Capability('b', ['y'])]);
  assert.ok(r.can('x'));
  assert.ok(r.can('y'));
});
test('revocable: works before revoke', () => {
  const { cap } = revocable(new Capability('fs', ['read']));
  assert.ok(cap.can('read'));
});
test('revocable: denied after revoke', () => {
  const { cap, revoke } = revocable(new Capability('fs', ['read']));
  revoke();
  assert.ok(!cap.can('read'));
});
test('guard: allowed → runs', () => assert.equal(guard(new Capability('x', ['go']), 'go', () => 42), 42));
test('guard: denied → error', () => assert.throws(() => guard(new Capability('x', []), 'go', () => 42), /Denied/));
test('membrane: allowed prop', () => {
  const obj = membrane({ a: 1, b: 2 }, ['a']);
  assert.equal(obj.a, 1);
});
test('membrane: blocked prop', () => {
  const obj = membrane({ a: 1, b: 2 }, ['a']);
  assert.throws(() => obj.b, /Blocked/);
});

console.log(`\nCapabilities tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
