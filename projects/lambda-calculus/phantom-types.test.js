import { strict as assert } from 'assert';
import {
  meters, feet, seconds, kilograms, add, sub, scale, feetToMeters, metersToFeet,
  createReadToken, createWriteToken, createAdminToken,
  requireRead, requireWrite, requireAdmin,
  userId, postId, lookupUser, lookupPost
} from './phantom-types.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try { fn(); passed++; } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

// Unit-safe arithmetic
test('add same units: meters + meters', () => {
  const r = add(meters(3), meters(4));
  assert.equal(r.value, 7);
});

test('add different units: meters + feet → ERROR', () => {
  assert.throws(() => add(meters(3), feet(4)), /mismatch/i);
});

test('sub same units', () => {
  const r = sub(meters(10), meters(3));
  assert.equal(r.value, 7);
});

test('scale: 3 × 5 meters = 15 meters', () => {
  const r = scale(3, meters(5));
  assert.equal(r.value, 15);
});

test('conversion: feet to meters', () => {
  const r = feetToMeters(feet(1));
  assert.ok(Math.abs(r.value - 0.3048) < 0.001);
});

test('conversion: meters to feet', () => {
  const r = metersToFeet(meters(1));
  assert.ok(Math.abs(r.value - 3.28084) < 0.01);
});

// Capability tokens
test('read token: requireRead passes', () => {
  requireRead(createReadToken()); // Should not throw
});

test('write token: requireRead fails', () => {
  assert.throws(() => requireRead(createWriteToken()), /Read/);
});

test('admin token: passes all checks', () => {
  const admin = createAdminToken();
  requireRead(admin);
  requireWrite(admin);
  requireAdmin(admin);
});

// Tagged IDs
test('userId → lookupUser works', () => {
  const user = lookupUser(userId(42));
  assert.equal(user.id, 42);
});

test('postId → lookupUser fails', () => {
  assert.throws(() => lookupUser(postId(42)), /Expected UserId/);
});

test('userId → lookupPost fails', () => {
  assert.throws(() => lookupPost(userId(42)), /Expected PostId/);
});

console.log(`\nPhantom types tests: ${passed}/${total} passed` + (failed ? ` (${failed} failed)` : ''));
if (failed) process.exit(1);
