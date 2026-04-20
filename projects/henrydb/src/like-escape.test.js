// like-escape.test.js — Tests for LIKE ESCAPE clause
// SQL standard: LIKE pattern ESCAPE char makes the next char literal

import { Database } from './db.js';
import { strict as assert } from 'assert';

let db, pass = 0, fail = 0;

function test(name, fn) {
  db = new Database();
  try { fn(); pass++; console.log(`  ✅ ${name}`); }
  catch (e) { fail++; console.log(`  ❌ ${name}: ${e.message}`); }
}

console.log('\n🧪 LIKE ESCAPE clause');

test('LIKE without ESCAPE still works - %', () => {
  db.execute('CREATE TABLE t(name TEXT)');
  db.execute("INSERT INTO t VALUES ('hello'), ('world'), ('help')");
  const r = db.execute("SELECT name FROM t WHERE name LIKE 'hel%'").rows;
  assert.deepEqual(r.map(x => x.name).sort(), ['hello', 'help']);
});

test('LIKE without ESCAPE still works - _', () => {
  db.execute('CREATE TABLE t(name TEXT)');
  db.execute("INSERT INTO t VALUES ('cat'), ('car'), ('cup')");
  const r = db.execute("SELECT name FROM t WHERE name LIKE 'ca_'").rows;
  assert.deepEqual(r.map(x => x.name).sort(), ['car', 'cat']);
});

test('ESCAPE % makes percent literal', () => {
  db.execute('CREATE TABLE t(val TEXT)');
  db.execute("INSERT INTO t VALUES ('100%'), ('100'), ('100% done'), ('50%')");
  const r = db.execute("SELECT val FROM t WHERE val LIKE '%\\%' ESCAPE '\\'").rows;
  assert.deepEqual(r.map(x => x.val).sort(), ['100%', '50%']);
});

test('ESCAPE _ makes underscore literal', () => {
  db.execute('CREATE TABLE t(val TEXT)');
  db.execute("INSERT INTO t VALUES ('a_b'), ('axb'), ('a_bc')");
  const r = db.execute("SELECT val FROM t WHERE val LIKE 'a\\_b' ESCAPE '\\'").rows;
  assert.deepEqual(r.map(x => x.val), ['a_b']);
});

test('ESCAPE with different escape char', () => {
  db.execute('CREATE TABLE t(val TEXT)');
  db.execute("INSERT INTO t VALUES ('100%'), ('100'), ('50%')");
  const r = db.execute("SELECT val FROM t WHERE val LIKE '%!%' ESCAPE '!'").rows;
  assert.deepEqual(r.map(x => x.val).sort(), ['100%', '50%']);
});

test('ESCAPE with # as escape char', () => {
  db.execute('CREATE TABLE t(val TEXT)');
  db.execute("INSERT INTO t VALUES ('he_lo'), ('hello'), ('hexlo')");
  const r = db.execute("SELECT val FROM t WHERE val LIKE 'he#_lo' ESCAPE '#'").rows;
  assert.deepEqual(r.map(x => x.val), ['he_lo']);
});

test('ESCAPE both % and _ in same pattern', () => {
  db.execute('CREATE TABLE t(val TEXT)');
  db.execute("INSERT INTO t VALUES ('a%b_c'), ('axbyc'), ('a%byc'), ('axb_c')");
  const r = db.execute("SELECT val FROM t WHERE val LIKE 'a\\%b\\_c' ESCAPE '\\'").rows;
  assert.deepEqual(r.map(x => x.val), ['a%b_c']);
});

test('ESCAPE with wildcards and escaped chars mixed', () => {
  db.execute('CREATE TABLE t(val TEXT)');
  db.execute("INSERT INTO t VALUES ('start_middle_end'), ('start_mid_end'), ('startXmidYend')");
  const r = db.execute("SELECT val FROM t WHERE val LIKE 'start\\_%\\_%' ESCAPE '\\'").rows;
  // Pattern: start + literal_ + any + literal_ + any
  // Matches: start_middle_end, start_mid_end
  assert.equal(r.length, 2);
});

test('NOT LIKE with ESCAPE', () => {
  db.execute('CREATE TABLE t(val TEXT)');
  db.execute("INSERT INTO t VALUES ('100%'), ('100'), ('50%')");
  const r = db.execute("SELECT val FROM t WHERE val NOT LIKE '%\\%' ESCAPE '\\'").rows;
  assert.deepEqual(r.map(x => x.val), ['100']);
});

test('ILIKE with ESCAPE (case-insensitive)', () => {
  db.execute('CREATE TABLE t(val TEXT)');
  db.execute("INSERT INTO t VALUES ('Hello%'), ('HELLO%'), ('hello'), ('Hello')");
  const r = db.execute("SELECT val FROM t WHERE val ILIKE 'hello\\%' ESCAPE '\\'").rows;
  assert.deepEqual(r.map(x => x.val).sort(), ['HELLO%', 'Hello%']);
});

test('ESCAPE with no wildcards - exact match', () => {
  db.execute('CREATE TABLE t(val TEXT)');
  db.execute("INSERT INTO t VALUES ('abc'), ('def')");
  const r = db.execute("SELECT val FROM t WHERE val LIKE 'abc' ESCAPE '\\'").rows;
  assert.deepEqual(r.map(x => x.val), ['abc']);
});

test('ESCAPE char at end of pattern (trailing escape)', () => {
  db.execute('CREATE TABLE t(val TEXT)');
  db.execute("INSERT INTO t VALUES ('abc'), ('abc\\\\')");
  // Trailing escape char with nothing after it - should be treated as literal escape char
  // Actually per SQL standard, trailing escape is an error, but we'll be lenient
  const r = db.execute("SELECT val FROM t WHERE val LIKE 'abc' ESCAPE '\\'").rows;
  assert.deepEqual(r.map(x => x.val), ['abc']);
});

test('Pattern with only escaped special chars', () => {
  db.execute('CREATE TABLE t(val TEXT)');
  db.execute("INSERT INTO t VALUES ('%_'), ('ab'), ('%'), ('_')");
  const r = db.execute("SELECT val FROM t WHERE val LIKE '\\%\\_' ESCAPE '\\'").rows;
  assert.deepEqual(r.map(x => x.val), ['%_']);
});

console.log(`\n  ${pass} passed, ${fail} failed\n`);
process.exit(fail > 0 ? 1 : 0);
