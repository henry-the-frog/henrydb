// forth/test.js — Comprehensive test suite
'use strict';

const { Forth } = require('./forth.js');

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try {
    fn();
    passed++;
  } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

function eq(a, b, msg = '') {
  if (JSON.stringify(a) !== JSON.stringify(b)) {
    throw new Error(`Expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}${msg ? ' — ' + msg : ''}`);
  }
}

function throws(fn) {
  try { fn(); throw new Error('Expected error but succeeded'); }
  catch (e) { if (e.message === 'Expected error but succeeded') throw e; }
}

// ═══════════════════════════════════════════
// Arithmetic
// ═══════════════════════════════════════════
console.log('── Arithmetic ──');

test('addition', () => {
  const f = new Forth();
  f.eval('3 4 +');
  eq(f.getStack(), [7]);
});

test('subtraction', () => {
  const f = new Forth();
  f.eval('10 3 -');
  eq(f.getStack(), [7]);
});

test('multiplication', () => {
  const f = new Forth();
  f.eval('6 7 *');
  eq(f.getStack(), [42]);
});

test('division', () => {
  const f = new Forth();
  f.eval('17 5 /');
  eq(f.getStack(), [3]);
});

test('modulo', () => {
  const f = new Forth();
  f.eval('17 5 mod');
  eq(f.getStack(), [2]);
});

test('negate', () => {
  const f = new Forth();
  f.eval('5 negate');
  eq(f.getStack(), [-5]);
});

test('abs', () => {
  const f = new Forth();
  f.eval('-7 abs');
  eq(f.getStack(), [7]);
});

test('min max', () => {
  const f = new Forth();
  f.eval('3 5 min');
  eq(f.getStack(), [3]);
  f.reset();
  f.eval('3 5 max');
  eq(f.getStack(), [5]);
});

test('/mod', () => {
  const f = new Forth();
  f.eval('17 5 /mod');
  eq(f.getStack(), [2, 3]); // remainder, quotient
});

test('complex arithmetic', () => {
  const f = new Forth();
  f.eval('2 3 + 4 *');
  eq(f.getStack(), [20]);
});

// ═══════════════════════════════════════════
// Stack Manipulation
// ═══════════════════════════════════════════
console.log('── Stack Manipulation ──');

test('dup', () => {
  const f = new Forth();
  f.eval('5 dup');
  eq(f.getStack(), [5, 5]);
});

test('drop', () => {
  const f = new Forth();
  f.eval('1 2 3 drop');
  eq(f.getStack(), [1, 2]);
});

test('swap', () => {
  const f = new Forth();
  f.eval('1 2 swap');
  eq(f.getStack(), [2, 1]);
});

test('over', () => {
  const f = new Forth();
  f.eval('1 2 over');
  eq(f.getStack(), [1, 2, 1]);
});

test('rot', () => {
  const f = new Forth();
  f.eval('1 2 3 rot');
  eq(f.getStack(), [2, 3, 1]);
});

test('2dup', () => {
  const f = new Forth();
  f.eval('1 2 2dup');
  eq(f.getStack(), [1, 2, 1, 2]);
});

test('2drop', () => {
  const f = new Forth();
  f.eval('1 2 3 4 2drop');
  eq(f.getStack(), [1, 2]);
});

test('nip', () => {
  const f = new Forth();
  f.eval('1 2 nip');
  eq(f.getStack(), [2]);
});

test('tuck', () => {
  const f = new Forth();
  f.eval('1 2 tuck');
  eq(f.getStack(), [2, 1, 2]);
});

test('?dup non-zero', () => {
  const f = new Forth();
  f.eval('5 ?dup');
  eq(f.getStack(), [5, 5]);
});

test('?dup zero', () => {
  const f = new Forth();
  f.eval('0 ?dup');
  eq(f.getStack(), [0]);
});

test('depth', () => {
  const f = new Forth();
  f.eval('1 2 3 depth');
  eq(f.getStack(), [1, 2, 3, 3]);
});

test('stack underflow', () => {
  const f = new Forth();
  throws(() => f.eval('+'));
});

// ═══════════════════════════════════════════
// Comparison
// ═══════════════════════════════════════════
console.log('── Comparison ──');

test('equal true', () => {
  const f = new Forth();
  f.eval('5 5 =');
  eq(f.getStack(), [-1]);
});

test('equal false', () => {
  const f = new Forth();
  f.eval('5 3 =');
  eq(f.getStack(), [0]);
});

test('less than', () => {
  const f = new Forth();
  f.eval('3 5 <');
  eq(f.getStack(), [-1]);
});

test('greater than', () => {
  const f = new Forth();
  f.eval('5 3 >');
  eq(f.getStack(), [-1]);
});

test('not equal', () => {
  const f = new Forth();
  f.eval('1 2 <>');
  eq(f.getStack(), [-1]);
});

test('0=', () => {
  const f = new Forth();
  f.eval('0 0=');
  eq(f.getStack(), [-1]);
  f.reset();
  f.eval('5 0=');
  eq(f.getStack(), [0]);
});

// ═══════════════════════════════════════════
// Boolean
// ═══════════════════════════════════════════
console.log('── Boolean ──');

test('and', () => {
  const f = new Forth();
  f.eval('-1 -1 and');
  eq(f.getStack(), [-1]);
  f.reset();
  f.eval('-1 0 and');
  eq(f.getStack(), [0]);
});

test('or', () => {
  const f = new Forth();
  f.eval('0 -1 or');
  eq(f.getStack(), [-1]);
});

test('invert', () => {
  const f = new Forth();
  f.eval('-1 invert');
  eq(f.getStack(), [0]);
  f.reset();
  f.eval('0 invert');
  eq(f.getStack(), [-1]);
});

test('true false', () => {
  const f = new Forth();
  f.eval('true false');
  eq(f.getStack(), [-1, 0]);
});

// ═══════════════════════════════════════════
// I/O
// ═══════════════════════════════════════════
console.log('── I/O ──');

test('dot prints', () => {
  const f = new Forth();
  f.eval('42 .');
  eq(f.getOutput(), '42');
});

test('cr outputs newline', () => {
  const f = new Forth();
  f.eval('1 . cr 2 .');
  eq(f.getOutput(), '1\n2');
});

test('emit', () => {
  const f = new Forth();
  f.eval('65 emit');
  eq(f.getOutput(), 'A');
});

test('.s shows stack', () => {
  const f = new Forth();
  f.eval('1 2 3 .s');
  eq(f.getOutput(), '<3> 1 2 3');
});

test('space', () => {
  const f = new Forth();
  f.eval('1 . space 2 .');
  eq(f.getOutput(), '1 2');
});

test('string literal', () => {
  const f = new Forth();
  f.eval('." Hello, World!"');
  eq(f.getOutput(), 'Hello, World!');
});

// ═══════════════════════════════════════════
// Word Definitions
// ═══════════════════════════════════════════
console.log('── Definitions ──');

test('simple definition', () => {
  const f = new Forth();
  f.eval(': square dup * ;');
  f.eval('5 square');
  eq(f.getStack(), [25]);
});

test('definition using other words', () => {
  const f = new Forth();
  f.eval(': square dup * ;');
  f.eval(': cube dup square * ;');
  f.eval('3 cube');
  eq(f.getStack(), [27]);
});

test('definition with output', () => {
  const f = new Forth();
  f.eval(': greet ." Hello" ;');
  f.eval('greet');
  eq(f.getOutput(), 'Hello');
});

test('definition redefine', () => {
  const f = new Forth();
  f.eval(': x 1 ;');
  f.eval('x');
  eq(f.getStack(), [1]);
  f.eval(': x 2 ;');
  f.eval('x');
  eq(f.getStack(), [1, 2]);
});

// ═══════════════════════════════════════════
// Control Flow
// ═══════════════════════════════════════════
console.log('── Control Flow ──');

test('if true', () => {
  const f = new Forth();
  f.eval(': test 1 = if 10 then ;');
  f.eval('1 test');
  eq(f.getStack(), [10]);
});

test('if false', () => {
  const f = new Forth();
  f.eval(': test 1 = if 10 then ;');
  f.eval('2 test');
  eq(f.getStack(), []);
});

test('if else', () => {
  const f = new Forth();
  f.eval(': abs-val dup 0< if negate else then ;');
  f.eval('-5 abs-val');
  eq(f.getStack(), [5]);
  f.reset();
  f.eval('3 abs-val');
  eq(f.getStack(), [3]);
});

test('if else then', () => {
  const f = new Forth();
  f.eval(': sign dup 0> if drop 1 else 0< if -1 else 0 then then ;');
  f.eval('5 sign');
  eq(f.getStack(), [1]);
  f.reset();
  f.eval('-3 sign');
  eq(f.getStack(), [-1]);
  f.reset();
  f.eval('0 sign');
  eq(f.getStack(), [0]);
});

// ═══════════════════════════════════════════
// Loops
// ═══════════════════════════════════════════
console.log('── Loops ──');

test('do loop', () => {
  const f = new Forth();
  f.eval(': count 5 0 do i . space loop ;');
  f.eval('count');
  eq(f.getOutput(), '0 1 2 3 4 ');
});

test('do loop sum', () => {
  const f = new Forth();
  f.eval(': sum 0 10 0 do i + loop ;');
  f.eval('sum');
  eq(f.getStack(), [45]);
});

test('begin until', () => {
  const f = new Forth();
  f.eval(': countdown 5 begin dup . space 1 - dup 0= until drop ;');
  f.eval('countdown');
  eq(f.getOutput(), '5 4 3 2 1 ');
});

test('begin while repeat', () => {
  const f = new Forth();
  f.eval(': countdown 5 begin dup 0> while dup . space 1 - repeat drop ;');
  f.eval('countdown');
  eq(f.getOutput(), '5 4 3 2 1 ');
});

// ═══════════════════════════════════════════
// Variables & Constants
// ═══════════════════════════════════════════
console.log('── Variables ──');

test('variable', () => {
  const f = new Forth();
  f.eval('variable x');
  f.eval('42 x !');
  f.eval('x @');
  eq(f.getStack(), [42]);
});

test('variable +!', () => {
  const f = new Forth();
  f.eval('variable count');
  f.eval('10 count !');
  f.eval('5 count +!');
  f.eval('count @');
  eq(f.getStack(), [15]);
});

test('constant', () => {
  const f = new Forth();
  f.eval('42 constant answer');
  f.eval('answer');
  eq(f.getStack(), [42]);
});

test('multiple variables', () => {
  const f = new Forth();
  f.eval('variable x variable y');
  f.eval('3 x ! 4 y !');
  f.eval('x @ y @ +');
  eq(f.getStack(), [7]);
});

// ═══════════════════════════════════════════
// Recursion
// ═══════════════════════════════════════════
console.log('── Recursion ──');

test('recursive factorial', () => {
  const f = new Forth();
  f.eval(': fact dup 1 > if dup 1 - recurse * else drop 1 then ;');
  f.eval('5 fact');
  eq(f.getStack(), [120]);
});

test('recursive fibonacci', () => {
  const f = new Forth();
  f.eval(': fib dup 2 < if else dup 1 - recurse swap 2 - recurse + then ;');
  f.eval('10 fib');
  eq(f.getStack(), [55]);
});

// ═══════════════════════════════════════════
// Return Stack
// ═══════════════════════════════════════════
console.log('── Return Stack ──');

test('>r r>', () => {
  const f = new Forth();
  f.eval('1 2 3 >r + r>');
  eq(f.getStack(), [3, 3]);
});

test('r@', () => {
  const f = new Forth();
  f.eval('42 >r r@ r>');
  eq(f.getStack(), [42, 42]);
});

// ═══════════════════════════════════════════
// Comments
// ═══════════════════════════════════════════
console.log('── Comments ──');

test('backslash comment', () => {
  const f = new Forth();
  f.eval('1 2 + \\ this is a comment');
  eq(f.getStack(), [3]);
});

test('paren comment', () => {
  const f = new Forth();
  f.eval('1 ( this is a comment ) 2 +');
  eq(f.getStack(), [3]);
});

// ═══════════════════════════════════════════
// Complex Programs
// ═══════════════════════════════════════════
console.log('── Complex Programs ──');

test('FizzBuzz', () => {
  const f = new Forth();
  f.eval(`
    : fizzbuzz
      16 1 do
        i 15 mod 0= if ." FizzBuzz" else
        i 3 mod 0= if ." Fizz" else
        i 5 mod 0= if ." Buzz" else
        i .
        then then then
        space
      loop
    ;
  `);
  f.eval('fizzbuzz');
  const out = f.getOutput();
  eq(out.includes('1'), true);
  eq(out.includes('Fizz'), true);
  eq(out.includes('Buzz'), true);
  eq(out.includes('FizzBuzz'), true);
});

test('Pythagorean check', () => {
  const f = new Forth();
  f.eval(': sq dup * ;');
  f.eval(': pythag? rot sq rot sq + swap sq = ;');
  f.eval('3 4 5 pythag?');
  eq(f.getStack(), [-1]);
  f.reset();
  f.eval('3 4 6 pythag?');
  eq(f.getStack(), [0]);
});

test('GCD', () => {
  const f = new Forth();
  // Standard Forth GCD: tuck mod swaps a,b and computes a%b → b, a%b
  f.eval(': gcd begin dup 0<> while tuck mod repeat drop ;');
  f.eval('12 8 gcd');
  eq(f.getStack(), [4]);
});

test('array sum with do loop', () => {
  const f = new Forth();
  // Create array starting at here
  f.eval('here');  // save start address
  f.eval('10 , 20 , 30 , 40 , 50 ,');
  // Address is on stack, sum elements
  f.eval(': arr-sum 0 swap 5 0 do 2dup + @ rot + rot rot 1 + loop drop drop ;');
  // Simpler approach: use variable
  const f2 = new Forth();
  f2.eval('variable arr here arr !');
  f2.eval('10 , 20 , 30 , 40 , 50 ,');
  f2.eval('0 5 0 do arr @ i + @ + loop');
  eq(f2.getStack(), [150]);
});

test('star triangle', () => {
  const f = new Forth();
  f.eval(': stars 0 do 42 emit loop ;');
  f.eval(': triangle 5 1 do i stars cr loop ;');
  f.eval('triangle');
  const out = f.getOutput();
  eq(out.includes('*'), true);
  eq(out.includes('\n'), true);
});

test('multiple evaluations maintain state', () => {
  const f = new Forth();
  f.eval('1 2 3');
  f.eval('+ +');
  eq(f.getStack(), [6]);
});

test('undefined word error', () => {
  const f = new Forth();
  throws(() => f.eval('nonexistent'));
});

// ═══════════════════════════════════════════
// Edge Cases
// ═══════════════════════════════════════════
console.log('── Edge Cases ──');

test('empty input', () => {
  const f = new Forth();
  f.eval('');
  eq(f.getStack(), []);
});

test('negative numbers', () => {
  const f = new Forth();
  f.eval('-5 3 +');
  eq(f.getStack(), [-2]);
});

test('large numbers', () => {
  const f = new Forth();
  f.eval('1000000 1000000 *');
  eq(f.getStack(), [1000000000000]);
});

test('nested definitions', () => {
  const f = new Forth();
  f.eval(': double 2 * ;');
  f.eval(': quadruple double double ;');
  f.eval('5 quadruple');
  eq(f.getStack(), [20]);
});

test('2swap', () => {
  const f = new Forth();
  f.eval('1 2 3 4 2swap');
  eq(f.getStack(), [3, 4, 1, 2]);
});

// ═══════════════════════════════════════════

console.log(`\n══════════════════════════════`);
console.log(`  ${passed}/${total} passed, ${failed} failed`);
console.log(`══════════════════════════════`);
process.exit(failed > 0 ? 1 : 0);
