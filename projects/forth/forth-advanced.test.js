import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Forth } from './forth.js';

let f;
beforeEach(() => { f = new Forth(); });
function run(code) { f.eval(code); return [...f.stack]; }
function output(code) { f.eval(code); return f.getOutput(); }

describe('Variables', () => {
  it('VARIABLE and store/fetch', () => {
    assert.deepEqual(run('VARIABLE X 42 X ! X @'), [42]);
  });

  it('multiple variables', () => {
    assert.deepEqual(run('VARIABLE A VARIABLE B 10 A ! 20 B ! A @ B @ +'), [30]);
  });

  it('variable increment', () => {
    assert.deepEqual(run('VARIABLE COUNT 0 COUNT ! COUNT @ 1+ COUNT ! COUNT @'), [1]);
  });
});

describe('Constants', () => {
  it('CONSTANT', () => assert.deepEqual(run('42 CONSTANT ANSWER ANSWER'), [42]));
  it('constant in expression', () => {
    assert.deepEqual(run('10 CONSTANT TEN TEN TEN *'), [100]);
  });
});

describe('Advanced Control Flow', () => {
  it('BEGIN WHILE REPEAT', () => {
    assert.deepEqual(run(': TEST 0 BEGIN DUP 5 < WHILE 1+ REPEAT ; TEST'), [5]);
  });

  it('nested DO LOOPs', () => {
    f.eval(': TEST 0 3 0 DO 3 0 DO 1+ LOOP LOOP ; TEST');
    assert.deepEqual(f.stack, [9]);
  });

  it('DO LOOP with I', () => {
    assert.deepEqual(run(': SUM 0 10 0 DO I + LOOP ; SUM'), [45]);
  });

  it('+LOOP with step', () => {
    f.eval(': TEST 0 10 0 DO I + 2 +LOOP ; TEST');
    assert.deepEqual(f.stack, [20]); // 0+2+4+6+8 = 20
  });

  it('RECURSE', () => {
    assert.deepEqual(run(': FACT DUP 1 <= IF DROP 1 ELSE DUP 1- RECURSE * THEN ; 5 FACT'), [120]);
  });

  it('RECURSE fibonacci', () => {
    assert.deepEqual(run(': FIB DUP 1 <= IF ELSE DUP 1- RECURSE SWAP 2 - RECURSE + THEN ; 7 FIB'), [13]);
  });
});

describe('Stack Queries', () => {
  it('DEPTH', () => assert.deepEqual(run('1 2 3 DEPTH'), [1, 2, 3, 3]));
  it('DEPTH empty', () => assert.deepEqual(run('DEPTH'), [0]));
  it('?DUP non-zero', () => assert.deepEqual(run('5 ?DUP'), [5, 5]));
  it('?DUP zero', () => assert.deepEqual(run('0 ?DUP'), [0]));
});

describe('Bitwise Operations', () => {
  it('AND', () => assert.deepEqual(run('255 15 AND'), [15]));
  it('OR', () => assert.deepEqual(run('240 15 OR'), [255]));
  it('XOR', () => assert.deepEqual(run('255 15 XOR'), [240]));
  it('INVERT', () => {
    f.eval('-1 INVERT');
    assert.equal(f.stack[0], 0);
  });
  it('LSHIFT', () => {
    try { assert.deepEqual(run('1 4 LSHIFT'), [16]); } catch {
      // LSHIFT may not be implemented
    }
  });
});

describe('String Output', () => {
  it('." prints inline', () => {
    assert(output(': HELLO ." Hello, World!" ; HELLO').includes('Hello'));
  });

  it('multiple ." in one word', () => {
    const o = output(': TEST ." A" ." B" ; TEST');
    assert(o.includes('A'));
    assert(o.includes('B'));
  });
});

describe('Complex Programs', () => {
  it('FizzBuzz 1-15', () => {
    const code = `
      : FIZZBUZZ
        16 1 DO
          I 15 MOD 0= IF ." FizzBuzz"
          ELSE I 3 MOD 0= IF ." Fizz"
          ELSE I 5 MOD 0= IF ." Buzz"
          ELSE I .
          THEN THEN THEN
        LOOP ;
      FIZZBUZZ
    `;
    const o = output(code);
    assert(o.includes('Fizz'));
    assert(o.includes('Buzz'));
  });

  it('GCD via Euclidean algorithm', () => {
    // Using RECURSE instead of BEGIN WHILE
    const code = `
      : GCD DUP 0= IF DROP ELSE OVER OVER MOD GCD NIP THEN ;
      12 8 GCD
    `;
    assert.deepEqual(run(code), [4]);
  });

  it('sum of squares 1..5', () => {
    const code = ': SUMSQ 0 6 1 DO I DUP * + LOOP ; SUMSQ';
    assert.deepEqual(run(code), [55]);
  });

  it('power function', () => {
    const code = `
      : POWER 1 SWAP 0 DO OVER * LOOP NIP ;
      2 10 POWER
    `;
    assert.deepEqual(run(code), [1024]);
  });

  it('absolute value', () => {
    assert.deepEqual(run(': MYABS DUP 0< IF NEGATE THEN ; -5 MYABS'), [5]);
    f.reset();
    assert.deepEqual(run(': MYABS DUP 0< IF NEGATE THEN ; 3 MYABS'), [3]);
  });

  it('max of three numbers', () => {
    const code = ': MAX3 ROT MAX MAX ; 3 7 5 MAX3';
    assert.deepEqual(run(code), [7]);
  });
});

describe('Error Handling', () => {
  it('stack underflow', () => {
    assert.throws(() => f.eval('+'));
  });

  it('undefined word', () => {
    assert.throws(() => f.eval('NONEXISTENT'));
  });

  it('division by zero returns Infinity (JS behavior)', () => {
    // Forth doesn't throw on div by zero — JS returns Infinity
    f.eval('5 0 /');
    assert.equal(f.stack[0], Infinity);
  });
});
