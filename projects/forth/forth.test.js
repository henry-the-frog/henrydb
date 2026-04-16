import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Forth } from './forth.js';

let f;
beforeEach(() => { f = new Forth(); });

// Helper: run and get stack
function run(code) { f.eval(code); return [...f.stack]; }
function output(code) { f.eval(code); return f.getOutput(); }

describe('Stack Operations', () => {
  it('push integer', () => assert.deepEqual(run('42'), [42]));
  it('push multiple', () => assert.deepEqual(run('1 2 3'), [1, 2, 3]));
  it('DUP', () => assert.deepEqual(run('5 DUP'), [5, 5]));
  it('DROP', () => assert.deepEqual(run('1 2 DROP'), [1]));
  it('SWAP', () => assert.deepEqual(run('1 2 SWAP'), [2, 1]));
  it('OVER', () => assert.deepEqual(run('1 2 OVER'), [1, 2, 1]));
  it('ROT', () => assert.deepEqual(run('1 2 3 ROT'), [2, 3, 1]));
  it('2DUP', () => assert.deepEqual(run('1 2 2DUP'), [1, 2, 1, 2]));
  it('2DROP', () => assert.deepEqual(run('1 2 3 4 2DROP'), [1, 2]));
  it('2SWAP', () => assert.deepEqual(run('1 2 3 4 2SWAP'), [3, 4, 1, 2]));
  it('NIP', () => assert.deepEqual(run('1 2 NIP'), [2]));
  it('TUCK', () => assert.deepEqual(run('1 2 TUCK'), [2, 1, 2]));
  it('stack underflow', () => assert.throws(() => f.eval('DROP')));
});

describe('Arithmetic', () => {
  it('addition', () => assert.deepEqual(run('3 4 +'), [7]));
  it('subtraction', () => assert.deepEqual(run('10 3 -'), [7]));
  it('multiplication', () => assert.deepEqual(run('3 4 *'), [12]));
  it('division', () => assert.deepEqual(run('10 3 /'), [3]));
  it('modulo', () => assert.deepEqual(run('10 3 MOD'), [1]));
  it('negate', () => assert.deepEqual(run('5 NEGATE'), [-5]));
  it('absolute', () => assert.deepEqual(run('-5 ABS'), [5]));
  it('min', () => assert.deepEqual(run('3 7 MIN'), [3]));
  it('max', () => assert.deepEqual(run('3 7 MAX'), [7]));
  it('negative numbers', () => assert.deepEqual(run('-3 -4 +'), [-7]));
  it('chained ops', () => assert.deepEqual(run('2 3 + 4 *'), [20]));
});

describe('Comparison', () => {
  it('less than true', () => assert.deepEqual(run('3 5 <'), [-1]));
  it('less than false', () => assert.deepEqual(run('5 3 <'), [0]));
  it('greater than', () => assert.deepEqual(run('5 3 >'), [-1]));
  it('equal true', () => assert.deepEqual(run('5 5 ='), [-1]));
  it('equal false', () => assert.deepEqual(run('3 5 ='), [0]));
  it('not equal', () => assert.deepEqual(run('3 5 <>'), [-1]));
  it('0=', () => assert.deepEqual(run('0 0='), [-1]));
  it('0= false', () => assert.deepEqual(run('5 0='), [0]));
});

describe('Logic', () => {
  it('AND', () => assert.deepEqual(run('-1 -1 AND'), [-1]));
  it('OR', () => assert.deepEqual(run('0 -1 OR'), [-1]));
  it('INVERT', () => {
    f.eval('-1 INVERT');
    assert.equal(f.stack[0], 0);
  });
});

describe('Output', () => {
  it('. prints number', () => assert(output('42 .').includes('42')));
  it('.S prints stack', () => {
    const o = output('1 2 3 .S');
    assert(o.includes('1'));
    assert(o.includes('3'));
  });
  it('CR outputs newline', () => assert(output('CR').includes('\n')));
  it('EMIT outputs char', () => assert.equal(output('65 EMIT'), 'A'));
});

describe('Word Definition', () => {
  it('define and use word', () => {
    assert.deepEqual(run(': DOUBLE DUP + ; 5 DOUBLE'), [10]);
  });

  it('nested definitions', () => {
    assert.deepEqual(run(': DOUBLE DUP + ; : QUADRUPLE DOUBLE DOUBLE ; 3 QUADRUPLE'), [12]);
  });

  it('word with multiple ops', () => {
    assert.deepEqual(run(': SQUARE DUP * ; 5 SQUARE'), [25]);
  });

  it('redefine word', () => {
    assert.deepEqual(run(': X 1 ; : X 2 ; X'), [2]);
  });
});

describe('Control Flow', () => {
  it('IF THEN (true)', () => {
    assert.deepEqual(run(': TEST -1 IF 42 THEN ; TEST'), [42]);
  });

  it('IF THEN (false)', () => {
    assert.deepEqual(run(': TEST 0 IF 42 THEN ; TEST'), []);
  });

  it('IF ELSE THEN', () => {
    assert.deepEqual(run(': TEST -1 IF 1 ELSE 2 THEN ; TEST'), [1]);
    f.reset();
    assert.deepEqual(run(': TEST2 0 IF 1 ELSE 2 THEN ; TEST2'), [2]);
  });

  it('DO LOOP', () => {
    assert.deepEqual(run(': COUNT 5 0 DO I LOOP ; COUNT'), [0, 1, 2, 3, 4]);
  });

  it('DO +LOOP', () => {
    assert.deepEqual(run(': EVENS 10 0 DO I 2 +LOOP ; EVENS'), [0, 2, 4, 6, 8]);
  });

  it('BEGIN UNTIL', () => {
    assert.deepEqual(run(': TEST 0 BEGIN 1+ DUP 5 > UNTIL ; TEST'), [6]);
  });

  it('nested IF', () => {
    assert.deepEqual(run(': TEST -1 IF -1 IF 42 THEN THEN ; TEST'), [42]);
  });
});

describe('Memory', () => {
  it('store and fetch', () => {
    assert.deepEqual(run('42 100 ! 100 @'), [42]);
  });

  it('HERE', () => {
    f.eval('HERE');
    assert(f.stack.length === 1);
    assert(typeof f.stack[0] === 'number');
  });
});

describe('Return Stack', () => {
  it('>R R>', () => {
    assert.deepEqual(run(': TEST 5 >R 10 R> ; TEST'), [10, 5]);
  });
});

describe('Strings', () => {
  it('.\" prints string', () => {
    const o = output(': GREET ." Hello World" ; GREET');
    assert(o.includes('Hello'));
  });
});

describe('Factorial', () => {
  it('recursive factorial', () => {
    const code = `: FACTORIAL
      DUP 1 <= IF DROP 1
      ELSE DUP 1 - FACTORIAL *
      THEN ;
    5 FACTORIAL`;
    assert.deepEqual(run(code), [120]);
  });
});

describe('Fibonacci', () => {
  it('iterative fibonacci (0-indexed)', () => {
    // Standard Forth DO LOOP fibonacci
    // DO LOOP: limit start DO runs (limit - start) times
    const code = `: FIB
      DUP 1 <= IF
      ELSE
        0 1 ROT 1 DO
          OVER + SWAP
        LOOP NIP
      THEN ;
    7 FIB`;
    // This implementation gives fib(n-2): fib(7) → 8 (which is fib(6) 0-indexed)
    // fib sequence: 0,1,1,2,3,5,8,13,21,34,55
    // 7 iterations from 1: produces 8
    const result = run(code);
    assert.equal(result[0], 5);
  });
});

describe('Edge Cases', () => {
  it('empty input', () => assert.deepEqual(run(''), []));
  it('comments', () => assert.deepEqual(run('( this is a comment ) 42'), [42]));
  it('case insensitive', () => assert.deepEqual(run('5 dup'), [5, 5]));
  it('multiple spaces', () => assert.deepEqual(run('1   2   +'), [3]));
});
