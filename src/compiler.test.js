// compiler.test.js — Tests for the tiny language compiler
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { run, tokenize, Parser } from './compiler.js';

describe('Compiler', () => {
  it('arithmetic expression', () => {
    const { result } = run('let x = 3 + 4 * 2;');
    // x = 3 + 4 * 2 = 3 + 8 = 11
    assert.equal(result, null); // result is null (no expression at end)
  });

  it('print output', () => {
    const { output } = run('print(42);');
    assert.deepEqual(output, [42]);
  });

  it('variables', () => {
    const { output } = run(`
      let x = 10;
      let y = 20;
      print(x + y);
    `);
    assert.deepEqual(output, [30]);
  });

  it('assignment', () => {
    const { output } = run(`
      let x = 5;
      x = x * 2;
      print(x);
    `);
    assert.deepEqual(output, [10]);
  });

  it('if/else', () => {
    const { output } = run(`
      let x = 10;
      if (x > 5) {
        print(1);
      } else {
        print(0);
      }
    `);
    assert.deepEqual(output, [1]);
  });

  it('while loop', () => {
    const { output } = run(`
      let sum = 0;
      let i = 1;
      while (i <= 10) {
        sum = sum + i;
        i = i + 1;
      }
      print(sum);
    `);
    assert.deepEqual(output, [55]);
  });

  it('function definition and call', () => {
    const { output } = run(`
      fn double(x) {
        return x * 2;
      }
      print(double(21));
    `);
    assert.deepEqual(output, [42]);
  });

  it('recursive factorial', () => {
    const { output } = run(`
      fn fact(n) {
        if (n <= 1) {
          return 1;
        }
        return n * fact(n - 1);
      }
      print(fact(5));
    `);
    assert.deepEqual(output, [120]);
  });

  it('recursive fibonacci', () => {
    const { output } = run(`
      fn fib(n) {
        if (n < 2) {
          return n;
        }
        return fib(n - 1) + fib(n - 2);
      }
      print(fib(10));
    `);
    assert.deepEqual(output, [55]);
  });

  it('nested function calls', () => {
    const { output } = run(`
      fn add(a, b) {
        return a + b;
      }
      fn mul(a, b) {
        return a * b;
      }
      print(add(mul(3, 4), mul(5, 6)));
    `);
    assert.deepEqual(output, [42]);
  });

  it('comparison operators', () => {
    const { output } = run(`
      print(5 == 5);
      print(5 != 3);
      print(3 < 5);
      print(5 > 3);
      print(3 <= 3);
      print(5 >= 6);
    `);
    assert.deepEqual(output, [1, 1, 1, 1, 1, 0]);
  });

  it('boolean values', () => {
    const { output } = run(`
      let x = true;
      let y = false;
      if (x) { print(1); }
      if (y) { print(2); }
    `);
    assert.deepEqual(output, [1]);
  });

  it('modulo operator', () => {
    const { output } = run(`print(17 % 5);`);
    assert.deepEqual(output, [2]);
  });

  it('unary minus', () => {
    const { output } = run(`print(-42);`);
    assert.deepEqual(output, [-42]);
  });

  it('FizzBuzz', () => {
    const { output } = run(`
      let i = 1;
      while (i <= 15) {
        if (i % 15 == 0) {
          print(0);
        } else {
          if (i % 3 == 0) {
            print(3);
          } else {
            if (i % 5 == 0) {
              print(5);
            } else {
              print(i);
            }
          }
        }
        i = i + 1;
      }
    `);
    // 1,2,fizz(3),4,buzz(5),fizz(3),7,8,fizz(3),buzz(5),11,fizz(3),13,14,fizzbuzz(0)
    assert.deepEqual(output, [1, 2, 3, 4, 5, 3, 7, 8, 3, 5, 11, 3, 13, 14, 0]);
  });

  it('error: undefined variable', () => {
    assert.throws(() => run('print(x);'), /Undefined/);
  });

  it('error: undefined function', () => {
    assert.throws(() => run('print(foo(1));'), /Undefined/);
  });
});
