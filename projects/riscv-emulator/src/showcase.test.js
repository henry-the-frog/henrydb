// showcase.test.js — Full feature showcase for Monkey → RISC-V compilation
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RiscVCodeGen } from './monkey-codegen.js';
import { inferTypes } from './type-infer.js';
import { peepholeOptimize } from './riscv-peephole.js';
import { Assembler } from './assembler.js';
import { CPU } from './cpu.js';
import { Lexer } from '/Users/henry/repos/monkey-lang/src/lexer.js';
import { Parser } from '/Users/henry/repos/monkey-lang/src/parser.js';

function runFull(input, { useRegisters = false, optimize = false } = {}) {
  const p = new Parser(new Lexer(input));
  const prog = p.parseProgram();
  if (p.errors.length > 0) throw new Error(p.errors.join('\n'));
  const typeInfo = inferTypes(prog);
  const cg = new RiscVCodeGen({ useRegisters });
  let asm = cg.compile(prog, typeInfo);
  if (optimize) asm = peepholeOptimize(asm).optimized;
  const assembler = new Assembler();
  const result = assembler.assemble(asm);
  if (result.errors.length > 0) throw new Error(`Asm: ${result.errors.map(e=>e.message).join(', ')}\n${asm}`);
  const cpu = new CPU();
  cpu.loadProgram(result.words);
  cpu.regs.set(2, 0x100000 - 4);
  cpu.run(2000000);
  return { output: cpu.output.join(''), cycles: cpu.cycles, words: result.words.length };
}

describe('Showcase: Feature-Complete Demo', () => {
  it('FizzBuzz 1..20', () => {
    const { output } = runFull(`
      let fizzbuzz = fn(n) {
        if (n % 15 == 0) { puts("FizzBuzz") }
        if (n % 15 != 0) {
          if (n % 3 == 0) { puts("Fizz") }
          if (n % 3 != 0) {
            if (n % 5 == 0) { puts("Buzz") }
            if (n % 5 != 0) { puts(n) }
          }
        }
      }
      let i = 1
      while (i <= 20) {
        fizzbuzz(i)
        set i = i + 1
      }
    `);
    assert.equal(output, '12Fizz4BuzzFizz78FizzBuzz11Fizz1314FizzBuzz1617Fizz19Buzz');
  });

  it('Sieve of Eratosthenes', () => {
    const { output } = runFull(`
      let is_prime = fn(n) {
        if (n < 2) { return 0 }
        let i = 2
        while (i * i <= n) {
          if (n % i == 0) { return 0 }
          set i = i + 1
        }
        return 1
      }
      let primes = []
      let n = 2
      while (n <= 50) {
        if (is_prime(n) == 1) {
          set primes = push(primes, n)
        }
        set n = n + 1
      }
      puts("Primes: ")
      for (p in primes) {
        puts(p)
        puts(" ")
      }
    `);
    assert.ok(output.includes('Primes: '));
    assert.ok(output.includes('2 '));
    assert.ok(output.includes('47 '));
  });

  it('String builder pattern', () => {
    const { output } = runFull(`
      let repeat = fn(s, n) {
        let result = ""
        let i = 0
        while (i < n) {
          set result = result + s
          set i = i + 1
        }
        return result
      }
      puts(repeat("ha", 3))
    `);
    assert.equal(output, 'hahaha');
  });

  it('Tower of Hanoi counter', () => {
    const { output } = runFull(`
      let hanoi_count = fn(n) {
        if (n == 0) { return 0 }
        return 1 + hanoi_count(n - 1) * 2
      }
      puts("Moves for 1: "); puts(hanoi_count(1))
      puts(" Moves for 5: "); puts(hanoi_count(5))
      puts(" Moves for 10: "); puts(hanoi_count(10))
    `);
    assert.ok(output.includes('Moves for 1: 1'));
    assert.ok(output.includes('Moves for 5: 31'));
    assert.ok(output.includes('Moves for 10: 1023'));
  });

  it('Binary search', () => {
    const { output } = runFull(`
      let binary_search = fn(arr, target, lo, hi) {
        if (lo > hi) { return -1 }
        let mid = (lo + hi) / 2
        if (arr[mid] == target) { return mid }
        if (arr[mid] < target) {
          return binary_search(arr, target, mid + 1, hi)
        }
        return binary_search(arr, target, lo, mid - 1)
      }
      let data = [2, 5, 8, 12, 16, 23, 38, 56, 72, 91]
      puts(binary_search(data, 23, 0, 9))
    `);
    assert.equal(output, '5');
  });

  it('Matrix-like computation', () => {
    const { output } = runFull(`
      let dot = fn(a, b, n) {
        let sum = 0
        let i = 0
        while (i < n) {
          set sum = sum + a[i] * b[i]
          set i = i + 1
        }
        return sum
      }
      let v1 = [1, 0, 0]
      let v2 = [0, 1, 0]
      let v3 = [1, 1, 1]
      puts("v1·v2="); puts(dot(v1, v2, 3))
      puts(" v1·v3="); puts(dot(v1, v3, 3))
      puts(" v3·v3="); puts(dot(v3, v3, 3))
    `);
    assert.ok(output.includes('v1·v2=0'));
    assert.ok(output.includes('v1·v3=1'));
    assert.ok(output.includes('v3·v3=3'));
  });

  it('Complete pipeline: all features with optimization', () => {
    const code = `
      let greet = fn(name) {
        puts("Hello, " + name + "!")
      }
      let fib = fn(n) {
        if (n <= 1) { return n }
        return fib(n - 1) + fib(n - 2)
      }
      let names = ["Alice", "Bob", "Charlie"]
      for (name in names) {
        greet(name)
      }
      puts(" fib(8)=")
      puts(fib(8))
      let squares = []
      let i = 0
      while (i < 5) {
        set squares = push(squares, i * i)
        set i = i + 1
      }
      puts(" squares=")
      for (s in squares) {
        puts(s)
        puts(" ")
      }
    `;

    // Run without optimization
    const base = runFull(code);
    assert.ok(base.output.includes('Hello, Alice!'));
    assert.ok(base.output.includes('Hello, Bob!'));
    assert.ok(base.output.includes('Hello, Charlie!'));
    assert.ok(base.output.includes('fib(8)=21'));
    assert.ok(base.output.includes('squares='));
    assert.ok(base.output.includes('0 1 4 9 16'));

    // Run with register allocation + peephole
    const opt = runFull(code, { useRegisters: true, optimize: true });
    assert.ok(opt.output.includes('Hello, Alice!'));
    assert.ok(opt.output.includes('fib(8)=21'));

    console.log(`  Showcase: ${base.words} instructions, ${base.cycles} cycles (base)`);
    console.log(`  Showcase: ${opt.words} instructions, ${opt.cycles} cycles (optimized)`);
    console.log(`  Savings: ${base.cycles - opt.cycles} cycles (${((base.cycles - opt.cycles)/base.cycles*100).toFixed(1)}%)`);
  });
});
