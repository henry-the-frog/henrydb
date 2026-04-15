// closures.test.js — Closure compilation tests for RISC-V codegen
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RiscVCodeGen } from './monkey-codegen.js';
import { inferTypes } from './type-infer.js';
import { analyzeFreeVars } from './closure-analysis.js';
import { Assembler } from './assembler.js';
import { CPU } from './cpu.js';
import { Lexer } from '/Users/henry/repos/monkey-lang/src/lexer.js';
import { Parser } from '/Users/henry/repos/monkey-lang/src/parser.js';

function run(input) {
  const p = new Parser(new Lexer(input));
  const prog = p.parseProgram();
  if (p.errors.length > 0) throw new Error(p.errors.join('\n'));
  const typeInfo = inferTypes(prog);
  const closureInfo = analyzeFreeVars(prog);
  const cg = new RiscVCodeGen();
  const asm = cg.compile(prog, typeInfo, closureInfo);
  if (cg.errors.length > 0) throw new Error(`Codegen: ${cg.errors.join(', ')}`);
  const result = new Assembler().assemble(asm);
  if (result.errors.length > 0) throw new Error(`Asm: ${result.errors.map(e=>e.message).join(', ')}\n${asm}`);
  const cpu = new CPU();
  cpu.loadProgram(result.words);
  cpu.regs.set(2, 0x100000 - 4);
  cpu.run(500000);
  return cpu.output.join('');
}

describe('Closures — basic capture', () => {
  it('captures outer variable', () => {
    assert.equal(run('let x = 10; let add_x = fn(y) { x + y }; puts(add_x(5))'), '15');
  });

  it('captures multiple outer variables', () => {
    assert.equal(run('let a = 3; let b = 4; let f = fn(x) { a + b + x }; puts(f(10))'), '17');
  });

  it('captures with different operations', () => {
    assert.equal(run('let factor = 7; let mul = fn(x) { factor * x }; puts(mul(6))'), '42');
  });

  it('closure does not interfere with regular functions', () => {
    assert.equal(run(`
      let regular = fn(x) { x * 2 }
      let y = 10
      let closure = fn(x) { y + x }
      puts(regular(5))
      puts(closure(3))
    `), '1013');
  });

  it('closure called multiple times', () => {
    assert.equal(run(`
      let offset = 100
      let add = fn(x) { offset + x }
      puts(add(1))
      puts(add(2))
      puts(add(3))
    `), '101102103');
  });

  it('captures string variable', () => {
    assert.equal(run(`
      let greeting = "Hello, "
      let greet = fn(name) { puts(greeting + name) }
      greet("World")
    `), 'Hello, World');
  });

  it('captures array variable', () => {
    assert.equal(run(`
      let data = [10, 20, 30]
      let get = fn(i) { data[i] }
      puts(get(0))
      puts(get(1))
      puts(get(2))
    `), '102030');
  });
});

describe('Closures — non-closure functions still work', () => {
  it('regular function', () => {
    assert.equal(run('let f = fn(x) { x * 2 }; puts(f(21))'), '42');
  });

  it('recursive function', () => {
    assert.equal(run('let fib = fn(n) { if (n <= 1) { return n }; return fib(n-1) + fib(n-2) }; puts(fib(10))'), '55');
  });

  it('multiple regular functions', () => {
    assert.equal(run(`
      let add = fn(a, b) { return a + b }
      let mul = fn(a, b) { return a * b }
      puts(add(3, 4))
      puts(mul(5, 6))
    `), '730');
  });
});

describe('Closures — edge cases', () => {
  it('closure with no parameters', () => {
    assert.equal(run('let x = 42; let get_x = fn() { x }; puts(get_x())'), '42');
  });

  it('closure captures boolean value', () => {
    assert.equal(run('let flag = true; let check = fn() { flag }; puts(check())'), '1');
  });

  it('closure with if/else using captured var', () => {
    assert.equal(run(`
      let threshold = 10
      let classify = fn(x) {
        if (x > threshold) { return 1 }
        return 0
      }
      puts(classify(15))
      puts(classify(5))
    `), '10');
  });

  it('closure in loop', () => {
    assert.equal(run(`
      let multiplier = 3
      let mul = fn(x) { multiplier * x }
      let i = 1
      while (i <= 5) {
        puts(mul(i))
        set i = i + 1
      }
    `), '3691215');
  });
});
