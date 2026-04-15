// monkey-codegen.test.js — Tests for Monkey → RISC-V code generation
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RiscVCodeGen } from './monkey-codegen.js';
import { Assembler } from './assembler.js';
import { CPU } from './cpu.js';

// We need the monkey-lang parser
import { Lexer } from '/Users/henry/repos/monkey-lang/src/lexer.js';
import { Parser } from '/Users/henry/repos/monkey-lang/src/parser.js';

function parse(input) {
  const lexer = new Lexer(input);
  const parser = new Parser(lexer);
  const program = parser.parseProgram();
  if (parser.errors.length > 0) {
    throw new Error(`Parse errors: ${parser.errors.join('\n')}`);
  }
  return program;
}

function compileToAsm(input) {
  const program = parse(input);
  const codegen = new RiscVCodeGen();
  return codegen.compile(program);
}

function compileAndRun(input, maxCycles = 50000) {
  const asm = compileToAsm(input);
  const assembler = new Assembler();
  const result = assembler.assemble(asm);
  if (result.errors && result.errors.length > 0) {
    throw new Error(`Assembly errors: ${result.errors.map(e => e.message || e).join('\n')}\n\nAssembly:\n${asm}`);
  }
  const cpu = new CPU();
  cpu.loadProgram(result.words);
  // Set up stack pointer
  cpu.regs.set(2, 0x100000 - 4); // sp = near top of memory
  cpu.run(maxCycles);
  return cpu;
}

function getOutput(input) {
  const cpu = compileAndRun(input);
  return cpu.output.join('');
}

function getExitCode(input) {
  const cpu = compileAndRun(input);
  return cpu.exitCode;
}

describe('Monkey → RISC-V Code Generation', () => {
  describe('integer arithmetic', () => {
    it('compiles integer literal', () => {
      const asm = compileToAsm('42');
      assert.ok(asm.includes('li a0, 42'));
    });

    it('compiles addition', () => {
      const asm = compileToAsm('3 + 4');
      assert.ok(asm.includes('add'));
    });

    it('prints integer via puts', () => {
      const output = getOutput('puts(42)');
      assert.equal(output, '42');
    });

    it('prints addition result', () => {
      const output = getOutput('puts(3 + 4)');
      assert.equal(output, '7');
    });

    it('prints subtraction result', () => {
      const output = getOutput('puts(10 - 3)');
      assert.equal(output, '7');
    });

    it('prints multiplication result', () => {
      const output = getOutput('puts(6 * 7)');
      assert.equal(output, '42');
    });

    it('prints division result', () => {
      const output = getOutput('puts(42 / 6)');
      assert.equal(output, '7');
    });

    it('prints modulo result', () => {
      const output = getOutput('puts(17 % 5)');
      assert.equal(output, '2');
    });

    it('compound arithmetic', () => {
      const output = getOutput('puts(2 + 3 * 4)');
      // Parser handles precedence: 2 + (3 * 4) = 14
      assert.equal(output, '14');
    });

    it('nested arithmetic', () => {
      const output = getOutput('puts((10 - 3) * 2 + 1)');
      assert.equal(output, '15');
    });

    it('negative literal', () => {
      const output = getOutput('puts(-5)');
      assert.equal(output, '-5');
    });

    it('double negation', () => {
      const output = getOutput('puts(-(-7))');
      assert.equal(output, '7');
    });
  });

  describe('let bindings', () => {
    it('let and use', () => {
      const output = getOutput('let x = 10; puts(x)');
      assert.equal(output, '10');
    });

    it('multiple lets', () => {
      const output = getOutput('let x = 3; let y = 4; puts(x + y)');
      assert.equal(output, '7');
    });

    it('let with expression', () => {
      const output = getOutput('let x = 2 + 3; let y = x * 2; puts(y)');
      assert.equal(output, '10');
    });

    it('set mutation', () => {
      const output = getOutput('let x = 1; set x = x + 1; puts(x)');
      assert.equal(output, '2');
    });
  });

  describe('boolean and comparison', () => {
    it('true is 1', () => {
      const output = getOutput('puts(true)');
      assert.equal(output, '1');
    });

    it('false is 0', () => {
      const output = getOutput('puts(false)');
      assert.equal(output, '0');
    });

    it('less than (true)', () => {
      const output = getOutput('puts(3 < 5)');
      assert.equal(output, '1');
    });

    it('less than (false)', () => {
      const output = getOutput('puts(5 < 3)');
      assert.equal(output, '0');
    });

    it('greater than', () => {
      const output = getOutput('puts(5 > 3)');
      assert.equal(output, '1');
    });

    it('equals (true)', () => {
      const output = getOutput('puts(5 == 5)');
      assert.equal(output, '1');
    });

    it('equals (false)', () => {
      const output = getOutput('puts(5 == 3)');
      assert.equal(output, '0');
    });

    it('not equals', () => {
      const output = getOutput('puts(5 != 3)');
      assert.equal(output, '1');
    });

    it('logical not', () => {
      const output = getOutput('puts(!false)');
      assert.equal(output, '1');
    });

    it('not of truthy', () => {
      const output = getOutput('puts(!5)');
      assert.equal(output, '0');
    });
  });

  describe('if/else', () => {
    it('if true branch', () => {
      const output = getOutput('if (true) { puts(1) }');
      assert.equal(output, '1');
    });

    it('if false — no output', () => {
      const output = getOutput('if (false) { puts(1) }');
      assert.equal(output, '');
    });

    it('if/else — true branch', () => {
      const output = getOutput('if (1 < 2) { puts(10) } else { puts(20) }');
      assert.equal(output, '10');
    });

    it('if/else — false branch', () => {
      const output = getOutput('if (1 > 2) { puts(10) } else { puts(20) }');
      assert.equal(output, '20');
    });

    it('if with computed condition', () => {
      const output = getOutput('let x = 5; if (x > 3) { puts(x) }');
      assert.equal(output, '5');
    });

    it('nested if', () => {
      const output = getOutput(`
        let x = 10
        if (x > 5) {
          if (x > 8) {
            puts(1)
          } else {
            puts(2)
          }
        }
      `);
      assert.equal(output, '1');
    });
  });

  describe('while loops', () => {
    it('simple while loop', () => {
      const output = getOutput(`
        let i = 0
        while (i < 5) {
          puts(i)
          set i = i + 1
        }
      `);
      assert.equal(output, '01234');
    });

    it('sum loop', () => {
      const output = getOutput(`
        let sum = 0
        let i = 1
        while (i <= 10) {
          set sum = sum + i
          set i = i + 1
        }
        puts(sum)
      `);
      assert.equal(output, '55');
    });
  });

  describe('functions', () => {
    it('simple function call', () => {
      const output = getOutput(`
        let double = fn(x) { return x * 2 }
        puts(double(21))
      `);
      assert.equal(output, '42');
    });

    it('function with two args', () => {
      const output = getOutput(`
        let add = fn(a, b) { return a + b }
        puts(add(3, 4))
      `);
      assert.equal(output, '7');
    });

    it('recursive fibonacci', () => {
      const output = getOutput(`
        let fib = fn(n) {
          if (n <= 1) { return n }
          return fib(n - 1) + fib(n - 2)
        }
        puts(fib(10))
      `);
      assert.equal(output, '55');
    });

    it('factorial', () => {
      const output = getOutput(`
        let fact = fn(n) {
          if (n <= 1) { return 1 }
          return n * fact(n - 1)
        }
        puts(fact(5))
      `);
      assert.equal(output, '120');
    });

    it('function with no return (implicit)', () => {
      const output = getOutput(`
        let greet = fn(x) { puts(x) }
        greet(42)
      `);
      assert.equal(output, '42');
    });
  });

  describe('assembly output', () => {
    it('produces valid assembly', () => {
      const asm = compileToAsm('let x = 1; let y = 2; puts(x + y)');
      assert.ok(asm.includes('_start'));
      assert.ok(asm.includes('ecall'));
    });

    it('includes comments', () => {
      const asm = compileToAsm('let x = 5');
      assert.ok(asm.includes('# let x'));
    });
  });
});
