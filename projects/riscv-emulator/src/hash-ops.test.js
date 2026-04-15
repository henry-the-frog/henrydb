// hash-ops.test.js — Hash literal + access tests for RISC-V codegen
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

describe('Hash literals', () => {
  it('create and access', () => {
    assert.equal(run('let h = {1: 10, 2: 20, 3: 30}; puts(h[2])'), '20');
  });

  it('single pair', () => {
    assert.equal(run('let h = {42: 100}; puts(h[42])'), '100');
  });

  it('access first element', () => {
    assert.equal(run('let h = {1: 10, 2: 20}; puts(h[1])'), '10');
  });

  it('access last element', () => {
    assert.equal(run('let h = {1: 10, 2: 20, 3: 30}; puts(h[3])'), '30');
  });

  it('computed values', () => {
    assert.equal(run('let x = 5; let h = {1: x, 2: x * 2}; puts(h[1]); puts(h[2])'), '510');
  });

  it('hash in variable', () => {
    assert.equal(run('let config = {1: 100, 2: 200}; puts(config[1] + config[2])'), '300');
  });
});

describe('Hash with functions', () => {
  it('hash as function argument', () => {
    assert.equal(run(`
      let get = fn(h, k) { h[k] }
      let data = {10: 100, 20: 200}
      puts(get(data, 10))
    `), '100');
  });

  it('function returning hash', () => {
    assert.equal(run(`
      let make_pair = fn(a, b) { return {1: a, 2: b} }
      let p = make_pair(10, 20)
      puts(p[1])
      puts(p[2])
    `), '1020');
  });
});

describe('Hash with boolean keys', () => {
  it('true/false keys', () => {
    assert.equal(run('let h = {1: 10, 0: 20}; puts(h[1]); puts(h[0])'), '1020');
  });
});

describe('Hash — missing key', () => {
  it('returns 0 for missing key', () => {
    assert.equal(run('let h = {1: 10}; puts(h[99])'), '0');
  });
});

describe('Hash — complex programs', () => {
  it('frequency counter', () => {
    assert.equal(run(`
      let data = [1, 2, 3, 1, 2, 1]
      let count_1 = 0
      let count_2 = 0
      let count_3 = 0
      for (x in data) {
        if (x == 1) { set count_1 = count_1 + 1 }
        if (x == 2) { set count_2 = count_2 + 1 }
        if (x == 3) { set count_3 = count_3 + 1 }
      }
      puts(count_1)
      puts(count_2)
      puts(count_3)
    `), '321');
  });

  it('lookup table', () => {
    assert.equal(run(`
      let squares = {1: 1, 2: 4, 3: 9, 4: 16, 5: 25}
      let sum = 0
      let i = 1
      while (i <= 5) {
        set sum = sum + squares[i]
        set i = i + 1
      }
      puts(sum)
    `), '55');
  });
});
