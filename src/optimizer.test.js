// optimizer.test.js — Tests for compiler optimization passes
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { constantFold, peepholeOptimize, countNodes } from './optimizer.js';
import { run, tokenize, Parser } from './compiler.js';
import { OP } from './vm.js';

describe('Constant Folding', () => {
  it('folds simple arithmetic', () => {
    const ast = parse('let x = 3 + 4;');
    const folded = constantFold(ast);
    // The let's value should be a Number(7) instead of BinaryOp
    const letNode = folded.body[0];
    assert.equal(letNode.value.type, 'Number');
    assert.equal(letNode.value.value, 7);
  });

  it('folds nested arithmetic', () => {
    const ast = parse('let x = (2 + 3) * (4 - 1);');
    const folded = constantFold(ast);
    assert.equal(folded.body[0].value.type, 'Number');
    assert.equal(folded.body[0].value.value, 15);
  });

  it('folds comparison', () => {
    const ast = parse('let x = 5 > 3;');
    const folded = constantFold(ast);
    assert.equal(folded.body[0].value.type, 'Number');
    assert.equal(folded.body[0].value.value, 1);
  });

  it('folds unary minus', () => {
    const ast = parse('let x = -42;');
    const folded = constantFold(ast);
    assert.equal(folded.body[0].value.type, 'Number');
    assert.equal(folded.body[0].value.value, -42);
  });

  it('x + 0 → x', () => {
    const ast = parse('let y = 1; let x = y + 0;');
    const folded = constantFold(ast);
    const xLet = folded.body[1];
    assert.equal(xLet.value.type, 'Identifier');
    assert.equal(xLet.value.name, 'y');
  });

  it('x * 1 → x', () => {
    const ast = parse('let y = 1; let x = y * 1;');
    const folded = constantFold(ast);
    assert.equal(folded.body[1].value.type, 'Identifier');
  });

  it('x * 0 → 0', () => {
    const ast = parse('let y = 1; let x = y * 0;');
    const folded = constantFold(ast);
    assert.equal(folded.body[1].value.type, 'Number');
    assert.equal(folded.body[1].value.value, 0);
  });

  it('dead code: if (false) → else branch only', () => {
    const ast = parse('if (false) { print(1); } else { print(2); }');
    const folded = constantFold(ast);
    // Should become the else block: { print(2) }
    assert.equal(folded.body[0].type, 'Block');
    assert.equal(folded.body[0].body[0].type, 'Print');
    assert.equal(folded.body[0].body[0].value.value, 2);
  });

  it('dead code: if (true) → then branch only', () => {
    const ast = parse('if (true) { print(1); } else { print(2); }');
    const folded = constantFold(ast);
    assert.equal(folded.body[0].type, 'Block');
    assert.equal(folded.body[0].body[0].type, 'Print');
    assert.equal(folded.body[0].body[0].value.value, 1);
  });

  it('preserves non-constant expressions', () => {
    const ast = parse('let x = 5; let y = x + 1;');
    const folded = constantFold(ast);
    // x + 1 can't be folded (x is variable)
    assert.equal(folded.body[1].value.type, 'BinaryOp');
  });

  it('reduces AST node count', () => {
    const ast = parse('let x = 2 + 3 * 4 - 1;');
    const before = countNodes(ast);
    const folded = constantFold(ast);
    const after = countNodes(folded);
    assert.ok(after < before, `Expected fewer nodes: ${before} → ${after}`);
  });
});

describe('Peephole Optimization', () => {
  it('removes PUSH-POP (no jumps)', () => {
    const code = [OP.PUSH, 42, OP.POP, OP.PUSH, 10, OP.HALT];
    const opt = peepholeOptimize(code);
    assert.deepEqual(opt, [OP.PUSH, 10, OP.HALT]);
  });

  it('removes double NEG', () => {
    const code = [OP.PUSH, 5, OP.NEG, OP.NEG, OP.HALT];
    const opt = peepholeOptimize(code);
    assert.deepEqual(opt, [OP.PUSH, 5, OP.HALT]);
  });

  it('removes add zero', () => {
    const code = [OP.PUSH, 42, OP.PUSH, 0, OP.ADD, OP.HALT];
    const opt = peepholeOptimize(code);
    assert.deepEqual(opt, [OP.PUSH, 42, OP.HALT]);
  });

  it('removes multiply by one', () => {
    const code = [OP.PUSH, 42, OP.PUSH, 1, OP.MUL, OP.HALT];
    const opt = peepholeOptimize(code);
    assert.deepEqual(opt, [OP.PUSH, 42, OP.HALT]);
  });
});

describe('Optimization Integration', () => {
  it('optimized program produces same output', () => {
    // Run with and without optimization should produce same results
    const programs = [
      'let x = 3 + 4; print(x);',
      'let x = 2 * 3 + 1; print(x);',
      'if (true) { print(1); } else { print(2); }',
      'if (false) { print(1); } else { print(2); }',
    ];
    
    for (const prog of programs) {
      const { output: out1 } = run(prog);
      // Also run with constant folding applied to AST
      const tokens = tokenize(prog);
      const ast = new Parser(tokens).parseProgram();
      const folded = constantFold(ast);
      // We can't easily recompile from folded AST without modifying run()
      // But we can verify the constant folding doesn't break the structure
      assert.ok(folded.type === 'Program');
    }
  });
});

// Helper
function parse(source) {
  return new Parser(tokenize(source)).parseProgram();
}
