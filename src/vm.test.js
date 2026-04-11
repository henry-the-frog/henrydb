// vm.test.js — Tests for bytecode VM
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { VM, OP, assemble } from './vm.js';

describe('VM', () => {
  it('push and halt', () => {
    const vm = new VM();
    const result = vm.execute([OP.PUSH, 42, OP.HALT]);
    assert.equal(result, 42);
  });

  it('arithmetic: 3 + 4', () => {
    const vm = new VM();
    const result = vm.execute([OP.PUSH, 3, OP.PUSH, 4, OP.ADD, OP.HALT]);
    assert.equal(result, 7);
  });

  it('arithmetic: (10 - 3) * 2', () => {
    const vm = new VM();
    const result = vm.execute([
      OP.PUSH, 10, OP.PUSH, 3, OP.SUB,
      OP.PUSH, 2, OP.MUL,
      OP.HALT
    ]);
    assert.equal(result, 14);
  });

  it('division and modulo', () => {
    const vm = new VM();
    assert.equal(vm.execute([OP.PUSH, 17, OP.PUSH, 5, OP.DIV, OP.HALT]), 3);
    assert.equal(vm.execute([OP.PUSH, 17, OP.PUSH, 5, OP.MOD, OP.HALT]), 2);
  });

  it('comparison operators', () => {
    const vm = new VM();
    assert.equal(vm.execute([OP.PUSH, 5, OP.PUSH, 5, OP.EQ, OP.HALT]), 1);
    assert.equal(vm.execute([OP.PUSH, 5, OP.PUSH, 3, OP.EQ, OP.HALT]), 0);
    assert.equal(vm.execute([OP.PUSH, 3, OP.PUSH, 5, OP.LT, OP.HALT]), 1);
    assert.equal(vm.execute([OP.PUSH, 5, OP.PUSH, 3, OP.GT, OP.HALT]), 1);
  });

  it('conditional jump: max(a, b)', () => {
    const vm = new VM();
    // if a > b then a else b
    const code = [
      OP.PUSH, 7,    // a = 7
      OP.PUSH, 3,    // b = 3
      // Stack: [7, 3]. Compare: dup both, compare
      OP.DUP,        // [7, 3, 3]
      OP.PUSH, 7,    // [7, 3, 3, 7]
      OP.SWAP,       // [7, 3, 7, 3]
      OP.GT,         // [7, 3, 1] (7 > 3 = true)
      OP.JZ, 16,     // if false, jump to take b
      OP.POP,        // discard b, keep a
      OP.HALT,       // return a
      // addr 16:
      OP.SWAP,       // swap a and b
      OP.POP,        // discard a, keep b
      OP.HALT,       // return b
    ];
    assert.equal(vm.execute(code), 7);
  });

  it('local variables', () => {
    const vm = new VM();
    const code = [
      OP.PUSH, 42,
      OP.STORE, 0,   // x = 42
      OP.PUSH, 10,
      OP.STORE, 1,   // y = 10
      OP.LOAD, 0,    // push x
      OP.LOAD, 1,    // push y
      OP.ADD,         // x + y
      OP.HALT,
    ];
    assert.equal(vm.execute(code), 52);
  });

  it('loop: sum 1 to 10', () => {
    const vm = new VM();
    // sum = 0; i = 1; while (i <= 10) { sum += i; i++; }
    const code = [
      OP.PUSH, 0,    // 0: sum = 0
      OP.STORE, 0,   // 2:
      OP.PUSH, 1,    // 4: i = 1
      OP.STORE, 1,   // 6:
      // loop start (addr 8):
      OP.LOAD, 1,    // 8: push i
      OP.PUSH, 10,   // 10:
      OP.GT,         // 12: i > 10?
      OP.JNZ, 28,    // 13: if yes, exit loop (addr 28)
      OP.LOAD, 0,    // 15: push sum
      OP.LOAD, 1,    // 17: push i
      OP.ADD,         // 19: sum + i
      OP.STORE, 0,   // 20: sum = sum + i
      OP.LOAD, 1,    // 22: push i
      OP.PUSH, 1,    // 24:
      OP.ADD,         // 26: i + 1
      OP.STORE, 1,   // 27: i = i + 1  (wait, this should be addr 27)
      // Actually let me recalculate addresses...
    ];
    // Let me use the assembler instead
    const asm = assemble(`
      PUSH 0
      STORE 0
      PUSH 1
      STORE 1
    loop:
      LOAD 1
      PUSH 10
      GT
      JNZ done
      LOAD 0
      LOAD 1
      ADD
      STORE 0
      LOAD 1
      PUSH 1
      ADD
      STORE 1
      JMP loop
    done:
      LOAD 0
      HALT
    `);
    assert.equal(vm.execute(asm), 55);
  });

  it('function call: double(x)', () => {
    const vm = new VM();
    const code = assemble(`
      PUSH 21
      CALL double
      HALT
    double:
      DUP
      ADD
      RET
    `);
    assert.equal(vm.execute(code), 42);
  });

  it('recursive factorial', () => {
    const vm = new VM();
    const code = assemble(`
      PUSH 5
      CALL fact
      HALT
    fact:
      DUP
      PUSH 1
      LTE
      JNZ base
      DUP
      PUSH 1
      SUB
      CALL fact
      MUL
      RET
    base:
      RET
    `);
    assert.equal(vm.execute(code), 120);
  });

  it('fibonacci(10)', () => {
    const vm = new VM();
    const code = assemble(`
      PUSH 10
      CALL fib
      HALT
    fib:
      DUP
      PUSH 2
      LT
      JNZ base
      DUP
      PUSH 1
      SUB
      CALL fib
      SWAP
      PUSH 2
      SUB
      CALL fib
      ADD
      RET
    base:
      RET
    `);
    assert.equal(vm.execute(code), 55);
  });

  it('print output', () => {
    const vm = new VM();
    const code = [OP.PUSH, 1, OP.PRINT, OP.PUSH, 2, OP.PRINT, OP.PUSH, 3, OP.PRINT, OP.HALT];
    vm.execute(code);
    assert.deepEqual(vm.output, [1, 2, 3]);
  });

  it('division by zero throws', () => {
    const vm = new VM();
    assert.throws(() => vm.execute([OP.PUSH, 1, OP.PUSH, 0, OP.DIV, OP.HALT]), /Division by zero/);
  });

  it('execution limit prevents infinite loops', () => {
    const vm = new VM({ maxSteps: 100 });
    const code = assemble(`
    loop:
      PUSH 1
      POP
      JMP loop
    `);
    assert.throws(() => vm.execute(code), /Execution limit/);
  });

  it('disassemble', () => {
    const code = [OP.PUSH, 42, OP.PUSH, 10, OP.ADD, OP.HALT];
    const disasm = VM.disassemble(code);
    assert.ok(disasm.includes('PUSH 42'));
    assert.ok(disasm.includes('ADD'));
    assert.ok(disasm.includes('HALT'));
  });
});

describe('Assembler', () => {
  it('basic assembly', () => {
    const code = assemble('PUSH 42\nHALT');
    assert.deepEqual(code, [OP.PUSH, 42, OP.HALT]);
  });

  it('labels resolve to correct addresses', () => {
    const code = assemble(`
      JMP end
      PUSH 99
    end:
      PUSH 42
      HALT
    `);
    const vm = new VM();
    assert.equal(vm.execute(code), 42);
  });

  it('ignores comments', () => {
    const code = assemble('; this is a comment\nPUSH 42\n; another comment\nHALT');
    assert.deepEqual(code, [OP.PUSH, 42, OP.HALT]);
  });
});
