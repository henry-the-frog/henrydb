// vm.js — Stack-based bytecode virtual machine
// Instruction set similar to JVM/Python bytecode.

// Opcodes
export const OP = {
  PUSH:    0x01,  // Push constant onto stack
  POP:     0x02,  // Pop and discard top of stack
  DUP:     0x03,  // Duplicate top of stack
  SWAP:    0x04,  // Swap top two stack values
  
  // Arithmetic
  ADD:     0x10,  // a + b
  SUB:     0x11,  // a - b
  MUL:     0x12,  // a * b
  DIV:     0x13,  // a / b (integer division)
  MOD:     0x14,  // a % b
  NEG:     0x15,  // -a
  
  // Comparison (push 1 for true, 0 for false)
  EQ:      0x20,  // a == b
  NEQ:     0x21,  // a != b
  LT:      0x22,  // a < b
  GT:      0x23,  // a > b
  LTE:     0x24,  // a <= b
  GTE:     0x25,  // a >= b
  
  // Logic
  AND:     0x30,
  OR:      0x31,
  NOT:     0x32,
  
  // Control flow
  JMP:     0x40,  // Unconditional jump to address
  JZ:      0x41,  // Jump if top of stack is zero (falsy)
  JNZ:     0x42,  // Jump if top of stack is non-zero (truthy)
  
  // Variables (local slots)
  LOAD:    0x50,  // Load local variable
  STORE:   0x51,  // Store to local variable
  
  // Functions
  CALL:    0x60,  // Call function at address
  RET:     0x61,  // Return from function
  
  // I/O
  PRINT:   0x70,  // Print top of stack
  
  // System
  HALT:    0xFF,  // Stop execution
};

// Reverse lookup for disassembly
const OP_NAMES = {};
for (const [name, code] of Object.entries(OP)) OP_NAMES[code] = name;

/**
 * VM — Stack-based bytecode virtual machine.
 */
export class VM {
  constructor(options = {}) {
    this._stack = [];
    this._callStack = []; // Return addresses + saved frame pointers
    this._locals = [];    // Local variable slots (per frame)
    this._ip = 0;         // Instruction pointer
    this._fp = 0;         // Frame pointer (base of current locals)
    this._output = [];    // Captured print output
    this._maxSteps = options.maxSteps ?? 1000000;
    this._stepCount = 0;
  }

  get stack() { return [...this._stack]; }
  get output() { return [...this._output]; }
  get stepCount() { return this._stepCount; }

  /**
   * Execute a bytecode program.
   * @param {number[]} bytecode — array of opcodes and operands
   * @returns {number|null} — top of stack at HALT, or null
   */
  execute(bytecode) {
    this._ip = 0;
    this._stack = [];
    this._callStack = [];
    this._locals = new Array(256).fill(0);
    this._fp = 0;
    this._output = [];
    this._stepCount = 0;
    
    while (this._ip < bytecode.length) {
      if (++this._stepCount > this._maxSteps) {
        throw new Error(`Execution limit exceeded (${this._maxSteps} steps)`);
      }
      
      const op = bytecode[this._ip++];
      
      switch (op) {
        case OP.PUSH:
          this._stack.push(bytecode[this._ip++]);
          break;
          
        case OP.POP:
          this._stack.pop();
          break;
          
        case OP.DUP:
          this._stack.push(this._stack[this._stack.length - 1]);
          break;
          
        case OP.SWAP: {
          const a = this._stack.pop();
          const b = this._stack.pop();
          this._stack.push(a, b);
          break;
        }
          
        case OP.ADD: {
          const b = this._stack.pop();
          const a = this._stack.pop();
          this._stack.push(a + b);
          break;
        }
        case OP.SUB: {
          const b = this._stack.pop();
          const a = this._stack.pop();
          this._stack.push(a - b);
          break;
        }
        case OP.MUL: {
          const b = this._stack.pop();
          const a = this._stack.pop();
          this._stack.push(a * b);
          break;
        }
        case OP.DIV: {
          const b = this._stack.pop();
          const a = this._stack.pop();
          if (b === 0) throw new Error('Division by zero');
          this._stack.push(Math.trunc(a / b));
          break;
        }
        case OP.MOD: {
          const b = this._stack.pop();
          const a = this._stack.pop();
          this._stack.push(a % b);
          break;
        }
        case OP.NEG:
          this._stack.push(-this._stack.pop());
          break;
          
        case OP.EQ: {
          const b = this._stack.pop(), a = this._stack.pop();
          this._stack.push(a === b ? 1 : 0);
          break;
        }
        case OP.NEQ: {
          const b = this._stack.pop(), a = this._stack.pop();
          this._stack.push(a !== b ? 1 : 0);
          break;
        }
        case OP.LT: {
          const b = this._stack.pop(), a = this._stack.pop();
          this._stack.push(a < b ? 1 : 0);
          break;
        }
        case OP.GT: {
          const b = this._stack.pop(), a = this._stack.pop();
          this._stack.push(a > b ? 1 : 0);
          break;
        }
        case OP.LTE: {
          const b = this._stack.pop(), a = this._stack.pop();
          this._stack.push(a <= b ? 1 : 0);
          break;
        }
        case OP.GTE: {
          const b = this._stack.pop(), a = this._stack.pop();
          this._stack.push(a >= b ? 1 : 0);
          break;
        }
          
        case OP.AND: {
          const b = this._stack.pop(), a = this._stack.pop();
          this._stack.push(a && b ? 1 : 0);
          break;
        }
        case OP.OR: {
          const b = this._stack.pop(), a = this._stack.pop();
          this._stack.push(a || b ? 1 : 0);
          break;
        }
        case OP.NOT:
          this._stack.push(this._stack.pop() ? 0 : 1);
          break;
          
        case OP.JMP:
          this._ip = bytecode[this._ip];
          break;
          
        case OP.JZ:
          if (this._stack.pop() === 0) {
            this._ip = bytecode[this._ip];
          } else {
            this._ip++;
          }
          break;
          
        case OP.JNZ:
          if (this._stack.pop() !== 0) {
            this._ip = bytecode[this._ip];
          } else {
            this._ip++;
          }
          break;
          
        case OP.LOAD: {
          const slot = bytecode[this._ip++];
          this._stack.push(this._locals[this._fp + slot]);
          break;
        }
          
        case OP.STORE: {
          const slot = bytecode[this._ip++];
          this._locals[this._fp + slot] = this._stack.pop();
          break;
        }
          
        case OP.CALL: {
          const addr = bytecode[this._ip++];
          this._callStack.push({ returnAddr: this._ip, savedFp: this._fp });
          this._fp += 16; // Allocate new frame (16 local slots)
          this._ip = addr;
          break;
        }
          
        case OP.RET: {
          if (this._callStack.length === 0) return this._stack[this._stack.length - 1] ?? null;
          const frame = this._callStack.pop();
          this._ip = frame.returnAddr;
          this._fp = frame.savedFp;
          break;
        }
          
        case OP.PRINT:
          this._output.push(this._stack[this._stack.length - 1]);
          break;
          
        case OP.HALT:
          return this._stack[this._stack.length - 1] ?? null;
          
        default:
          throw new Error(`Unknown opcode: 0x${op.toString(16)} at ip=${this._ip - 1}`);
      }
    }
    
    return this._stack[this._stack.length - 1] ?? null;
  }

  /**
   * Disassemble bytecode to human-readable form.
   */
  static disassemble(bytecode) {
    const lines = [];
    let ip = 0;
    while (ip < bytecode.length) {
      const op = bytecode[ip];
      const name = OP_NAMES[op] || `0x${op.toString(16)}`;
      
      if ([OP.PUSH, OP.JMP, OP.JZ, OP.JNZ, OP.LOAD, OP.STORE, OP.CALL].includes(op)) {
        const operand = bytecode[ip + 1];
        lines.push(`${String(ip).padStart(4, '0')}: ${name} ${operand}`);
        ip += 2;
      } else {
        lines.push(`${String(ip).padStart(4, '0')}: ${name}`);
        ip += 1;
      }
    }
    return lines.join('\n');
  }
}

/**
 * Assembler — convert assembly-like text to bytecode.
 * Format: one instruction per line, labels end with ':'
 */
export function assemble(source) {
  const lines = source.split('\n').map(l => l.trim()).filter(l => l && !l.startsWith(';'));
  const labels = {};
  const instructions = [];
  
  // First pass: find labels
  let addr = 0;
  for (const line of lines) {
    if (line.endsWith(':')) {
      labels[line.slice(0, -1)] = addr;
      continue;
    }
    const parts = line.split(/\s+/);
    const opName = parts[0].toUpperCase();
    addr += [OP.PUSH, OP.JMP, OP.JZ, OP.JNZ, OP.LOAD, OP.STORE, OP.CALL].includes(OP[opName]) ? 2 : 1;
    instructions.push({ line, parts });
  }
  
  // Second pass: emit bytecode
  const bytecode = [];
  for (const { parts } of instructions) {
    const opName = parts[0].toUpperCase();
    const opCode = OP[opName];
    if (opCode === undefined) throw new Error(`Unknown instruction: ${parts[0]}`);
    
    bytecode.push(opCode);
    
    if ([OP.PUSH, OP.LOAD, OP.STORE].includes(opCode)) {
      bytecode.push(Number(parts[1]));
    } else if ([OP.JMP, OP.JZ, OP.JNZ, OP.CALL].includes(opCode)) {
      const target = parts[1];
      const addr = labels[target] !== undefined ? labels[target] : Number(target);
      bytecode.push(addr);
    }
  }
  
  return bytecode;
}
