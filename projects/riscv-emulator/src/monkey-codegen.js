// monkey-codegen.js — Monkey Language → RISC-V RV32I Code Generator
//
// Compiles monkey-lang AST to RISC-V assembly text.
// Assembly can then be fed to the Assembler to produce machine code
// for execution on the RISC-V CPU emulator.
//
// Approach: stack-based code generation
//   - All intermediate values are pushed/popped on the RISC-V stack
//   - Variables are stack-allocated with known offsets from frame pointer (s0/fp)
//   - Functions use standard RISC-V calling convention (a0-a7 for args)
//   - Result of expression evaluation ends up in a0
//
// Calling convention:
//   - a0-a7 (x10-x17): arguments and return values
//   - s0 (x8): frame pointer
//   - sp (x2): stack pointer
//   - ra (x1): return address
//
// Syscalls for I/O:
//   - ecall with a7=1: print integer in a0
//   - ecall with a7=10: exit
//   - ecall with a7=11: print char in a0

export class RiscVCodeGen {
  constructor(options = {}) {
    this.output = [];         // Assembly lines
    this.variables = new Map(); // name → { type: 'stack'|'reg', offset?, reg? }
    this.stackOffset = 8;    // Start at 8: reserve s0-4=ra, s0-8=old_s0
    this.labelCount = 0;     // For generating unique labels
    this.functions = [];      // Deferred function bodies
    this.currentScope = null; // Variable scope chain
    this.errors = [];
    this.frameSize = 256;    // Default frame size
    
    // Register allocation
    this.useRegisters = options.useRegisters || false;
    // Callee-saved registers available for locals: s1-s11
    this.availableRegs = ['s1','s2','s3','s4','s5','s6','s7','s8','s9','s10','s11'];
    this.nextRegIdx = 0;     // Next available register index
    this.usedRegs = new Set(); // Which s-registers are in use (for save/restore)
  }

  /** Generate a unique label */
  _label(prefix = 'L') {
    return `${prefix}_${this.labelCount++}`;
  }

  /** Emit a line of assembly */
  _emit(line) {
    this.output.push(line);
  }

  /** Emit a comment */
  _comment(text) {
    this._emit(`  # ${text}`);
  }

  /** Emit a label */
  _emitLabel(label) {
    this._emit(`${label}:`);
  }

  /** Allocate storage for a variable — register if available, otherwise stack */
  _allocLocal(name) {
    if (this.useRegisters && this.nextRegIdx < this.availableRegs.length) {
      const reg = this.availableRegs[this.nextRegIdx++];
      this.usedRegs.add(reg);
      this.variables.set(name, { type: 'reg', reg });
      return { type: 'reg', reg };
    }
    // Fall back to stack
    this.stackOffset += 4;
    const offset = -this.stackOffset;
    this.variables.set(name, { type: 'stack', offset });
    return { type: 'stack', offset };
  }

  /** Look up variable location */
  _lookupVar(name) {
    if (this.variables.has(name)) {
      return this.variables.get(name);
    }
    this.errors.push(`Undefined variable: ${name}`);
    return { type: 'stack', offset: 0 };
  }

  /** Emit: load variable value into a0 */
  _emitLoadVar(name) {
    const loc = this._lookupVar(name);
    if (loc.type === 'reg') {
      this._emit(`  mv a0, ${loc.reg}`);
    } else {
      this._emit(`  lw a0, ${loc.offset}(s0)`);
    }
  }

  /** Emit: store a0 into variable */
  _emitStoreVar(name) {
    const loc = this._lookupVar(name);
    if (loc.type === 'reg') {
      this._emit(`  mv ${loc.reg}, a0`);
    } else {
      this._emit(`  sw a0, ${loc.offset}(s0)`);
    }
  }

  /** Emit function prologue — returns placeholder index for deferred save/restore */
  _emitPrologue() {
    this._emit(`  addi sp, sp, -${this.frameSize}`);
    this._emit(`  sw ra, ${this.frameSize - 4}(sp)`);
    this._emit(`  sw s0, ${this.frameSize - 8}(sp)`);
    // Placeholder for callee-saved register saves — will be filled in later
    this._prologueSaveIdx = this.output.length;
    this._emit(`  addi s0, sp, ${this.frameSize}`);
  }

  /** Emit function epilogue */
  _emitEpilogue() {
    // Restore callee-saved registers
    if (this.useRegisters) {
      let saveOffset = this.frameSize - 12; // After ra and s0
      for (const reg of this.usedRegs) {
        this._emit(`  lw ${reg}, ${saveOffset}(sp)`);
        saveOffset -= 4;
      }
    }
    this._emit(`  lw ra, ${this.frameSize - 4}(sp)`);
    this._emit(`  lw s0, ${this.frameSize - 8}(sp)`);
    this._emit(`  addi sp, sp, ${this.frameSize}`);
  }

  /** Patch prologue to save callee-saved registers (call after compilation) */
  _patchPrologueSaves() {
    if (!this.useRegisters || this.usedRegs.size === 0) return;
    const saves = [];
    let saveOffset = this.frameSize - 12;
    for (const reg of this.usedRegs) {
      saves.push(`  sw ${reg}, ${saveOffset}(sp)`);
      saveOffset -= 4;
    }
    // Insert saves at the placeholder position
    this.output.splice(this._prologueSaveIdx, 0, ...saves);
  }

  /**
   * Compile a monkey-lang program to RISC-V assembly.
   * @param {import('../../monkey-lang/src/ast.js').Program} program
   * @returns {string} Assembly text
   */
  compile(program) {
    this.output = [];
    this.variables = new Map();
    this.stackOffset = 8; // Reserve for ra + s0
    this.labelCount = 0;
    this.functions = [];
    this.errors = [];
    this.nextRegIdx = 0;
    this.usedRegs = new Set();

    // Prologue: set up main frame
    this._emit('  # Monkey → RISC-V compiled program');
    this._emitLabel('_start');
    this._emitPrologue();

    // Compile program body
    for (const stmt of program.statements) {
      this._compileStatement(stmt);
    }

    // Epilogue: exit
    this._comment('exit');
    this._emitEpilogue();
    this._emit('  li a7, 10');           // exit syscall
    this._emit('  ecall');

    // Patch prologue with callee-saved register saves
    this._patchPrologueSaves();

    // Append deferred function bodies
    for (const fn of this.functions) {
      this._emit('');
      this.output.push(...fn);
    }

    if (this.errors.length > 0) {
      throw new Error(`Compilation errors:\n${this.errors.join('\n')}`);
    }

    return this.output.join('\n');
  }

  // --- Statement compilation ---

  _compileStatement(stmt) {
    const type = stmt.constructor.name;
    switch (type) {
      case 'LetStatement':
        return this._compileLet(stmt);
      case 'SetStatement':
        return this._compileSet(stmt);
      case 'ReturnStatement':
        return this._compileReturn(stmt);
      case 'ExpressionStatement':
        return this._compileExpression(stmt.expression);
      default:
        this.errors.push(`Unsupported statement: ${type}`);
    }
  }

  _compileLet(stmt) {
    const name = stmt.name.value;
    this._comment(`let ${name}`);
    
    // Check if value is a function literal
    if (stmt.value && stmt.value.constructor.name === 'FunctionLiteral') {
      this._compileFunctionDef(name, stmt.value);
      return;
    }
    
    // Compile the value expression (result in a0)
    this._compileExpression(stmt.value);
    
    // Allocate storage and store
    this._allocLocal(name);
    this._emitStoreVar(name);
  }

  _compileSet(stmt) {
    const name = stmt.name.value;
    this._comment(`set ${name}`);
    this._compileExpression(stmt.value);
    this._emitStoreVar(name);
  }

  _compileReturn(stmt) {
    this._comment('return');
    if (stmt.returnValue) {
      this._compileExpression(stmt.returnValue);
    }
    // a0 already has the return value
    this._emitEpilogue();
    this._emit('  ret');
  }

  // --- Expression compilation ---
  // All expressions leave their result in a0

  _compileExpression(expr) {
    if (!expr) return;
    const type = expr.constructor.name;
    switch (type) {
      case 'IntegerLiteral':
        return this._compileIntegerLiteral(expr);
      case 'BooleanLiteral':
        return this._compileBooleanLiteral(expr);
      case 'Identifier':
        return this._compileIdentifier(expr);
      case 'PrefixExpression':
        return this._compilePrefixExpression(expr);
      case 'InfixExpression':
        return this._compileInfixExpression(expr);
      case 'IfExpression':
        return this._compileIfExpression(expr);
      case 'CallExpression':
        return this._compileCallExpression(expr);
      case 'BlockStatement':
        return this._compileBlock(expr);
      case 'WhileExpression':
        return this._compileWhile(expr);
      default:
        this.errors.push(`Unsupported expression: ${type}`);
    }
  }

  _compileIntegerLiteral(expr) {
    const val = expr.value;
    // li pseudo-instruction handles all sizes
    this._emit(`  li a0, ${val}`);
  }

  _compileBooleanLiteral(expr) {
    this._emit(`  li a0, ${expr.value ? 1 : 0}`);
  }

  _compileIdentifier(expr) {
    const name = expr.value;
    this._emitLoadVar(name);
  }

  _compilePrefixExpression(expr) {
    this._compileExpression(expr.right);
    switch (expr.operator) {
      case '-':
        this._emit('  neg a0, a0');  // pseudo: sub a0, zero, a0
        break;
      case '!':
        this._emit('  seqz a0, a0'); // a0 = (a0 == 0) ? 1 : 0
        break;
      default:
        this.errors.push(`Unsupported prefix operator: ${expr.operator}`);
    }
  }

  _compileInfixExpression(expr) {
    // Check for comparison operators
    const op = expr.operator;
    
    // Compile left, push to stack
    this._compileExpression(expr.left);
    this._emit('  addi sp, sp, -4');
    this._emit('  sw a0, 0(sp)');
    
    // Compile right (result in a0)
    this._compileExpression(expr.right);
    
    // Pop left into t0
    this._emit('  lw t0, 0(sp)');
    this._emit('  addi sp, sp, 4');
    
    // t0 = left, a0 = right
    switch (op) {
      case '+':
        this._emit('  add a0, t0, a0');
        break;
      case '-':
        this._emit('  sub a0, t0, a0');
        break;
      case '*':
        // RV32I doesn't have MUL — use software multiply
        // Actually RV32M extension has mul, let's use it (our emulator supports it)
        this._emit('  mul a0, t0, a0');
        break;
      case '/':
        this._emit('  div a0, t0, a0');
        break;
      case '%':
        this._emit('  rem a0, t0, a0');
        break;
      case '<':
        this._emit('  slt a0, t0, a0');
        break;
      case '>':
        this._emit('  slt a0, a0, t0');
        break;
      case '==':
        this._emit('  sub a0, t0, a0');
        this._emit('  seqz a0, a0');
        break;
      case '!=':
        this._emit('  sub a0, t0, a0');
        this._emit('  snez a0, a0');
        break;
      case '<=':
        // a <= b ≡ !(a > b) ≡ !(b < a)
        this._emit('  slt a0, a0, t0');  // a0 = (right < left)
        this._emit('  xori a0, a0, 1'); // negate
        break;
      case '>=':
        // a >= b ≡ !(a < b)
        this._emit('  slt a0, t0, a0');  // a0 = (left < right)
        this._emit('  xori a0, a0, 1'); // negate
        break;
      default:
        this.errors.push(`Unsupported infix operator: ${op}`);
    }
  }

  _compileIfExpression(expr) {
    const elseLabel = this._label('else');
    const endLabel = this._label('endif');
    
    // Compile condition
    this._compileExpression(expr.condition);
    this._emit(`  beqz a0, ${elseLabel}`);
    
    // Compile consequence
    this._compileBlock(expr.consequence);
    
    if (expr.alternative) {
      this._emit(`  j ${endLabel}`);
      this._emitLabel(elseLabel);
      this._compileBlock(expr.alternative);
      this._emitLabel(endLabel);
    } else {
      this._emitLabel(elseLabel);
    }
  }

  _compileBlock(block) {
    if (!block || !block.statements) return;
    for (const stmt of block.statements) {
      this._compileStatement(stmt);
    }
  }

  _compileWhile(expr) {
    const loopLabel = this._label('while');
    const endLabel = this._label('endwhile');
    
    this._emitLabel(loopLabel);
    this._compileExpression(expr.condition);
    this._emit(`  beqz a0, ${endLabel}`);
    this._compileBlock(expr.body);
    this._emit(`  j ${loopLabel}`);
    this._emitLabel(endLabel);
  }

  _compileCallExpression(expr) {
    const funcName = expr.function.value || expr.function.toString();
    
    // Special case: puts() → print_int syscall
    if (funcName === 'puts') {
      this._comment('puts()');
      if (expr.arguments.length > 0) {
        this._compileExpression(expr.arguments[0]);
        this._emit('  mv a0, a0');  // nop — value already in a0
        this._emit('  li a7, 1');   // print_int syscall
        this._emit('  ecall');
      }
      return;
    }
    
    // General function call
    this._comment(`call ${funcName}`);
    
    // Push current temp registers to save them
    // Compile arguments into a0-a7
    const args = expr.arguments;
    if (args.length > 8) {
      this.errors.push(`Too many arguments (max 8): ${args.length}`);
      return;
    }
    
    // Evaluate args and save on stack
    for (let i = 0; i < args.length; i++) {
      this._compileExpression(args[i]);
      this._emit('  addi sp, sp, -4');
      this._emit('  sw a0, 0(sp)');
    }
    
    // Pop into argument registers (reverse order)
    for (let i = args.length - 1; i >= 0; i--) {
      this._emit(`  lw a${i}, ${(args.length - 1 - i) * 4}(sp)`);
    }
    this._emit(`  addi sp, sp, ${args.length * 4}`);
    
    // Call the function
    this._emit(`  jal ${funcName}`);
    // Result is in a0
  }

  _compileFunctionDef(name, funcLit) {
    this._comment(`function ${name} (deferred)`);
    
    // Save function label for calls
    this.variables.set(name, { type: 'func', label: name });
    
    // Generate function body (deferred — appended after main)
    const savedOutput = this.output;
    const savedVars = new Map(this.variables);
    const savedOffset = this.stackOffset;
    const savedNextReg = this.nextRegIdx;
    const savedUsedRegs = new Set(this.usedRegs);
    
    this.output = [];
    this.variables = new Map();
    this.stackOffset = 8; // Reserve for ra + s0
    this.nextRegIdx = 0;
    this.usedRegs = new Set();
    
    this._emitLabel(name);
    this._emitPrologue();
    
    // Map parameters to storage (registers or stack)
    if (funcLit.parameters) {
      for (let i = 0; i < funcLit.parameters.length; i++) {
        const paramName = funcLit.parameters[i].value;
        this._allocLocal(paramName);
        // Store from argument register to allocated location
        const loc = this._lookupVar(paramName);
        if (loc.type === 'reg') {
          this._emit(`  mv ${loc.reg}, a${i}`);
        } else {
          this._emit(`  sw a${i}, ${loc.offset}(s0)`);
        }
      }
    }
    
    // Compile body
    let hasReturn = false;
    if (funcLit.body && funcLit.body.statements) {
      for (const stmt of funcLit.body.statements) {
        this._compileStatement(stmt);
        if (stmt.constructor.name === 'ReturnStatement') {
          hasReturn = true;
          break;
        }
      }
    }
    
    // Default epilogue (if no explicit return)
    if (!hasReturn) {
      this._emitEpilogue();
      this._emit('  ret');
    }
    
    // Patch prologue saves for this function
    this._patchPrologueSaves();
    
    const funcBody = this.output;
    
    // Restore state
    this.output = savedOutput;
    this.variables = savedVars;
    this.stackOffset = savedOffset;
    this.nextRegIdx = savedNextReg;
    this.usedRegs = savedUsedRegs;
    
    this.functions.push(funcBody);
  }
}
