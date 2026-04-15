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
    
    // Heap allocation
    this.heapBase = 0x10000;  // Heap starts at 64KB
    this.needsHeap = false;   // Set true if any heap allocation needed
    this.needsAlloc = false;  // Set true if _alloc subroutine needed
    
    // Type tracking for type-directed compilation
    this.varTypes = new Map(); // name → 'int' | 'string' | 'array' | 'unknown'
    this._lastExprType = 'int'; // Type of last compiled expression
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
  compile(program, typeInfo = null, closureInfo = null) {
    this.output = [];
    this.variables = new Map();
    this.stackOffset = 8; // Reserve for ra + s0
    this.labelCount = 0;
    this.functions = [];
    this.errors = [];
    this.nextRegIdx = 0;
    this.usedRegs = new Set();
    this.varTypes = new Map();
    this._lastExprType = 'int';
    
    // Apply type info from inference pass
    this._typeInfo = typeInfo;
    this._closureInfo = closureInfo;
    this._closureLabels = [];
    this._varClosureLabels = new Map();
    if (typeInfo?.varTypes) {
      for (const [k, v] of typeInfo.varTypes) {
        this.varTypes.set(k, v);
      }
    }

    // Prologue: set up main frame
    this._emit('  # Monkey → RISC-V compiled program');
    this._emitLabel('_start');
    // Initialize heap pointer (gp = x3)
    this._emit(`  li gp, ${this.heapBase}`);
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

    // Append _alloc subroutine if needed
    if (this.needsAlloc) {
      this._emit('');
      this._emit('# Bump allocator: a0 = size in bytes, returns pointer in a0');
      this._emitLabel('_alloc');
      this._emit('  mv a0, gp');           // Return current heap pointer
      this._emit('  add gp, gp, a1');      // Bump by size (a1 = size passed by caller)
      this._emit('  ret');                   // Return with pointer in a0
    }

    // Append _str_eq subroutine if needed
    if (this.needsStrEq) {
      this._emit('');
      this._emit('# String equality: a0=str1, a1=str2, returns a0=1 if equal, 0 otherwise');
      this._emitLabel('_str_eq');
      this._emit('  lw t0, 0(a0)');        // t0 = len1
      this._emit('  lw t1, 0(a1)');        // t1 = len2
      this._emit('  bne t0, t1, _str_eq_ne'); // different lengths → not equal
      this._emit('  li t2, 0');             // t2 = char index
      this._emitLabel('_str_eq_loop');
      this._emit('  bge t2, t0, _str_eq_eq'); // all chars compared → equal
      this._emit('  slli t3, t2, 2');       // index * 4
      this._emit('  add t4, a0, t3');       
      this._emit('  lw t4, 4(t4)');         // str1[i]
      this._emit('  add t5, a1, t3');
      this._emit('  lw t5, 4(t5)');         // str2[i]
      this._emit('  bne t4, t5, _str_eq_ne'); // chars differ → not equal
      this._emit('  addi t2, t2, 1');
      this._emit('  j _str_eq_loop');
      this._emitLabel('_str_eq_eq');
      this._emit('  li a0, 1');
      this._emit('  ret');
      this._emitLabel('_str_eq_ne');
      this._emit('  li a0, 0');
      this._emit('  ret');
    }

    // Append _closure_dispatch trampoline if needed
    if (this.needsClosureDispatch && this._closureLabels && this._closureLabels.length > 0) {
      this._emit('');
      this._emit('# Closure dispatch: a0=closure_ptr, a1+=args');
      this._emit('# Reads fn_id from closure[0], checks num_captured');
      this._emit('# If num_captured==0 (function ref), shifts args down (a0=a1, a1=a2, ...)');
      this._emitLabel('_closure_dispatch');
      this._emit('  lw t0, 0(a0)');    // t0 = fn_id
      this._emit('  lw t2, 4(a0)');    // t2 = num_captured
      
      // If num_captured == 0, shift args down (this is a plain function ref, not a real closure)
      const noShift = this._label('_cd_noshift');
      this._emit(`  bnez t2, ${noShift}`);
      // Shift: a0=a1, a1=a2, a2=a3, ...
      this._emit('  mv a0, a1');
      this._emit('  mv a1, a2');
      this._emit('  mv a2, a3');
      this._emit('  mv a3, a4');
      this._emit('  mv a4, a5');
      this._emit('  mv a5, a6');
      this._emit('  mv a6, a7');
      this._emitLabel(noShift);
      
      for (let i = 0; i < this._closureLabels.length; i++) {
        const skipLabel = this._label('_cd_skip');
        this._emit(`  li t1, ${i}`);
        this._emit(`  bne t0, t1, ${skipLabel}`);
        this._emit(`  j ${this._closureLabels[i]}`);  // tail call — ra already set by caller's jal
        this._emitLabel(skipLabel);
      }
      // Fallthrough: unknown closure id — halt
      this._emit('  li a7, 10');
      this._emit('  ecall');
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
      // Check if it has free variables (is a closure)
      const freeVars = this._closureInfo?.get(stmt.value);
      if (freeVars && freeVars.length > 0) {
        // Compile as closure expression
        this._compileExpression(stmt.value);
        this.varTypes.set(name, 'closure');
        // Record which closure label this variable maps to
        this._varClosureLabels = this._varClosureLabels || new Map();
        const lastLabel = this._closureLabels?.[this._closureLabels.length - 1];
        if (lastLabel) this._varClosureLabels.set(name, lastLabel);
        this._allocLocal(name);
        this._emitStoreVar(name);
        return;
      }
      this._compileFunctionDef(name, stmt.value);
      return;
    }
    
    // Compile the value expression (result in a0)
    this._compileExpression(stmt.value);
    
    // Track the type of this variable
    this.varTypes.set(name, this._lastExprType);
    
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
      case 'ArrayLiteral':
        return this._compileArrayLiteral(expr);
      case 'StringLiteral':
        return this._compileStringLiteral(expr);
      case 'HashLiteral':
        return this._compileHashLiteral(expr);
      case 'IndexExpression':
        return this._compileIndexExpression(expr.left, expr.index);
      case 'ForInExpression':
        return this._compileForIn(expr);
      case 'FunctionLiteral':
        return this._compileFunctionLiteralExpr(expr);
      default:
        this.errors.push(`Unsupported expression: ${type}`);
    }
  }

  _compileIntegerLiteral(expr) {
    const val = expr.value;
    this._emit(`  li a0, ${val}`);
    this._lastExprType = 'int';
  }

  _compileBooleanLiteral(expr) {
    this._emit(`  li a0, ${expr.value ? 1 : 0}`);
    this._lastExprType = 'int';
  }

  _compileIdentifier(expr) {
    const name = expr.value;
    const varInfo = this._lookupVar(name);
    
    // If it's a function reference used as a value (not called), 
    // create a closure wrapper for it
    if (varInfo && varInfo.type === 'func') {
      this._comment(`function ref → closure: ${name}`);
      this.needsClosureDispatch = true;
      this._closureLabels = this._closureLabels || [];
      
      // Find or create closure label for this function
      let closureId = this._closureLabels.indexOf(name);
      if (closureId === -1) {
        closureId = this._closureLabels.length;
        this._closureLabels.push(name);
      }
      
      // Allocate closure object: [fn_id (4), num_captured=0 (4)]
      this._emit('  mv t1, gp');
      this._emit('  addi gp, gp, 8');
      this._emit(`  li t2, ${closureId}`);
      this._emit('  sw t2, 0(t1)');
      this._emit('  li t2, 0');       // no captured vars
      this._emit('  sw t2, 4(t1)');
      this._emit('  mv a0, t1');
      this._lastExprType = 'closure';
      return;
    }
    
    this._emitLoadVar(name);
    this._lastExprType = this.varTypes.get(name) || 'unknown';
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
    const op = expr.operator;
    
    // Check if this is string concatenation
    if (op === '+') {
      const leftType = this._inferExprType(expr.left);
      const rightType = this._inferExprType(expr.right);
      if (leftType === 'string' || rightType === 'string') {
        return this._compileStringConcat(expr.left, expr.right);
      }
    }
    
    // Check if this is string comparison
    if (op === '==' || op === '!=') {
      const leftType = this._inferExprType(expr.left);
      const rightType = this._inferExprType(expr.right);
      if (leftType === 'string' || rightType === 'string') {
        return this._compileStringCompare(expr.left, expr.right, op);
      }
    }
    
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
        this._emit('  slt a0, a0, t0');
        this._emit('  xori a0, a0, 1');
        break;
      case '>=':
        this._emit('  slt a0, t0, a0');
        this._emit('  xori a0, a0, 1');
        break;
      default:
        this.errors.push(`Unsupported infix operator: ${op}`);
    }
    this._lastExprType = 'int';
  }

  /** Infer the type of an expression from the AST (quick check, no deep analysis) */
  _inferExprType(expr) {
    if (!expr) return 'unknown';
    const name = expr.constructor.name;
    if (name === 'StringLiteral') return 'string';
    if (name === 'IntegerLiteral') return 'int';
    if (name === 'BooleanLiteral') return 'int';
    if (name === 'ArrayLiteral') return 'array';
    if (name === 'HashLiteral') return 'hash';
    if (name === 'Identifier') return this.varTypes.get(expr.value) || 'unknown';
    if (name === 'InfixExpression' && expr.operator === '+') {
      const lt = this._inferExprType(expr.left);
      const rt = this._inferExprType(expr.right);
      if (lt === 'string' || rt === 'string') return 'string';
    }
    return 'unknown';
  }

  /** Compile string concatenation: allocate new string, copy both */
  _compileStringConcat(left, right) {
    this._comment('string concat');
    
    // Compile left string, push pointer
    this._compileExpression(left);
    this._emit('  addi sp, sp, -4');
    this._emit('  sw a0, 0(sp)');
    
    // Compile right string, push pointer
    this._compileExpression(right);
    this._emit('  addi sp, sp, -4');
    this._emit('  sw a0, 0(sp)');
    
    // Pop both: t0 = right, t1 = left (reversed because stack is LIFO)
    this._emit('  lw t0, 0(sp)');    // t0 = right string ptr
    this._emit('  lw t1, 4(sp)');    // t1 = left string ptr
    this._emit('  addi sp, sp, 8');
    
    // Get lengths
    this._emit('  lw t2, 0(t1)');    // t2 = left length
    this._emit('  lw t3, 0(t0)');    // t3 = right length
    
    // New length = left + right
    this._emit('  add t4, t2, t3');   // t4 = total length
    
    // Allocate new string: 4 + totalLen * 4
    this._emit('  mv a0, gp');        // new string base
    this._emit('  addi t5, t4, 1');   // len + 1 (for header word)
    this._emit('  slli t5, t5, 2');   // * 4
    this._emit('  add gp, gp, t5');   // bump allocator
    
    // Store new length
    this._emit('  sw t4, 0(a0)');
    
    // Copy left string chars
    this._emit('  li t5, 0');         // i = 0
    const copyLeft = this._label('concat_left');
    const copyLeftEnd = this._label('concat_left_end');
    this._emitLabel(copyLeft);
    this._emit(`  bge t5, t2, ${copyLeftEnd}`);
    this._emit('  slli t6, t5, 2');
    this._emit('  add t6, t1, t6');
    this._emit('  lw t6, 4(t6)');     // left[i]
    this._emit('  slli a1, t5, 2');   // use a1 as temp
    this._emit('  add a1, a0, a1');
    this._emit('  sw t6, 4(a1)');     // new[i] = left[i]
    this._emit('  addi t5, t5, 1');
    this._emit(`  j ${copyLeft}`);
    this._emitLabel(copyLeftEnd);
    
    // Copy right string chars (starting at offset = left length)
    this._emit('  li t5, 0');         // j = 0
    const copyRight = this._label('concat_right');
    const copyRightEnd = this._label('concat_right_end');
    this._emitLabel(copyRight);
    this._emit(`  bge t5, t3, ${copyRightEnd}`);
    this._emit('  slli t6, t5, 2');
    this._emit('  add t6, t0, t6');
    this._emit('  lw t6, 4(t6)');     // right[j]
    this._emit('  add a1, t2, t5');   // offset = leftLen + j
    this._emit('  slli a1, a1, 2');
    this._emit('  add a1, a0, a1');
    this._emit('  sw t6, 4(a1)');     // new[leftLen+j] = right[j]
    this._emit('  addi t5, t5, 1');
    this._emit(`  j ${copyRight}`);
    this._emitLabel(copyRightEnd);
    
    // a0 = new string pointer
    this._lastExprType = 'string';
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

  _compileArrayLiteral(expr) {
    this._comment('array literal');
    this.needsAlloc = true;
    const elements = expr.elements || [];
    const numElements = elements.length;
    const totalSize = 4 + numElements * 4; // 4 bytes for length + 4 per element
    
    // Evaluate all elements first, push onto stack
    for (let i = 0; i < numElements; i++) {
      this._compileExpression(elements[i]);
      this._emit('  addi sp, sp, -4');
      this._emit('  sw a0, 0(sp)');
    }
    
    // Allocate heap space: save current gp as array base
    this._emit(`  mv t1, gp`);            // t1 = array base pointer
    this._emit(`  addi gp, gp, ${totalSize}`); // bump allocator
    
    // Store length at [base+0]
    this._emit(`  li t2, ${numElements}`);
    this._emit(`  sw t2, 0(t1)`);          // [base] = length
    
    // Pop elements from stack and store in array (reverse order)
    for (let i = numElements - 1; i >= 0; i--) {
      this._emit(`  lw t2, 0(sp)`);
      this._emit(`  addi sp, sp, 4`);
      this._emit(`  sw t2, ${4 + i * 4}(t1)`); // [base + 4 + i*4] = element[i]
    }
    
    // Result: array pointer in a0
    this._emit(`  mv a0, t1`);
    this._lastExprType = 'array';
  }

  _compileStringLiteral(expr) {
    this._comment(`string "${expr.value.slice(0, 20)}${expr.value.length > 20 ? '...' : ''}"`);
    const chars = expr.value;
    const len = chars.length;
    // String layout: [length (4 bytes)][char0 (4 bytes)][char1 (4 bytes)]...
    const totalSize = 4 + len * 4;
    
    // Allocate on heap
    this._emit('  mv t1, gp');
    this._emit(`  addi gp, gp, ${totalSize}`);
    
    // Store length
    this._emit(`  li t2, ${len}`);
    this._emit('  sw t2, 0(t1)');
    
    // Store characters
    for (let i = 0; i < len; i++) {
      const code = chars.charCodeAt(i);
      this._emit(`  li t2, ${code}`);
      this._emit(`  sw t2, ${4 + i * 4}(t1)`);
    }
    
    // Result: string pointer in a0 (untagged — type tracked at compile time)
    this._emit('  mv a0, t1');
    this._lastExprType = 'string';
  }

  _compileHashLiteral(expr) {
    this._comment('hash literal');
    const pairs = [...expr.pairs]; // Convert Map to array of [key, value]
    const numPairs = pairs.length;
    // Hash layout: [num_pairs (4)][key0 (4)][val0 (4)][key1 (4)][val1 (4)]...
    const totalSize = 4 + numPairs * 8;
    
    // Evaluate all keys and values, push to stack
    for (let i = 0; i < numPairs; i++) {
      const [key, value] = pairs[i];
      this._compileExpression(key);
      this._emit('  addi sp, sp, -4');
      this._emit('  sw a0, 0(sp)');
      this._compileExpression(value);
      this._emit('  addi sp, sp, -4');
      this._emit('  sw a0, 0(sp)');
    }
    
    // Allocate on heap
    this._emit('  mv t1, gp');
    this._emit(`  addi gp, gp, ${totalSize}`);
    
    // Store num_pairs
    this._emit(`  li t2, ${numPairs}`);
    this._emit('  sw t2, 0(t1)');
    
    // Pop pairs from stack (in reverse order) and store
    for (let i = numPairs - 1; i >= 0; i--) {
      // Pop value
      this._emit('  lw t2, 0(sp)');
      this._emit('  addi sp, sp, 4');
      this._emit(`  sw t2, ${4 + i * 8 + 4}(t1)`); // value slot
      
      // Pop key
      this._emit('  lw t2, 0(sp)');
      this._emit('  addi sp, sp, 4');
      this._emit(`  sw t2, ${4 + i * 8}(t1)`); // key slot
    }
    
    // Result: hash pointer in a0
    this._emit('  mv a0, t1');
    this._lastExprType = 'hash';
  }

  /** Compile string equality/inequality comparison */
  _compileStringCompare(left, right, op) {
    this._comment(`string ${op}`);
    
    // Compile both strings
    this._compileExpression(left);
    this._emit('  addi sp, sp, -4');
    this._emit('  sw a0, 0(sp)');
    this._compileExpression(right);
    this._emit('  lw t0, 0(sp)');     // t0 = left string ptr
    this._emit('  addi sp, sp, 4');
    this._emit('  mv t1, a0');        // t1 = right string ptr
    
    const equalLabel = this._label('streq');
    const notEqualLabel = this._label('strne');
    const endLabel = this._label('strcmp_end');
    
    // Compare lengths first
    this._emit('  lw t2, 0(t0)');     // t2 = left length
    this._emit('  lw t3, 0(t1)');     // t3 = right length
    this._emit(`  bne t2, t3, ${notEqualLabel}`);
    
    // Lengths match — compare chars
    this._emit('  li t4, 0');         // i = 0
    const charLoop = this._label('strcmp_loop');
    this._emitLabel(charLoop);
    this._emit(`  bge t4, t2, ${equalLabel}`);
    this._emit('  slli t5, t4, 2');
    this._emit('  add t5, t0, t5');
    this._emit('  lw t5, 4(t5)');     // left[i]
    this._emit('  slli t6, t4, 2');
    this._emit('  add t6, t1, t6');
    this._emit('  lw t6, 4(t6)');     // right[i]
    this._emit(`  bne t5, t6, ${notEqualLabel}`);
    this._emit('  addi t4, t4, 1');
    this._emit(`  j ${charLoop}`);
    
    // Equal
    this._emitLabel(equalLabel);
    this._emit(`  li a0, ${op === '==' ? 1 : 0}`);
    this._emit(`  j ${endLabel}`);
    
    // Not equal
    this._emitLabel(notEqualLabel);
    this._emit(`  li a0, ${op === '==' ? 0 : 1}`);
    
    this._emitLabel(endLabel);
    this._lastExprType = 'int';
  }

  _compileIndexExpression(left, index) {
    // Check if left is a hash
    const leftType = this._inferExprType(left);
    
    if (leftType === 'hash') {
      return this._compileHashAccess(left, index);
    }
    
    // Array/string indexing
    // Compile left (array/string pointer), push to stack
    this._compileExpression(left);
    this._emit('  addi sp, sp, -4');
    this._emit('  sw a0, 0(sp)');
    
    // Compile index
    this._compileExpression(index);
    
    // Pop array pointer into t0
    this._emit('  lw t0, 0(sp)');
    this._emit('  addi sp, sp, 4');
    
    // Compute element address: base + 4 + index * 4
    this._emit('  slli a0, a0, 2');        // index * 4
    this._emit('  add a0, t0, a0');        // base + index * 4
    this._emit('  lw a0, 4(a0)');          // load [base + 4 + index * 4]
    this._lastExprType = 'int'; // element type
  }

  _compileHashAccess(hashExpr, keyExpr) {
    this._comment('hash access');
    
    // Compile hash pointer
    this._compileExpression(hashExpr);
    this._emit('  addi sp, sp, -4');
    this._emit('  sw a0, 0(sp)');
    
    // Compile key
    const keyType = this._inferExprType(keyExpr);
    this._compileExpression(keyExpr);
    this._emit('  addi sp, sp, -4');
    this._emit('  sw a0, 0(sp)');
    
    // Pop key and hash pointer
    this._emit('  lw t0, 0(sp)');       // t0 = key
    this._emit('  lw t1, 4(sp)');       // t1 = hash pointer
    this._emit('  addi sp, sp, 8');
    
    // Linear scan: iterate through pairs
    this._emit('  lw t2, 0(t1)');       // t2 = num_pairs
    this._emit('  li t3, 0');           // t3 = i
    
    const scanLoop = this._label('hash_scan');
    const found = this._label('hash_found');
    const notFound = this._label('hash_notfound');
    
    this._emitLabel(scanLoop);
    this._emit(`  bge t3, t2, ${notFound}`);
    
    // Load key at index i
    this._emit('  slli t4, t3, 3');     // i * 8
    this._emit('  add t4, t1, t4');     // hash + i * 8
    this._emit('  lw t5, 4(t4)');       // key[i]
    
    if (keyType === 'string') {
      // String comparison via subroutine: a0=str1, a1=str2, returns a0=1 if equal
      this.needsStrEq = true;
      
      // Save loop context
      this._emit('  addi sp, sp, -20');
      this._emit('  sw t0, 0(sp)');   // search key
      this._emit('  sw t1, 4(sp)');   // hash ptr
      this._emit('  sw t2, 8(sp)');   // num_pairs
      this._emit('  sw t3, 12(sp)');  // loop index
      this._emit('  sw ra, 16(sp)');  // save ra for subroutine call
      
      // Call _str_eq(t0, t5) → result in a0
      this._emit('  mv a0, t0');      // a0 = search key
      this._emit('  mv a1, t5');      // a1 = candidate key
      this._emit('  jal _str_eq');
      
      // Restore loop context
      this._emit('  lw t0, 0(sp)');
      this._emit('  lw t1, 4(sp)');
      this._emit('  lw t2, 8(sp)');
      this._emit('  lw t3, 12(sp)');
      this._emit('  lw ra, 16(sp)');
      this._emit('  addi sp, sp, 20');
      
      // If a0 == 1, strings match → found
      this._emit(`  li t4, 1`);
      this._emit(`  beq a0, t4, ${found}`);
    } else {
      // Integer comparison
      this._emit(`  beq t0, t5, ${found}`);
    }
    
    this._emit('  addi t3, t3, 1');
    this._emit(`  j ${scanLoop}`);
    
    // Found: load value
    this._emitLabel(found);
    this._emit('  slli t4, t3, 3');
    this._emit('  add t4, t1, t4');
    this._emit('  lw a0, 8(t4)');       // value[i]
    const endLabel = this._label('hash_end');
    this._emit(`  j ${endLabel}`);
    
    // Not found: return 0 (null)
    this._emitLabel(notFound);
    this._emit('  li a0, 0');
    
    this._emitLabel(endLabel);
    this._lastExprType = 'unknown';
  }

  _compileForIn(expr) {
    this._comment(`for (${expr.variable} in ...)`);
    const uid = this.labelCount++;
    const loopLabel = this._label('forin');
    const endLabel = this._label('endforin');
    const arrName = `__forin_arr_${uid}`;
    const idxName = `__forin_idx_${uid}`;
    
    // Compile iterable (array pointer → a0)
    this._compileExpression(expr.iterable);
    
    // Save array pointer to a stack slot
    this._allocLocal(arrName);
    this._emitStoreVar(arrName);
    
    // Allocate loop counter
    this._allocLocal(idxName);
    this._emit('  li a0, 0');
    this._emitStoreVar(idxName);
    
    // Allocate loop variable
    this._allocLocal(expr.variable);
    
    // Loop start
    this._emitLabel(loopLabel);
    
    // Check: idx < len(arr)
    this._emitLoadVar(idxName);
    this._emit('  addi sp, sp, -4');
    this._emit('  sw a0, 0(sp)');       // push idx
    this._emitLoadVar(arrName);
    this._emit('  lw a0, 0(a0)');       // a0 = len(arr)
    this._emit('  lw t0, 0(sp)');       // t0 = idx
    this._emit('  addi sp, sp, 4');
    this._emit('  slt a0, t0, a0');     // a0 = (idx < len)
    this._emit(`  beqz a0, ${endLabel}`);
    
    // Load arr[idx] into loop variable
    this._emitLoadVar(arrName);
    this._emit('  mv t1, a0');          // t1 = arr pointer
    this._emitLoadVar(idxName);
    this._emit('  slli a0, a0, 2');     // idx * 4
    this._emit('  add a0, t1, a0');     // arr + idx * 4
    this._emit('  lw a0, 4(a0)');       // arr[idx]
    this._emitStoreVar(expr.variable);
    
    // Execute body
    this._compileBlock(expr.body);
    
    // Increment index
    this._emitLoadVar(idxName);
    this._emit('  addi a0, a0, 1');
    this._emitStoreVar(idxName);
    
    this._emit(`  j ${loopLabel}`);
    this._emitLabel(endLabel);
  }

  /** Compile a function literal as an expression (closure creation) */
  _compileFunctionLiteralExpr(funcLit) {
    const closureLabel = this._label('closure_fn');
    this._comment(`closure ${closureLabel}`);
    
    // Identify free variables using closure analysis
    const freeVars = this._closureInfo?.get(funcLit) || [];
    
    // Allocate closure object on heap: [fn_id (4)] [num_captured (4)] [var0 (4)] [var1 (4)] ...
    const closureSize = 8 + freeVars.length * 4;
    this._emit('  mv t1, gp');
    this._emit(`  addi gp, gp, ${closureSize}`);
    
    // Store closure function ID (we'll use the label index for dispatch)
    this._closureLabels = this._closureLabels || [];
    const closureId = this._closureLabels.length;
    this._closureLabels.push(closureLabel);
    this._emit(`  li t2, ${closureId}`);
    this._emit('  sw t2, 0(t1)');
    
    // Store number of captured variables
    this._emit(`  li t2, ${freeVars.length}`);
    this._emit('  sw t2, 4(t1)');
    
    // Capture current values of free variables
    for (let i = 0; i < freeVars.length; i++) {
      this._emitLoadVar(freeVars[i]);
      this._emit(`  sw a0, ${8 + i * 4}(t1)`);
    }
    
    // Compile the function body as a deferred function
    const savedOutput = this.output;
    const savedVars = new Map(this.variables);
    const savedOffset = this.stackOffset;
    const savedNextReg = this.nextRegIdx;
    const savedUsedRegs = new Set(this.usedRegs);
    const savedVarTypes = new Map(this.varTypes);
    
    this.output = [];
    this.variables = new Map();
    this.stackOffset = 8;
    this.nextRegIdx = 0;
    this.usedRegs = new Set();
    this.varTypes = new Map();
    
    this._emitLabel(closureLabel);
    this._emitPrologue();
    
    // First implicit parameter: closure environment pointer (in a0)
    const envName = '__closure_env';
    this._allocLocal(envName);
    this._emitStoreVar(envName);
    
    // Map captured variables to environment offsets
    for (let i = 0; i < freeVars.length; i++) {
      // Create a "virtual" local that loads from the env object
      const capturedName = freeVars[i];
      this._allocLocal(capturedName);
      // Load from env: env_ptr + 8 + i * 4
      this._emitLoadVar(envName);
      this._emit(`  lw a0, ${8 + i * 4}(a0)`);
      this._emitStoreVar(capturedName);
      this.varTypes.set(capturedName, savedVarTypes.get(capturedName) || 'unknown');
    }
    
    // Map explicit parameters (shifted by 1 for env pointer)
    if (funcLit.parameters) {
      for (let i = 0; i < funcLit.parameters.length; i++) {
        const paramName = funcLit.parameters[i].value;
        this._allocLocal(paramName);
        const loc = this._lookupVar(paramName);
        if (loc.type === 'reg') {
          this._emit(`  mv ${loc.reg}, a${i + 1}`); // shifted by 1
        } else {
          this._emit(`  sw a${i + 1}, ${loc.offset}(s0)`);
        }
      }
    }
    
    // Compile body
    let hasReturn = false;
    if (funcLit.body?.statements) {
      for (const stmt of funcLit.body.statements) {
        this._compileStatement(stmt);
        if (stmt.constructor.name === 'ReturnStatement') {
          hasReturn = true;
          break;
        }
      }
    }
    
    if (!hasReturn) {
      this._emitEpilogue();
      this._emit('  ret');
    }
    
    this._patchPrologueSaves();
    const funcBody = this.output;
    
    // Restore state
    this.output = savedOutput;
    this.variables = savedVars;
    this.stackOffset = savedOffset;
    this.nextRegIdx = savedNextReg;
    this.usedRegs = savedUsedRegs;
    this.varTypes = savedVarTypes;
    
    this.functions.push(funcBody);
    
    // Result: closure pointer in a0
    this._emit('  mv a0, t1');
    this._lastExprType = 'closure';
  }

  _compileCallExpression(expr) {
    const funcName = expr.function.value || expr.function.toString();
    
    // Special case: puts() → print_int or print_string
    if (funcName === 'puts') {
      this._comment('puts()');
      if (expr.arguments.length > 0) {
        this._compileExpression(expr.arguments[0]);
        const exprType = this._lastExprType;
        
        if (exprType === 'string') {
          // String: a0 has heap pointer, print chars
          this._emit('  mv t1, a0');
          this._emit('  lw t2, 0(t1)');     // length
          this._emit('  li t3, 0');          // index
          const charLoop = this._label('puts_char');
          const charEnd = this._label('puts_char_end');
          this._emitLabel(charLoop);
          this._emit(`  bge t3, t2, ${charEnd}`);
          this._emit('  slli t4, t3, 2');
          this._emit('  add t4, t1, t4');
          this._emit('  lw a0, 4(t4)');
          this._emit('  li a7, 11');         // print_char
          this._emit('  ecall');
          this._emit('  addi t3, t3, 1');
          this._emit(`  j ${charLoop}`);
          this._emitLabel(charEnd);
        } else {
          // Integer (or unknown): print as number
          this._emit('  li a7, 1');          // print_int
          this._emit('  ecall');
        }
      }
      return;
    }
    
    // Special case: len() → load length from header
    if (funcName === 'len') {
      this._comment('len()');
      if (expr.arguments.length > 0) {
        this._compileExpression(expr.arguments[0]);
        this._emit('  lw a0, 0(a0)');  // Load length from header
      }
      this._lastExprType = 'int'; // length is always an integer
      return;
    }
    
    // Special case: first() → arr[0]
    if (funcName === 'first') {
      this._comment('first()');
      if (expr.arguments.length > 0) {
        this._compileExpression(expr.arguments[0]);
        this._emit('  lw a0, 4(a0)');  // Load first element
      }
      this._lastExprType = 'int'; // element type unknown, assume int
      return;
    }
    
    // Special case: last() → arr[len-1]
    if (funcName === 'last') {
      this._comment('last()');
      if (expr.arguments.length > 0) {
        this._compileExpression(expr.arguments[0]);
        this._emit('  mv t0, a0');
        this._emit('  lw t1, 0(t0)');
        this._emit('  slli t1, t1, 2');
        this._emit('  add t0, t0, t1');
        this._emit('  lw a0, 0(t0)');
      }
      this._lastExprType = 'int';
      return;
    }
    
    // Special case: push() → create new array with element appended
    if (funcName === 'push') {
      this._comment('push()');
      this.needsAlloc = true;
      if (expr.arguments.length >= 2) {
        // Compile array arg
        this._compileExpression(expr.arguments[0]);
        this._emit('  addi sp, sp, -4');
        this._emit('  sw a0, 0(sp)');     // push old array pointer
        
        // Compile element to push
        this._compileExpression(expr.arguments[1]);
        this._emit('  addi sp, sp, -4');
        this._emit('  sw a0, 0(sp)');     // push new element
        
        // Pop element and old array
        this._emit('  lw t2, 0(sp)');     // t2 = new element
        this._emit('  lw t0, 4(sp)');     // t0 = old array
        this._emit('  addi sp, sp, 8');
        
        // Get old length
        this._emit('  lw t1, 0(t0)');     // t1 = old length
        
        // Allocate new array: (length + 1 + 1) * 4 bytes (header + old elements + new)
        this._emit('  addi t3, t1, 1');   // t3 = new length
        this._emit('  addi t4, t3, 1');   // t4 = new length + 1 (header)
        this._emit('  slli t4, t4, 2');   // t4 * 4 = total size
        this._emit('  mv a0, gp');        // new array base
        this._emit('  add gp, gp, t4');   // bump allocator
        
        // Store new length
        this._emit('  sw t3, 0(a0)');     // [new_arr] = new_length
        
        // Copy old elements
        this._emit('  li t4, 0');         // i = 0
        const copyLoop = this._label('push_copy');
        const copyEnd = this._label('push_copy_end');
        this._emitLabel(copyLoop);
        this._emit('  bge t4, t1, ' + copyEnd);
        this._emit('  slli t5, t4, 2');   // i * 4
        this._emit('  add t5, t0, t5');   // old_arr + i * 4
        this._emit('  lw t6, 4(t5)');     // old_arr[i]
        this._emit('  slli t5, t4, 2');
        this._emit('  add t5, a0, t5');   // new_arr + i * 4
        this._emit('  sw t6, 4(t5)');     // new_arr[i] = old_arr[i]
        this._emit('  addi t4, t4, 1');
        this._emit('  j ' + copyLoop);
        this._emitLabel(copyEnd);
        
        // Store new element at end
        this._emit('  slli t5, t1, 2');   // old_length * 4
        this._emit('  add t5, a0, t5');   // new_arr + old_length * 4
        this._emit('  sw t2, 4(t5)');     // new_arr[old_length] = new_element
        
        // a0 already has new array pointer
      }
      return;
    }
    
    // General function call
    this._comment(`call ${funcName}`);
    
    // Check if this is a closure call (variable of type 'closure')
    const callerType = this.varTypes.get(funcName);
    const isClosure = callerType === 'closure';
    
    // Push current temp registers to save them
    // Compile arguments into a0-a7
    const args = expr.arguments;
    if (args.length > (isClosure ? 7 : 8)) {
      this.errors.push(`Too many arguments (max ${isClosure ? 7 : 8}): ${args.length}`);
      return;
    }
    
    if (isClosure) {
      // Closure call: first arg is the closure environment pointer
      // Evaluate all args first, save on stack
      for (let i = 0; i < args.length; i++) {
        this._compileExpression(args[i]);
        this._emit('  addi sp, sp, -4');
        this._emit('  sw a0, 0(sp)');
      }
      
      // Load closure pointer
      this._emitLoadVar(funcName);
      this._emit('  addi sp, sp, -4');
      this._emit('  sw a0, 0(sp)');
      
      // Pop closure pointer into a0 (first arg = env)
      this._emit('  lw a0, 0(sp)');
      // Pop regular args into a1, a2, ...
      for (let i = args.length - 1; i >= 0; i--) {
        this._emit(`  lw a${i + 1}, ${(args.length - i) * 4}(sp)`);
      }
      this._emit(`  addi sp, sp, ${(args.length + 1) * 4}`);
      
      // Get the closure function label from the closure labels table
      // For now, look up the closure label from the variable's associated closure
      const closureLabel = this._varClosureLabels?.get(funcName);
      if (closureLabel) {
        this._emit(`  jal ${closureLabel}`);
      } else {
        this.errors.push(`Unknown closure label for ${funcName}`);
      }
    } else {
      // Check if this is a variable that might hold a closure
      const varInfo = this._lookupVar(funcName);
      const isVarClosure = varInfo && (varInfo.type === 'stack' || varInfo.type === 'reg');
      
      if (isVarClosure) {
        // Variable-based closure call: load closure ptr, call through dispatch
        this._comment(`indirect closure call via ${funcName}`);
        this.needsClosureDispatch = true;
        
        // Evaluate args first, save on stack
        for (let i = 0; i < args.length; i++) {
          this._compileExpression(args[i]);
          this._emit('  addi sp, sp, -4');
          this._emit('  sw a0, 0(sp)');
        }
        
        // Load closure pointer
        this._emitLoadVar(funcName);
        this._emit('  addi sp, sp, -4');
        this._emit('  sw a0, 0(sp)');
        
        // Pop closure pointer into a0 (env)
        this._emit('  lw a0, 0(sp)');
        // Pop regular args into a1, a2, ...
        for (let i = args.length - 1; i >= 0; i--) {
          this._emit(`  lw a${i + 1}, ${(args.length - i) * 4}(sp)`);
        }
        this._emit(`  addi sp, sp, ${(args.length + 1) * 4}`);
        
        // Call the closure dispatch trampoline
        // a0 = closure pointer (env), a1+ = args
        this._emit('  jal _closure_dispatch');
      } else {
      // Regular function call
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
      }
    }
    // Result is in a0
    // Check if we know the return type
    if (this._typeInfo?.funcTypes?.has(funcName)) {
      this._lastExprType = this._typeInfo.funcTypes.get(funcName).returnType || 'unknown';
    } else {
      this._lastExprType = 'unknown';
    }
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
    
    // Add self-reference for recursive calls
    this.variables.set(name, { type: 'func', label: name });
    
    // Copy all function labels from outer scope (for cross-function calls)
    for (const [varName, varInfo] of savedVars) {
      if (varInfo.type === 'func') {
        this.variables.set(varName, varInfo);
      }
    }
    
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
        // Apply inferred parameter type
        if (this._typeInfo?.funcTypes?.has(name)) {
          const funcInfo = this._typeInfo.funcTypes.get(name);
          const paramType = funcInfo.params.get(paramName);
          if (paramType) {
            this.varTypes.set(paramName, paramType);
          }
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
