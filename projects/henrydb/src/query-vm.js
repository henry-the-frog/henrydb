// query-vm.js — Stack-based Query Virtual Machine
// Inspired by SQLite's VDBE (Virtual DataBase Engine)
//
// Compiles SQL-like operations to bytecode and executes them.
// Each instruction operates on a register file + stack.

// ============================================================
// OPCODES
// ============================================================
export const OP = {
  // Control flow
  HALT:       0x00,  // Stop execution
  GOTO:       0x01,  // Jump to address (p1)
  IF_TRUE:    0x02,  // If reg[p1] is truthy, jump to p2
  IF_FALSE:   0x03,  // If reg[p1] is falsy, jump to p2
  
  // Data movement
  LOAD_CONST: 0x10,  // reg[p1] = constants[p2]
  LOAD_NULL:  0x11,  // reg[p1] = null
  MOVE:       0x12,  // reg[p1] = reg[p2]
  COPY:       0x13,  // reg[p1] = deep copy of reg[p2]
  
  // Table operations
  OPEN_TABLE: 0x20,  // Open table p1 (name in constants[p2]) for scanning
  NEXT_ROW:   0x21,  // Load next row from table p1 into reg[p2]. If done, jump to p3
  COLUMN:     0x22,  // reg[p1] = current row's column named constants[p2] from table p3
  
  // Arithmetic
  ADD:        0x30,  // reg[p1] = reg[p2] + reg[p3]
  SUB:        0x31,  // reg[p1] = reg[p2] - reg[p3]
  MUL:        0x32,  // reg[p1] = reg[p2] * reg[p3]
  DIV:        0x33,  // reg[p1] = reg[p2] / reg[p3]
  MOD:        0x34,  // reg[p1] = reg[p2] % reg[p3]
  NEG:        0x35,  // reg[p1] = -reg[p2]
  
  // Comparison
  EQ:         0x40,  // reg[p1] = reg[p2] == reg[p3] ? 1 : 0
  NE:         0x41,  // reg[p1] = reg[p2] != reg[p3] ? 1 : 0
  LT:         0x42,  // reg[p1] = reg[p2] < reg[p3] ? 1 : 0
  LE:         0x43,  // reg[p1] = reg[p2] <= reg[p3] ? 1 : 0
  GT:         0x44,  // reg[p1] = reg[p2] > reg[p3] ? 1 : 0
  GE:         0x45,  // reg[p1] = reg[p2] >= reg[p3] ? 1 : 0
  IS_NULL:    0x46,  // reg[p1] = (reg[p2] === null) ? 1 : 0
  
  // String
  CONCAT:     0x50,  // reg[p1] = String(reg[p2]) + String(reg[p3])
  LIKE:       0x51,  // reg[p1] = reg[p2] matches pattern reg[p3] ? 1 : 0
  
  // Aggregate
  AGG_INIT:   0x60,  // Initialize aggregate slot p1 with function type p2 (0=count,1=sum,2=min,3=max,4=avg)
  AGG_STEP:   0x61,  // Feed value reg[p2] to aggregate slot p1
  AGG_FINAL:  0x62,  // reg[p1] = final value of aggregate slot p2
  
  // Output
  EMIT_ROW:   0x70,  // Emit current result row (from registers p1..p1+p2-1)
  
  // Hash operations (for GROUP BY / JOIN)
  HASH_INIT:  0x80,  // Initialize hash table in slot p1
  HASH_PUT:   0x81,  // hash[p1][reg[p2]] = current aggregate state
  HASH_GET:   0x82,  // Load aggregate state for key reg[p2] from hash[p1], or jump to p3 if new
  HASH_NEXT:  0x83,  // Iterate hash[p1]: load next group key into reg[p2], or jump to p3 if done
};

// Aggregate function types
export const AGG = {
  COUNT: 0,
  SUM:   1,
  MIN:   2,
  MAX:   3,
  AVG:   4,
};

// ============================================================
// INSTRUCTION FORMAT
// ============================================================
export class Instruction {
  constructor(op, p1 = 0, p2 = 0, p3 = 0, comment = '') {
    this.op = op;
    this.p1 = p1;
    this.p2 = p2;
    this.p3 = p3;
    this.comment = comment;
  }
}

// ============================================================
// PROGRAM — compiled bytecode + constants
// ============================================================
export class Program {
  constructor() {
    this.instructions = [];
    this.constants = [];
    this._constMap = new Map();
  }

  /** Add a constant and return its index (deduplicates). */
  addConst(value) {
    const key = typeof value === 'string' ? `s:${value}` : `n:${value}`;
    if (this._constMap.has(key)) return this._constMap.get(key);
    const idx = this.constants.length;
    this.constants.push(value);
    this._constMap.set(key, idx);
    return idx;
  }

  /** Add an instruction and return its address. */
  emit(op, p1 = 0, p2 = 0, p3 = 0, comment = '') {
    const addr = this.instructions.length;
    this.instructions.push(new Instruction(op, p1, p2, p3, comment));
    return addr;
  }

  /** Patch a previously emitted instruction's parameter. */
  patch(addr, param, value) {
    this.instructions[addr][param] = value;
  }

  /** Pretty-print the program for debugging. */
  toString() {
    const opNames = {};
    for (const [name, code] of Object.entries(OP)) opNames[code] = name;
    
    let lines = ['addr  opcode        p1   p2   p3   comment'];
    lines.push('-'.repeat(60));
    for (let i = 0; i < this.instructions.length; i++) {
      const ins = this.instructions[i];
      const name = (opNames[ins.op] || `0x${ins.op.toString(16)}`).padEnd(14);
      lines.push(
        `${String(i).padStart(4)}  ${name}${String(ins.p1).padStart(4)} ${String(ins.p2).padStart(4)} ${String(ins.p3).padStart(4)}   ${ins.comment}`
      );
    }
    if (this.constants.length > 0) {
      lines.push('\nConstants:');
      this.constants.forEach((c, i) => lines.push(`  [${i}] = ${JSON.stringify(c)}`));
    }
    return lines.join('\n');
  }
}

// ============================================================
// VM — executes programs
// ============================================================
export class QueryVM {
  constructor(tables = {}) {
    this.tables = tables;  // name → array of row objects
    this.registers = new Array(64).fill(null);
    this.results = [];
    this.aggregates = new Map();  // slot → {type, value, count}
    this.hashTables = new Map();  // slot → Map
    this._cursors = new Map();    // table slot → {data, idx}
    this._hashIterators = new Map(); // slot → iterator state
    this.stats = { instructionsExecuted: 0, rowsScanned: 0, rowsEmitted: 0 };
  }

  /** Execute a program. Returns result rows. */
  execute(program) {
    const { instructions, constants } = program;
    let pc = 0;
    this.results = [];
    this.stats = { instructionsExecuted: 0, rowsScanned: 0, rowsEmitted: 0 };

    while (pc < instructions.length) {
      const ins = instructions[pc];
      this.stats.instructionsExecuted++;
      
      switch (ins.op) {
        case OP.HALT:
          return this.results;

        case OP.GOTO:
          pc = ins.p1;
          continue;

        case OP.IF_TRUE:
          if (this.registers[ins.p1]) { pc = ins.p2; continue; }
          break;

        case OP.IF_FALSE:
          if (!this.registers[ins.p1]) { pc = ins.p2; continue; }
          break;

        case OP.LOAD_CONST:
          this.registers[ins.p1] = constants[ins.p2];
          break;

        case OP.LOAD_NULL:
          this.registers[ins.p1] = null;
          break;

        case OP.MOVE:
          this.registers[ins.p1] = this.registers[ins.p2];
          break;

        case OP.COPY:
          this.registers[ins.p1] = JSON.parse(JSON.stringify(this.registers[ins.p2]));
          break;

        case OP.OPEN_TABLE: {
          const name = constants[ins.p2];
          const data = this.tables[name] || [];
          this._cursors.set(ins.p1, { data, idx: 0 });
          break;
        }

        case OP.NEXT_ROW: {
          const cursor = this._cursors.get(ins.p1);
          if (!cursor || cursor.idx >= cursor.data.length) {
            pc = ins.p3;
            continue;
          }
          this.registers[ins.p2] = cursor.data[cursor.idx++];
          this.stats.rowsScanned++;
          break;
        }

        case OP.COLUMN: {
          const row = this.registers[ins.p3] || this._cursors.get(ins.p3)?.data?.[this._cursors.get(ins.p3)?.idx - 1];
          const colName = constants[ins.p2];
          this.registers[ins.p1] = row ? row[colName] : null;
          break;
        }

        // Arithmetic
        case OP.ADD: this.registers[ins.p1] = this.registers[ins.p2] + this.registers[ins.p3]; break;
        case OP.SUB: this.registers[ins.p1] = this.registers[ins.p2] - this.registers[ins.p3]; break;
        case OP.MUL: this.registers[ins.p1] = this.registers[ins.p2] * this.registers[ins.p3]; break;
        case OP.DIV: this.registers[ins.p1] = this.registers[ins.p3] !== 0 ? this.registers[ins.p2] / this.registers[ins.p3] : null; break;
        case OP.MOD: this.registers[ins.p1] = this.registers[ins.p2] % this.registers[ins.p3]; break;
        case OP.NEG: this.registers[ins.p1] = -this.registers[ins.p2]; break;

        // Comparison
        case OP.EQ: this.registers[ins.p1] = this.registers[ins.p2] === this.registers[ins.p3] ? 1 : 0; break;
        case OP.NE: this.registers[ins.p1] = this.registers[ins.p2] !== this.registers[ins.p3] ? 1 : 0; break;
        case OP.LT: this.registers[ins.p1] = this.registers[ins.p2] < this.registers[ins.p3] ? 1 : 0; break;
        case OP.LE: this.registers[ins.p1] = this.registers[ins.p2] <= this.registers[ins.p3] ? 1 : 0; break;
        case OP.GT: this.registers[ins.p1] = this.registers[ins.p2] > this.registers[ins.p3] ? 1 : 0; break;
        case OP.GE: this.registers[ins.p1] = this.registers[ins.p2] >= this.registers[ins.p3] ? 1 : 0; break;
        case OP.IS_NULL: this.registers[ins.p1] = this.registers[ins.p2] === null ? 1 : 0; break;

        // String
        case OP.CONCAT:
          this.registers[ins.p1] = String(this.registers[ins.p2] ?? '') + String(this.registers[ins.p3] ?? '');
          break;

        // Aggregates
        case OP.AGG_INIT: {
          const type = ins.p2;
          let init;
          switch (type) {
            case AGG.COUNT: init = 0; break;
            case AGG.SUM: init = 0; break;
            case AGG.MIN: init = Infinity; break;
            case AGG.MAX: init = -Infinity; break;
            case AGG.AVG: init = { sum: 0, count: 0 }; break;
          }
          this.aggregates.set(ins.p1, { type, value: init });
          break;
        }

        case OP.AGG_STEP: {
          const agg = this.aggregates.get(ins.p1);
          const val = this.registers[ins.p2];
          if (val === null || val === undefined) break;
          switch (agg.type) {
            case AGG.COUNT: agg.value++; break;
            case AGG.SUM: agg.value += val; break;
            case AGG.MIN: if (val < agg.value) agg.value = val; break;
            case AGG.MAX: if (val > agg.value) agg.value = val; break;
            case AGG.AVG: agg.value.sum += val; agg.value.count++; break;
          }
          break;
        }

        case OP.AGG_FINAL: {
          const agg = this.aggregates.get(ins.p2);
          if (agg.type === AGG.AVG) {
            this.registers[ins.p1] = agg.value.count > 0 ? agg.value.sum / agg.value.count : null;
          } else if (agg.type === AGG.MIN && agg.value === Infinity) {
            this.registers[ins.p1] = null;
          } else if (agg.type === AGG.MAX && agg.value === -Infinity) {
            this.registers[ins.p1] = null;
          } else {
            this.registers[ins.p1] = agg.value;
          }
          break;
        }

        // Hash table
        case OP.HASH_INIT:
          this.hashTables.set(ins.p1, new Map());
          break;

        case OP.HASH_PUT: {
          const ht = this.hashTables.get(ins.p1);
          const key = this.registers[ins.p2];
          // Save current aggregate state for this group
          const state = {};
          for (const [slot, agg] of this.aggregates) {
            state[slot] = JSON.parse(JSON.stringify(agg));
          }
          ht.set(key, state);
          break;
        }

        case OP.HASH_GET: {
          const ht = this.hashTables.get(ins.p1);
          const key = this.registers[ins.p2];
          if (ht.has(key)) {
            // Restore aggregate state for this group
            const state = ht.get(key);
            for (const [slot, agg] of Object.entries(state)) {
              this.aggregates.set(Number(slot), JSON.parse(JSON.stringify(agg)));
            }
          } else {
            // New group — jump to initialization
            pc = ins.p3;
            continue;
          }
          break;
        }

        case OP.HASH_NEXT: {
          const ht = this.hashTables.get(ins.p1);
          if (!this._hashIterators.has(ins.p1)) {
            this._hashIterators.set(ins.p1, ht.entries());
          }
          const iter = this._hashIterators.get(ins.p1);
          const { value, done } = iter.next();
          if (done) {
            this._hashIterators.delete(ins.p1);
            pc = ins.p3;
            continue;
          }
          const [key, state] = value;
          this.registers[ins.p2] = key;
          // Restore aggregate state for this group
          for (const [slot, agg] of Object.entries(state)) {
            this.aggregates.set(Number(slot), JSON.parse(JSON.stringify(agg)));
          }
          break;
        }

        // Output
        case OP.EMIT_ROW: {
          const row = {};
          for (let i = 0; i < ins.p2; i++) {
            row[`col${i}`] = this.registers[ins.p1 + i];
          }
          this.results.push(row);
          this.stats.rowsEmitted++;
          break;
        }

        default:
          throw new Error(`Unknown opcode: 0x${ins.op.toString(16)} at pc=${pc}`);
      }
      
      pc++;
    }

    return this.results;
  }
}

// ============================================================
// COMPILER — compile simple queries to bytecode
// ============================================================
export class QueryCompiler {
  constructor() {
    this._nextReg = 0;
  }

  _allocReg() { return this._nextReg++; }

  /**
   * Compile a simple SELECT with optional WHERE and GROUP BY.
   * Input: {table, columns: [{name, alias}], where: {col, op, value}, groupBy, aggregates}
   */
  compile(query) {
    const prog = new Program();
    this._nextReg = 0;

    if (query.groupBy) {
      return this._compileGroupBy(prog, query);
    }

    if (query.aggregates && query.aggregates.length > 0 && !query.groupBy) {
      return this._compileScalarAgg(prog, query);
    }

    return this._compileSimpleSelect(prog, query);
  }

  _compileSimpleSelect(prog, query) {
    const tableSlot = 0;
    const rowReg = this._allocReg(); // reg for current row
    const tableNameConst = prog.addConst(query.table);

    // Open table
    prog.emit(OP.OPEN_TABLE, tableSlot, tableNameConst, 0, `Open table '${query.table}'`);

    // Loop start
    const loopStart = prog.emit(OP.NEXT_ROW, tableSlot, rowReg, 0, 'Fetch next row');
    const endAddr = loopStart; // Will patch jump target

    // WHERE filter
    let skipAddr = -1;
    if (query.where) {
      const colReg = this._allocReg();
      const valReg = this._allocReg();
      const cmpReg = this._allocReg();
      
      const colConst = prog.addConst(query.where.col);
      const valConst = prog.addConst(query.where.value);

      prog.emit(OP.COLUMN, colReg, colConst, rowReg, `Load ${query.where.col}`);
      prog.emit(OP.LOAD_CONST, valReg, valConst, 0, `Load constant ${query.where.value}`);

      const cmpOp = { '=': OP.EQ, '!=': OP.NE, '<': OP.LT, '<=': OP.LE, '>': OP.GT, '>=': OP.GE }[query.where.op];
      prog.emit(cmpOp, cmpReg, colReg, valReg, `Compare ${query.where.col} ${query.where.op} ${query.where.value}`);
      skipAddr = prog.emit(OP.IF_FALSE, cmpReg, 0, 0, 'Skip if WHERE fails');
    }

    // Project columns
    const firstOutputReg = this._nextReg;
    for (const col of query.columns) {
      const reg = this._allocReg();
      const colConst = prog.addConst(col.name || col);
      prog.emit(OP.COLUMN, reg, colConst, rowReg, `Project ${col.name || col}`);
    }

    // Emit row
    prog.emit(OP.EMIT_ROW, firstOutputReg, query.columns.length, 0, `Emit ${query.columns.length} columns`);

    // Skip target (for WHERE false)
    if (skipAddr >= 0) {
      prog.patch(skipAddr, 'p2', prog.instructions.length);
    }

    // Loop back
    prog.emit(OP.GOTO, loopStart, 0, 0, 'Loop back');

    // End
    const haltAddr = prog.emit(OP.HALT, 0, 0, 0, 'Done');
    prog.patch(loopStart, 'p3', haltAddr);

    return prog;
  }

  _compileScalarAgg(prog, query) {
    const tableSlot = 0;
    const rowReg = this._allocReg();
    const tableNameConst = prog.addConst(query.table);

    // Initialize aggregates
    const aggSlots = [];
    for (let i = 0; i < query.aggregates.length; i++) {
      const agg = query.aggregates[i];
      const slot = i;
      const aggType = AGG[agg.func.toUpperCase()];
      prog.emit(OP.AGG_INIT, slot, aggType, 0, `Init ${agg.func}(${agg.arg})`);
      aggSlots.push(slot);
    }

    // Open and scan
    prog.emit(OP.OPEN_TABLE, tableSlot, tableNameConst, 0, `Open '${query.table}'`);
    const loopStart = prog.emit(OP.NEXT_ROW, tableSlot, rowReg, 0, 'Next row');

    // Feed each value to aggregate
    for (let i = 0; i < query.aggregates.length; i++) {
      const agg = query.aggregates[i];
      if (agg.arg === '*') {
        // COUNT(*): feed a non-null value
        const oneReg = this._allocReg();
        const oneConst = prog.addConst(1);
        prog.emit(OP.LOAD_CONST, oneReg, oneConst, 0, 'Load 1 for COUNT(*)');
        prog.emit(OP.AGG_STEP, aggSlots[i], oneReg, 0, `COUNT(*) step`);
      } else {
        const valReg = this._allocReg();
        const colConst = prog.addConst(agg.arg);
        prog.emit(OP.COLUMN, valReg, colConst, rowReg, `Load ${agg.arg}`);
        prog.emit(OP.AGG_STEP, aggSlots[i], valReg, 0, `${agg.func}(${agg.arg}) step`);
      }
    }

    prog.emit(OP.GOTO, loopStart, 0, 0, 'Loop');

    // After loop: finalize and emit
    const afterLoop = prog.instructions.length;
    prog.patch(loopStart, 'p3', afterLoop);

    const firstOutputReg = this._nextReg;
    for (let i = 0; i < query.aggregates.length; i++) {
      const reg = this._allocReg();
      prog.emit(OP.AGG_FINAL, reg, aggSlots[i], 0, `Finalize ${query.aggregates[i].func}`);
    }

    prog.emit(OP.EMIT_ROW, firstOutputReg, query.aggregates.length, 0, 'Emit aggregate result');
    prog.emit(OP.HALT, 0, 0, 0, 'Done');

    return prog;
  }

  _compileGroupBy(prog, query) {
    const tableSlot = 0;
    const hashSlot = 1;
    const rowReg = this._allocReg();
    const keyReg = this._allocReg();
    const tableNameConst = prog.addConst(query.table);
    const groupColConst = prog.addConst(query.groupBy[0]);

    // Initialize hash table
    prog.emit(OP.HASH_INIT, hashSlot, 0, 0, 'Init hash table for GROUP BY');

    // Open and scan
    prog.emit(OP.OPEN_TABLE, tableSlot, tableNameConst, 0, `Open '${query.table}'`);
    const loopStart = prog.emit(OP.NEXT_ROW, tableSlot, rowReg, 0, 'Next row');

    // Get group key
    prog.emit(OP.COLUMN, keyReg, groupColConst, rowReg, `Load group key ${query.groupBy[0]}`);

    // Check if group exists in hash table
    const hashGetAddr = prog.emit(OP.HASH_GET, hashSlot, keyReg, 0, 'Load existing group or jump to init');

    // Feed values to existing aggregates
    for (let i = 0; i < query.aggregates.length; i++) {
      const agg = query.aggregates[i];
      if (agg.arg === '*') {
        const oneReg = this._allocReg();
        prog.emit(OP.LOAD_CONST, oneReg, prog.addConst(1), 0);
        prog.emit(OP.AGG_STEP, i, oneReg, 0, `${agg.func}(*) step`);
      } else {
        const valReg = this._allocReg();
        prog.emit(OP.COLUMN, valReg, prog.addConst(agg.arg), rowReg, `Load ${agg.arg}`);
        prog.emit(OP.AGG_STEP, i, valReg, 0, `${agg.func}(${agg.arg}) step`);
      }
    }

    // Save state back to hash table
    prog.emit(OP.HASH_PUT, hashSlot, keyReg, 0, 'Save group state');
    prog.emit(OP.GOTO, loopStart, 0, 0, 'Continue scan');

    // New group initialization
    const initGroupAddr = prog.instructions.length;
    prog.patch(hashGetAddr, 'p3', initGroupAddr);

    for (let i = 0; i < query.aggregates.length; i++) {
      const agg = query.aggregates[i];
      prog.emit(OP.AGG_INIT, i, AGG[agg.func.toUpperCase()], 0, `Init ${agg.func} for new group`);
    }

    // Feed first values
    for (let i = 0; i < query.aggregates.length; i++) {
      const agg = query.aggregates[i];
      if (agg.arg === '*') {
        const oneReg = this._allocReg();
        prog.emit(OP.LOAD_CONST, oneReg, prog.addConst(1), 0);
        prog.emit(OP.AGG_STEP, i, oneReg, 0);
      } else {
        const valReg = this._allocReg();
        prog.emit(OP.COLUMN, valReg, prog.addConst(agg.arg), rowReg, `Load ${agg.arg}`);
        prog.emit(OP.AGG_STEP, i, valReg, 0);
      }
    }

    // Save new group to hash table
    prog.emit(OP.HASH_PUT, hashSlot, keyReg, 0, 'Save new group');
    prog.emit(OP.GOTO, loopStart, 0, 0, 'Continue scan');

    // After scan: iterate hash table and emit results
    const afterScan = prog.instructions.length;
    prog.patch(loopStart, 'p3', afterScan);

    const iterStart = prog.emit(OP.HASH_NEXT, hashSlot, keyReg, 0, 'Next group');

    // Finalize aggregates and emit
    const firstOutputReg = this._nextReg;
    const grpReg = this._allocReg();
    prog.emit(OP.MOVE, grpReg, keyReg, 0, 'Group key');

    for (let i = 0; i < query.aggregates.length; i++) {
      const reg = this._allocReg();
      prog.emit(OP.AGG_FINAL, reg, i, 0, `Final ${query.aggregates[i].func}`);
    }

    prog.emit(OP.EMIT_ROW, firstOutputReg, 1 + query.aggregates.length, 0, 'Emit group row');
    prog.emit(OP.GOTO, iterStart, 0, 0, 'Next group');

    // Done
    const haltAddr = prog.emit(OP.HALT, 0, 0, 0, 'Done');
    prog.patch(iterStart, 'p3', haltAddr);

    return prog;
  }
}
