// query-vm.test.js — Tests for the query bytecode VM
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { OP, AGG, Program, Instruction, QueryVM, QueryCompiler } from './query-vm.js';

describe('QueryVM — Direct Bytecode', () => {
  it('executes LOAD_CONST + EMIT_ROW', () => {
    const prog = new Program();
    prog.emit(OP.LOAD_CONST, 0, prog.addConst(42));
    prog.emit(OP.LOAD_CONST, 1, prog.addConst('hello'));
    prog.emit(OP.EMIT_ROW, 0, 2);
    prog.emit(OP.HALT);

    const vm = new QueryVM();
    const results = vm.execute(prog);
    assert.equal(results.length, 1);
    assert.equal(results[0].col0, 42);
    assert.equal(results[0].col1, 'hello');
  });

  it('arithmetic operations', () => {
    const prog = new Program();
    prog.emit(OP.LOAD_CONST, 0, prog.addConst(10));
    prog.emit(OP.LOAD_CONST, 1, prog.addConst(3));
    prog.emit(OP.ADD, 2, 0, 1);
    prog.emit(OP.SUB, 3, 0, 1);
    prog.emit(OP.MUL, 4, 0, 1);
    prog.emit(OP.DIV, 5, 0, 1);
    prog.emit(OP.MOD, 6, 0, 1);
    prog.emit(OP.NEG, 7, 0);
    prog.emit(OP.EMIT_ROW, 2, 6);
    prog.emit(OP.HALT);

    const vm = new QueryVM();
    const results = vm.execute(prog);
    assert.equal(results[0].col0, 13);    // 10 + 3
    assert.equal(results[0].col1, 7);     // 10 - 3
    assert.equal(results[0].col2, 30);    // 10 * 3
    assert.ok(Math.abs(results[0].col3 - 10/3) < 0.001); // 10 / 3
    assert.equal(results[0].col4, 1);     // 10 % 3
    assert.equal(results[0].col5, -10);   // -10
  });

  it('comparison operations', () => {
    const prog = new Program();
    prog.emit(OP.LOAD_CONST, 0, prog.addConst(5));
    prog.emit(OP.LOAD_CONST, 1, prog.addConst(10));
    prog.emit(OP.LT, 2, 0, 1);   // 5 < 10 = 1
    prog.emit(OP.GT, 3, 0, 1);   // 5 > 10 = 0
    prog.emit(OP.EQ, 4, 0, 0);   // 5 == 5 = 1
    prog.emit(OP.EMIT_ROW, 2, 3);
    prog.emit(OP.HALT);

    const vm = new QueryVM();
    const results = vm.execute(prog);
    assert.equal(results[0].col0, 1);
    assert.equal(results[0].col1, 0);
    assert.equal(results[0].col2, 1);
  });

  it('table scan with full projection', () => {
    const prog = new Program();
    prog.emit(OP.OPEN_TABLE, 0, prog.addConst('users'));
    const loop = prog.emit(OP.NEXT_ROW, 0, 0, 0);
    prog.emit(OP.COLUMN, 1, prog.addConst('name'), 0);
    prog.emit(OP.COLUMN, 2, prog.addConst('age'), 0);
    prog.emit(OP.EMIT_ROW, 1, 2);
    prog.emit(OP.GOTO, loop);
    const halt = prog.emit(OP.HALT);
    prog.patch(loop, 'p3', halt);

    const vm = new QueryVM({
      users: [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
        { name: 'Charlie', age: 35 },
      ],
    });
    const results = vm.execute(prog);
    assert.equal(results.length, 3);
    assert.equal(results[0].col0, 'Alice');
    assert.equal(results[1].col1, 25);
  });

  it('conditional branching (IF_FALSE for WHERE)', () => {
    const prog = new Program();
    prog.emit(OP.OPEN_TABLE, 0, prog.addConst('items'));
    const loop = prog.emit(OP.NEXT_ROW, 0, 0, 0);
    prog.emit(OP.COLUMN, 1, prog.addConst('price'), 0);
    prog.emit(OP.LOAD_CONST, 2, prog.addConst(50));
    prog.emit(OP.GT, 3, 1, 2);   // price > 50?
    const skip = prog.emit(OP.IF_FALSE, 3, 0);
    prog.emit(OP.COLUMN, 4, prog.addConst('name'), 0);
    prog.emit(OP.EMIT_ROW, 4, 1);
    prog.patch(skip, 'p2', prog.instructions.length);
    prog.emit(OP.GOTO, loop);
    const halt = prog.emit(OP.HALT);
    prog.patch(loop, 'p3', halt);

    const vm = new QueryVM({
      items: [
        { name: 'cheap', price: 10 },
        { name: 'mid', price: 50 },
        { name: 'expensive', price: 100 },
        { name: 'premium', price: 200 },
      ],
    });
    const results = vm.execute(prog);
    assert.equal(results.length, 2); // expensive + premium
    assert.equal(results[0].col0, 'expensive');
    assert.equal(results[1].col0, 'premium');
  });

  it('scalar aggregate (COUNT + SUM)', () => {
    const prog = new Program();
    prog.emit(OP.AGG_INIT, 0, AGG.COUNT);
    prog.emit(OP.AGG_INIT, 1, AGG.SUM);
    prog.emit(OP.OPEN_TABLE, 0, prog.addConst('sales'));
    const loop = prog.emit(OP.NEXT_ROW, 0, 0, 0);
    const oneConst = prog.addConst(1);
    prog.emit(OP.LOAD_CONST, 1, oneConst);
    prog.emit(OP.AGG_STEP, 0, 1);
    prog.emit(OP.COLUMN, 2, prog.addConst('amount'), 0);
    prog.emit(OP.AGG_STEP, 1, 2);
    prog.emit(OP.GOTO, loop);
    const afterLoop = prog.instructions.length;
    prog.patch(loop, 'p3', afterLoop);
    prog.emit(OP.AGG_FINAL, 3, 0);
    prog.emit(OP.AGG_FINAL, 4, 1);
    prog.emit(OP.EMIT_ROW, 3, 2);
    prog.emit(OP.HALT);

    const vm = new QueryVM({
      sales: [
        { amount: 100 }, { amount: 200 }, { amount: 300 },
      ],
    });
    const results = vm.execute(prog);
    assert.equal(results.length, 1);
    assert.equal(results[0].col0, 3);     // COUNT
    assert.equal(results[0].col1, 600);   // SUM
  });
});

describe('QueryCompiler', () => {
  const compiler = new QueryCompiler();

  it('compiles and executes simple SELECT', () => {
    const prog = compiler.compile({
      table: 'employees',
      columns: [{ name: 'name' }, { name: 'salary' }],
    });

    const vm = new QueryVM({
      employees: [
        { name: 'Alice', salary: 100000 },
        { name: 'Bob', salary: 90000 },
      ],
    });
    const results = vm.execute(prog);
    assert.equal(results.length, 2);
    assert.equal(results[0].col0, 'Alice');
    assert.equal(results[1].col1, 90000);
  });

  it('compiles SELECT with WHERE', () => {
    const prog = compiler.compile({
      table: 'products',
      columns: [{ name: 'name' }],
      where: { col: 'price', op: '>', value: 50 },
    });

    const vm = new QueryVM({
      products: [
        { name: 'A', price: 30 },
        { name: 'B', price: 60 },
        { name: 'C', price: 90 },
      ],
    });
    const results = vm.execute(prog);
    assert.equal(results.length, 2);
    assert.equal(results[0].col0, 'B');
  });

  it('compiles scalar aggregates', () => {
    const prog = compiler.compile({
      table: 'orders',
      columns: [],
      aggregates: [
        { func: 'COUNT', arg: '*' },
        { func: 'SUM', arg: 'total' },
        { func: 'AVG', arg: 'total' },
      ],
    });

    const vm = new QueryVM({
      orders: [
        { total: 100 }, { total: 200 }, { total: 300 }, { total: 400 },
      ],
    });
    const results = vm.execute(prog);
    assert.equal(results.length, 1);
    assert.equal(results[0].col0, 4);     // COUNT(*)
    assert.equal(results[0].col1, 1000);  // SUM(total)
    assert.equal(results[0].col2, 250);   // AVG(total)
  });

  it('compiles GROUP BY with aggregates', () => {
    const prog = compiler.compile({
      table: 'employees',
      columns: [],
      groupBy: ['dept'],
      aggregates: [
        { func: 'COUNT', arg: '*' },
        { func: 'SUM', arg: 'salary' },
      ],
    });

    const vm = new QueryVM({
      employees: [
        { dept: 'Eng', salary: 100 },
        { dept: 'Sales', salary: 80 },
        { dept: 'Eng', salary: 120 },
        { dept: 'Sales', salary: 90 },
        { dept: 'Eng', salary: 110 },
      ],
    });
    const results = vm.execute(prog);
    assert.equal(results.length, 2);
    
    const eng = results.find(r => r.col0 === 'Eng');
    const sales = results.find(r => r.col0 === 'Sales');
    
    assert.equal(eng.col1, 3);     // COUNT
    assert.equal(eng.col2, 330);   // SUM
    assert.equal(sales.col1, 2);
    assert.equal(sales.col2, 170);
  });

  it('program toString produces readable output', () => {
    const prog = compiler.compile({
      table: 'test',
      columns: [{ name: 'id' }],
      where: { col: 'active', op: '=', value: 1 },
    });
    const str = prog.toString();
    assert.ok(str.includes('OPEN_TABLE'));
    assert.ok(str.includes('NEXT_ROW'));
    assert.ok(str.includes('HALT'));
    assert.ok(str.includes('EQ'));
  });
});

describe('Performance — VM vs Direct', () => {
  const N = 100_000;
  const data = [];
  for (let i = 0; i < N; i++) {
    data.push({ dept: ['Eng', 'Sales', 'HR', 'Ops', 'Finance'][i % 5], salary: (Math.random() * 100000) | 0 });
  }

  it('benchmark: compiled GROUP BY on 100K rows', () => {
    const compiler = new QueryCompiler();
    const prog = compiler.compile({
      table: 'emp',
      columns: [],
      groupBy: ['dept'],
      aggregates: [{ func: 'SUM', arg: 'salary' }, { func: 'COUNT', arg: '*' }],
    });

    const t0 = performance.now();
    const vm = new QueryVM({ emp: data });
    const results = vm.execute(prog);
    const elapsed = performance.now() - t0;

    console.log(`    VM GROUP BY: ${results.length} groups from ${N} rows in ${elapsed.toFixed(1)}ms`);
    console.log(`    Instructions executed: ${vm.stats.instructionsExecuted}`);
    console.log(`    Rows scanned: ${vm.stats.rowsScanned}`);
    assert.equal(results.length, 5);
  });

  it('benchmark: direct JS GROUP BY on 100K rows', () => {
    const t0 = performance.now();
    const groups = new Map();
    for (const row of data) {
      const key = row.dept;
      if (!groups.has(key)) groups.set(key, { sum: 0, count: 0 });
      const g = groups.get(key);
      g.sum += row.salary;
      g.count++;
    }
    const elapsed = performance.now() - t0;
    console.log(`    Direct JS: ${groups.size} groups from ${N} rows in ${elapsed.toFixed(1)}ms`);
    assert.equal(groups.size, 5);
  });

  it('benchmark: compiled WHERE filter on 100K rows', () => {
    const compiler = new QueryCompiler();
    const prog = compiler.compile({
      table: 'emp',
      columns: [{ name: 'dept' }, { name: 'salary' }],
      where: { col: 'salary', op: '>', value: 50000 },
    });

    const t0 = performance.now();
    const vm = new QueryVM({ emp: data });
    const results = vm.execute(prog);
    const elapsed = performance.now() - t0;

    console.log(`    VM WHERE filter: ${results.length} rows from ${N} in ${elapsed.toFixed(1)}ms`);
    assert.ok(results.length > 0);
    assert.ok(results.length < N);
  });
});
