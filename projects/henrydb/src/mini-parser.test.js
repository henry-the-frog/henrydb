// mini-parser.test.js — Tests for the lightweight SQL parser
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { parseSQL, tokenize } from './mini-parser.js';
import { QueryCompiler, QueryVM } from './query-vm.js';

describe('Tokenizer', () => {
  it('tokenizes simple SELECT', () => {
    const tokens = tokenize("SELECT name, age FROM users");
    assert.equal(tokens.length, 6); // SELECT name , age FROM users
    assert.equal(tokens[0].type, 'KEYWORD');
    assert.equal(tokens[1].type, 'IDENT');
  });

  it('tokenizes numbers and strings', () => {
    const tokens = tokenize("WHERE age > 25 AND name = 'Alice'");
    const numToken = tokens.find(t => t.type === 'NUMBER');
    assert.equal(numToken.value, 25);
    const strToken = tokens.find(t => t.type === 'STRING');
    assert.equal(strToken.value, 'Alice');
  });

  it('tokenizes operators', () => {
    const tokens = tokenize("a >= 10 AND b != 5 AND c <= 3");
    const ops = tokens.filter(t => t.type === 'OP');
    assert.equal(ops[0].value, '>=');
    assert.equal(ops[1].value, '!=');
    assert.equal(ops[2].value, '<=');
  });

  it('tokenizes aggregate functions', () => {
    const tokens = tokenize("SELECT COUNT(*), SUM(salary) FROM emp");
    assert.ok(tokens.some(t => t.value === 'COUNT'));
    assert.ok(tokens.some(t => t.value === 'SUM'));
  });
});

describe('Parser — SELECT', () => {
  it('simple SELECT columns FROM table', () => {
    const ast = parseSQL("SELECT name, age FROM users");
    assert.equal(ast.table, 'users');
    assert.equal(ast.columns.length, 2);
    assert.equal(ast.columns[0].name, 'name');
    assert.equal(ast.columns[1].name, 'age');
  });

  it('SELECT with WHERE', () => {
    const ast = parseSQL("SELECT name FROM users WHERE age > 25");
    assert.equal(ast.where.col, 'age');
    assert.equal(ast.where.op, '>');
    assert.equal(ast.where.value, 25);
  });

  it('SELECT with string WHERE', () => {
    const ast = parseSQL("SELECT * FROM products WHERE category = 'electronics'");
    assert.equal(ast.where.col, 'category');
    assert.equal(ast.where.value, 'electronics');
  });

  it('SELECT with GROUP BY', () => {
    const ast = parseSQL("SELECT dept, COUNT(*) AS cnt FROM emp GROUP BY dept");
    assert.deepEqual(ast.groupBy, ['dept']);
    assert.equal(ast.aggregates.length, 1);
    assert.equal(ast.aggregates[0].func, 'COUNT');
    assert.equal(ast.aggregates[0].arg, '*');
    assert.equal(ast.aggregates[0].alias, 'cnt');
  });

  it('SELECT with ORDER BY', () => {
    const ast = parseSQL("SELECT name FROM users ORDER BY name DESC");
    assert.equal(ast.orderBy.column, 'name');
    assert.equal(ast.orderBy.descending, true);
  });

  it('SELECT with LIMIT', () => {
    const ast = parseSQL("SELECT name FROM users LIMIT 10");
    assert.equal(ast.limit, 10);
  });

  it('SELECT with multiple aggregates', () => {
    const ast = parseSQL("SELECT dept, SUM(salary) AS total, AVG(salary) AS avg_sal, COUNT(*) AS cnt FROM emp GROUP BY dept");
    assert.equal(ast.aggregates.length, 3);
    assert.equal(ast.aggregates[0].func, 'SUM');
    assert.equal(ast.aggregates[1].func, 'AVG');
    assert.equal(ast.aggregates[2].func, 'COUNT');
  });
});

describe('Parser → Compiler → VM (end-to-end)', () => {
  const employees = [
    { name: 'Alice', dept: 'Eng', salary: 100 },
    { name: 'Bob', dept: 'Sales', salary: 80 },
    { name: 'Charlie', dept: 'Eng', salary: 120 },
    { name: 'Diana', dept: 'Sales', salary: 90 },
    { name: 'Eve', dept: 'Eng', salary: 110 },
  ];

  it('SELECT name FROM emp WHERE salary > 100', () => {
    const ast = parseSQL("SELECT name FROM emp WHERE salary > 100");
    const compiler = new QueryCompiler();
    const prog = compiler.compile(ast);
    const vm = new QueryVM({ emp: employees });
    const results = vm.execute(prog);
    
    assert.equal(results.length, 2); // Charlie (120) and Eve (110)
    assert.equal(results[0].col0, 'Charlie');
    assert.equal(results[1].col0, 'Eve');
  });

  it('SELECT dept, SUM(salary) FROM emp GROUP BY dept', () => {
    const ast = parseSQL("SELECT dept, SUM(salary) AS total, COUNT(*) AS cnt FROM emp GROUP BY dept");
    const compiler = new QueryCompiler();
    const prog = compiler.compile(ast);
    const vm = new QueryVM({ emp: employees });
    const results = vm.execute(prog);
    
    assert.equal(results.length, 2);
    const eng = results.find(r => r.col0 === 'Eng');
    const sales = results.find(r => r.col0 === 'Sales');
    assert.equal(eng.col1, 330);  // SUM
    assert.equal(eng.col2, 3);    // COUNT
    assert.equal(sales.col1, 170);
    assert.equal(sales.col2, 2);
  });

  it('SELECT name, salary FROM emp (full scan)', () => {
    const ast = parseSQL("SELECT name, salary FROM emp");
    const compiler = new QueryCompiler();
    const prog = compiler.compile(ast);
    const vm = new QueryVM({ emp: employees });
    const results = vm.execute(prog);
    
    assert.equal(results.length, 5);
    assert.equal(results[0].col0, 'Alice');
    assert.equal(results[0].col1, 100);
  });

  it('scalar aggregate: COUNT, SUM, AVG', () => {
    const ast = parseSQL("SELECT COUNT(*) AS cnt, SUM(salary) AS total, AVG(salary) AS avg_sal FROM emp");
    const compiler = new QueryCompiler();
    const prog = compiler.compile(ast);
    const vm = new QueryVM({ emp: employees });
    const results = vm.execute(prog);
    
    assert.equal(results.length, 1);
    assert.equal(results[0].col0, 5);    // COUNT
    assert.equal(results[0].col1, 500);  // SUM
    assert.equal(results[0].col2, 100);  // AVG
  });
});
