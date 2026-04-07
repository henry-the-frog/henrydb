#!/usr/bin/env node
'use strict';

const { Solver, createSolver, encodeSudoku, encodeNQueens, encodeGraphColoring, encodePigeonhole, parseDIMACS, randomSAT } = require('./src/solver.cjs');
const { SMTSolver, parseSmtExpr } = require('./src/smt.cjs');
const readline = require('readline');

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
const prompt = (q) => new Promise(r => rl.question(q, r));

function printHelp() {
  console.log(`
SAT/SMT Solver — Interactive CLI

Commands:
  sudoku          Solve a Sudoku puzzle (enter 81 digits, 0=empty)
  queens <n>      Solve N-Queens problem
  pigeonhole <n>  Test pigeonhole principle (n+1 pigeons, n holes)
  color <nodes> <edges> <colors>  Graph coloring
  random <vars> <clauses>         Random 3-SAT
  dimacs <text>   Parse and solve DIMACS CNF
  smt             Enter SMT mode (S-expressions)
  help            Show this help
  quit            Exit
`);
}

function formatBoard(grid) {
  const lines = [];
  for (let r = 0; r < 9; r++) {
    if (r % 3 === 0 && r > 0) lines.push('------+-------+------');
    const row = [];
    for (let c = 0; c < 9; c++) {
      if (c % 3 === 0 && c > 0) row.push('|');
      row.push(grid[r][c] || '.');
    }
    lines.push(row.join(' '));
  }
  return lines.join('\n');
}

async function solveSudoku() {
  console.log('Enter Sudoku (81 digits, 0=empty, on one or multiple lines):');
  let digits = '';
  while (digits.replace(/[^0-9]/g, '').length < 81) {
    const line = await prompt('');
    digits += line;
  }
  const nums = digits.replace(/[^0-9]/g, '').slice(0, 81).split('').map(Number);
  const grid = [];
  for (let r = 0; r < 9; r++) {
    grid.push(nums.slice(r * 9, r * 9 + 9));
  }

  console.log('\nInput:');
  console.log(formatBoard(grid));

  const problem = encodeSudoku(grid);
  const solver = createSolver(problem);
  const start = Date.now();
  const result = solver.solve();
  const elapsed = Date.now() - start;

  if (result === 'SAT') {
    const solution = problem.decode(solver.getModel());
    console.log(`\nSolution (${elapsed}ms):`);
    console.log(formatBoard(solution));
  } else {
    console.log(`\nNo solution (${elapsed}ms)`);
  }
  console.log(`Stats: ${solver.conflicts} conflicts, ${solver.decisions} decisions, ${solver.propagations} propagations`);
}

async function solveQueens(n) {
  const problem = encodeNQueens(n);
  const solver = createSolver(problem);
  const start = Date.now();
  const result = solver.solve();
  const elapsed = Date.now() - start;

  if (result === 'SAT') {
    const queens = problem.decode(solver.getModel());
    console.log(`${n}-Queens solution (${elapsed}ms):`);
    for (let r = 0; r < n; r++) {
      const row = new Array(n).fill('.');
      for (const [qr, qc] of queens) {
        if (qr === r) row[qc] = 'Q';
      }
      console.log(row.join(' '));
    }
  } else {
    console.log(`${n}-Queens: no solution (${elapsed}ms)`);
  }
  console.log(`Stats: ${solver.conflicts} conflicts, ${solver.decisions} decisions`);
}

async function main() {
  console.log('SAT/SMT Solver v1.0 — Type "help" for commands\n');

  while (true) {
    const input = await prompt('sat> ');
    const parts = input.trim().split(/\s+/);
    const cmd = parts[0]?.toLowerCase();

    if (!cmd) continue;
    if (cmd === 'quit' || cmd === 'exit' || cmd === 'q') break;
    if (cmd === 'help') { printHelp(); continue; }

    if (cmd === 'sudoku') {
      await solveSudoku();
    } else if (cmd === 'queens') {
      const n = parseInt(parts[1]) || 8;
      await solveQueens(n);
    } else if (cmd === 'pigeonhole') {
      const n = parseInt(parts[1]) || 4;
      const problem = encodePigeonhole(n);
      const solver = createSolver(problem);
      const start = Date.now();
      const result = solver.solve();
      console.log(`Pigeonhole(${n+1}→${n}): ${result} (${Date.now()-start}ms, ${solver.conflicts} conflicts)`);
    } else if (cmd === 'random') {
      const vars = parseInt(parts[1]) || 30;
      const clauses = parseInt(parts[2]) || Math.floor(vars * 4.27);
      const problem = randomSAT(vars, clauses);
      const solver = createSolver(problem);
      const start = Date.now();
      const result = solver.solve();
      console.log(`Random ${vars}v/${clauses}c: ${result} (${Date.now()-start}ms, ${solver.conflicts} conflicts)`);
    } else if (cmd === 'smt') {
      console.log('SMT mode. Enter assertions as S-expressions. Type "check" to check, "done" to exit.');
      const smt = new SMTSolver();
      while (true) {
        const line = await prompt('smt> ');
        if (line.trim() === 'done') break;
        if (line.trim() === 'check') {
          console.log(smt.checkSat());
          continue;
        }
        const exprs = parseSmtExpr(line);
        for (const expr of exprs) smt.assert(expr);
      }
    } else {
      console.log(`Unknown command: ${cmd}. Type "help" for available commands.`);
    }
  }

  rl.close();
}

main().catch(console.error);
