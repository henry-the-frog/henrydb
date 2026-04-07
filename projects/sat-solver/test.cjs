#!/usr/bin/env node
'use strict';

// Run all tests
const { execSync } = require('child_process');
const path = require('path');

const tests = [
  'src/solver.test.cjs',
  'src/smt.test.cjs',
];

let totalPassed = 0, totalFailed = 0;

for (const test of tests) {
  console.log(`\n=== ${test} ===`);
  try {
    const output = execSync(`node ${test}`, { cwd: __dirname, encoding: 'utf8', timeout: 120000 });
    console.log(output);
    const match = output.match(/(\d+) passed, (\d+) failed/);
    if (match) {
      totalPassed += parseInt(match[1]);
      totalFailed += parseInt(match[2]);
    }
  } catch (e) {
    console.log(e.stdout || '');
    console.error(e.stderr || '');
    totalFailed++;
  }
}

console.log(`\n${'='.repeat(40)}`);
console.log(`TOTAL: ${totalPassed} passed, ${totalFailed} failed`);
if (totalFailed > 0) process.exit(1);
