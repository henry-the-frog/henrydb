#!/usr/bin/env node

/**
 * Lambda Calculus PL Theory Library — Test Runner
 * 
 * Discovers and runs all test files, reporting per-module results.
 * Usage: node run-tests.js [--verbose] [--filter pattern]
 */

import { readdirSync } from 'fs';
import { execSync } from 'child_process';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

const verbose = process.argv.includes('--verbose');
const filterIdx = process.argv.indexOf('--filter');
const filter = filterIdx !== -1 ? process.argv[filterIdx + 1] : null;

// Discover test files
const files = readdirSync(__dirname)
  .filter(f => f.endsWith('.test.js'))
  .filter(f => !filter || f.includes(filter))
  .sort();

console.log(`\n🔬 Lambda Calculus PL Theory Library — Test Runner`);
console.log(`   ${files.length} test files found${filter ? ` (filter: ${filter})` : ''}\n`);

let totalPassed = 0;
let totalFailed = 0;
let moduleResults = [];

for (const file of files) {
  const moduleName = file.replace('.test.js', '');
  try {
    const output = execSync(`node ${join(__dirname, file)}`, {
      timeout: 30000,
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe']
    });
    
    // Parse results from output
    let passed = 0, total = 0;
    
    // Custom reporter format: "N/M passed"
    const customMatch = output.match(/(\d+)\/(\d+) passed/);
    if (customMatch) {
      passed = parseInt(customMatch[1]);
      total = parseInt(customMatch[2]);
    } else {
      // TAP format: count "ok" lines
      passed = (output.match(/^ok /gm) || []).length;
      const failed = (output.match(/^not ok /gm) || []).length;
      total = passed + failed;
    }
    
    totalPassed += passed;
    const status = passed === total ? '✅' : '❌';
    moduleResults.push({ name: moduleName, passed, total, status, ok: passed === total });
    
    if (verbose) {
      console.log(`${status} ${moduleName}: ${passed}/${total}`);
    }
  } catch (e) {
    totalFailed++;
    moduleResults.push({ name: moduleName, passed: 0, total: 0, status: '💥', ok: false });
    console.log(`💥 ${moduleName}: CRASHED`);
    if (verbose) console.log(`   ${e.message.split('\n')[0]}`);
  }
}

// Summary
console.log('\n' + '═'.repeat(50));
console.log(`📊 Results: ${moduleResults.filter(r => r.ok).length}/${moduleResults.length} modules passing`);
console.log(`   Total tests: ${totalPassed} passed`);
if (totalFailed > 0) console.log(`   ❌ ${totalFailed} modules crashed`);

// Table for non-verbose
if (!verbose) {
  console.log('\n' + moduleResults.map(r => `${r.status} ${r.name} (${r.passed}/${r.total})`).join('\n'));
}

console.log('\n' + '═'.repeat(50));
const allOk = moduleResults.every(r => r.ok);
console.log(allOk ? '🎉 All tests pass!' : '❌ Some tests failed');
process.exit(allOk ? 0 : 1);
