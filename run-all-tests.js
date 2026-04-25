#!/usr/bin/env node
/**
 * Cross-project test runner — runs all test suites across all projects.
 * Usage: node run-all-tests.js [--verbose]
 */

import { execSync } from 'child_process';
import { existsSync, readdirSync } from 'fs';
import { join } from 'path';

const VERBOSE = process.argv.includes('--verbose');
const projectsDir = join(import.meta.dirname, 'projects');

const results = [];

for (const project of readdirSync(projectsDir)) {
  const projectPath = join(projectsDir, project);
  
  // Find test files
  let testGlob;
  if (existsSync(join(projectPath, 'src'))) {
    const srcFiles = readdirSync(join(projectPath, 'src')).filter(f => f.endsWith('.test.js'));
    if (srcFiles.length > 0) testGlob = 'src/*.test.js';
  }
  if (!testGlob) {
    const rootFiles = readdirSync(projectPath).filter(f => f.endsWith('.test.js'));
    if (rootFiles.length > 0) testGlob = '*.test.js';
  }
  
  if (!testGlob) {
    results.push({ project, status: 'skip', reason: 'no tests found' });
    continue;
  }
  
  try {
    const output = execSync(`node --test ${testGlob}`, {
      cwd: projectPath,
      timeout: 120000,
      stdio: ['pipe', 'pipe', 'pipe'],
    }).toString();
    
    const testsMatch = output.match(/# tests (\d+)/);
    const failMatch = output.match(/# fail (\d+)/);
    const tests = testsMatch ? parseInt(testsMatch[1]) : 0;
    const fails = failMatch ? parseInt(failMatch[1]) : 0;
    
    results.push({ project, status: fails === 0 ? 'pass' : 'fail', tests, fails });
  } catch (e) {
    const output = e.stdout?.toString() || '';
    const testsMatch = output.match(/# tests (\d+)/);
    const failMatch = output.match(/# fail (\d+)/);
    results.push({
      project,
      status: 'fail',
      tests: testsMatch ? parseInt(testsMatch[1]) : 0,
      fails: failMatch ? parseInt(failMatch[1]) : 0,
      error: e.message?.substring(0, 100),
    });
  }
}

// Report
console.log('\n=== Cross-Project Test Results ===\n');
let totalTests = 0, totalFails = 0, passing = 0, failing = 0, skipped = 0;

for (const r of results) {
  const icon = r.status === 'pass' ? '✅' : r.status === 'fail' ? '❌' : '⏭️';
  const detail = r.status === 'skip'
    ? r.reason
    : `${r.tests} suites, ${r.fails} failures`;
  console.log(`${icon} ${r.project.padEnd(20)} ${detail}`);
  
  if (r.status === 'pass') { passing++; totalTests += r.tests; }
  else if (r.status === 'fail') { failing++; totalTests += r.tests; totalFails += r.fails; }
  else { skipped++; }
}

console.log(`\n${'─'.repeat(50)}`);
console.log(`Passing: ${passing} | Failing: ${failing} | Skipped: ${skipped}`);
console.log(`Total suites: ${totalTests} | Total failures: ${totalFails}`);
