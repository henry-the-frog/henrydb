#!/usr/bin/env node
// test-runner.js — Run all HenryDB tests with formatted summary report
// Usage: node test-runner.js [--parallel] [--filter pattern]

import { execSync, spawn } from 'child_process';
import { readdirSync, statSync } from 'fs';
import { join, basename } from 'path';

const args = process.argv.slice(2);
const parallel = args.includes('--parallel');
const filterIdx = args.indexOf('--filter');
const filter = filterIdx >= 0 ? args[filterIdx + 1] : null;

const srcDir = new URL('./src/', import.meta.url).pathname;
const testFiles = readdirSync(srcDir)
  .filter(f => f.endsWith('.test.js'))
  .filter(f => !filter || f.includes(filter))
  .sort();

console.log(`\n🐘 HenryDB Test Runner`);
console.log(`${'═'.repeat(70)}`);
console.log(`Found ${testFiles.length} test files${filter ? ` (filter: ${filter})` : ''}`);
console.log(`Mode: ${parallel ? 'parallel' : 'sequential'}\n`);

const results = [];
const t0 = Date.now();

async function runTest(file) {
  const filePath = join(srcDir, file);
  const module = basename(file, '.test.js');
  
  return new Promise((resolve) => {
    const proc = spawn('node', ['--test', filePath], {
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: 30000,
    });
    
    let stdout = '', stderr = '';
    proc.stdout.on('data', d => stdout += d);
    proc.stderr.on('data', d => stderr += d);
    
    proc.on('close', (code) => {
      // Parse results from output
      const passMatch = stdout.match(/# pass (\d+)/);
      const failMatch = stdout.match(/# fail (\d+)/);
      const durationMatch = stdout.match(/# duration_ms ([\d.]+)/);
      
      const pass = passMatch ? parseInt(passMatch[1]) : 0;
      const fail = failMatch ? parseInt(failMatch[1]) : 0;
      const duration = durationMatch ? parseFloat(durationMatch[1]) : 0;
      
      resolve({
        module,
        file,
        pass,
        fail,
        total: pass + fail,
        duration,
        status: code === 0 ? '✅' : fail > 0 ? '❌' : '⚠️',
        error: code !== 0 && fail === 0 ? stderr.substring(0, 200) : null,
      });
    });
    
    proc.on('error', (err) => {
      resolve({
        module, file, pass: 0, fail: 0, total: 0, duration: 0,
        status: '💥', error: err.message,
      });
    });
  });
}

async function runAll() {
  if (parallel) {
    // Run in batches of 8
    const batchSize = 8;
    for (let i = 0; i < testFiles.length; i += batchSize) {
      const batch = testFiles.slice(i, i + batchSize);
      const batchResults = await Promise.all(batch.map(runTest));
      results.push(...batchResults);
      
      // Progress
      const done = Math.min(i + batchSize, testFiles.length);
      process.stdout.write(`\r  Progress: ${done}/${testFiles.length} files...`);
    }
    console.log();
  } else {
    for (let i = 0; i < testFiles.length; i++) {
      const file = testFiles[i];
      process.stdout.write(`\r  [${i+1}/${testFiles.length}] ${file.padEnd(40)}`);
      const result = await runTest(file);
      results.push(result);
    }
    console.log();
  }
  
  const totalTime = Date.now() - t0;
  
  // Print results
  console.log(`\n${'─'.repeat(70)}`);
  console.log(`${'Status'.padEnd(6)} ${'Module'.padEnd(35)} ${'Pass'.padStart(6)} ${'Fail'.padStart(6)} ${'Time'.padStart(8)}`);
  console.log(`${'─'.repeat(70)}`);
  
  // Sort: failures first, then by name
  const sorted = [...results].sort((a, b) => {
    if (a.fail > 0 && b.fail <= 0) return -1;
    if (a.fail <= 0 && b.fail > 0) return 1;
    return a.module.localeCompare(b.module);
  });
  
  for (const r of sorted) {
    const time = r.duration > 1000 ? `${(r.duration/1000).toFixed(1)}s` : `${r.duration.toFixed(0)}ms`;
    const failStr = r.fail > 0 ? `\x1b[31m${String(r.fail).padStart(6)}\x1b[0m` : String(r.fail).padStart(6);
    console.log(`${r.status.padEnd(4)}  ${r.module.padEnd(35)} ${String(r.pass).padStart(6)} ${failStr} ${time.padStart(8)}`);
    if (r.error) console.log(`      └─ ${r.error.substring(0, 60)}`);
  }
  
  console.log(`${'─'.repeat(70)}`);
  
  // Summary
  const totalPass = results.reduce((s, r) => s + r.pass, 0);
  const totalFail = results.reduce((s, r) => s + r.fail, 0);
  const totalTests = totalPass + totalFail;
  const passingFiles = results.filter(r => r.fail === 0 && r.pass > 0).length;
  const failingFiles = results.filter(r => r.fail > 0).length;
  const errorFiles = results.filter(r => r.status === '💥' || r.status === '⚠️').length;
  
  console.log(`\n📊 Summary`);
  console.log(`  Files:  ${testFiles.length} total, ${passingFiles} passing, ${failingFiles} failing${errorFiles ? `, ${errorFiles} errors` : ''}`);
  console.log(`  Tests:  ${totalTests} total, \x1b[32m${totalPass} passing\x1b[0m${totalFail > 0 ? `, \x1b[31m${totalFail} failing\x1b[0m` : ''}`);
  console.log(`  Time:   ${(totalTime / 1000).toFixed(1)}s`);
  console.log(`  Rate:   ${(totalTests / (totalTime / 1000)).toFixed(0)} tests/sec`);
  
  // Category breakdown
  const categories = {};
  for (const r of results) {
    let cat = 'other';
    if (r.module.match(/^(btree|bplus|art|skip|trie|bloom|bitmap|hash|rtree|inverted|gist)/)) cat = 'indexes';
    else if (r.module.match(/^(mvcc|lock|deadlock|occ|ssi|two-phase)/)) cat = 'concurrency';
    else if (r.module.match(/^(wal|aries|buffer|page|heap|lsm|slotted|disk)/)) cat = 'storage';
    else if (r.module.match(/^(server|pg-|connection|pool|replication|tls)/)) cat = 'server';
    else if (r.module.match(/^(sql|window|cte|aggregate|subquery|expression)/)) cat = 'sql';
    else if (r.module.match(/^(volcano|compiled|vectorized|codegen|adaptive|pipeline)/)) cat = 'engines';
    else if (r.module.match(/^(join|sort-merge|grace|radix|band|theta|nested)/)) cat = 'joins';
    else if (r.module.match(/^(raft|distributed|consistent|gossip|crdt|vector-clock)/)) cat = 'distributed';
    else if (r.module.match(/^(integration|benchmark|optimizer|stress)/)) cat = 'integration';
    else if (r.module.match(/^(column|compress|string-intern)/)) cat = 'columnar';
    
    if (!categories[cat]) categories[cat] = { pass: 0, fail: 0, files: 0 };
    categories[cat].pass += r.pass;
    categories[cat].fail += r.fail;
    categories[cat].files++;
  }
  
  console.log(`\n📁 By Category`);
  for (const [cat, data] of Object.entries(categories).sort((a, b) => b[1].pass - a[1].pass)) {
    const bar = '█'.repeat(Math.ceil(data.pass / 20));
    console.log(`  ${cat.padEnd(14)} ${String(data.pass).padStart(5)} pass ${data.fail > 0 ? `${data.fail} fail ` : ''}(${data.files} files) ${bar}`);
  }
  
  console.log(`\n${totalFail === 0 ? '🎉 All tests passing!' : `⚠️  ${totalFail} test(s) failing`}\n`);
  
  process.exit(totalFail > 0 ? 1 : 0);
}

runAll().catch(err => {
  console.error('Runner error:', err);
  process.exit(1);
});
