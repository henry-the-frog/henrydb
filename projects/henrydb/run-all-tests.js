// run-all-tests.js — Run all test files in parallel batches and report results
import { execSync } from 'child_process';
import { readdirSync } from 'fs';

const files = readdirSync('src').filter(f => f.endsWith('.test.js')).sort();
const TIMEOUT = 10000; // 10s per test
const BATCH = 20; // Run 20 in parallel

let ok = 0, fail = 0, err = 0, timeout = 0;
let totalPass = 0, totalFail = 0;
const failures = [];
const timeouts = [];

console.log(`Running ${files.length} test files in batches of ${BATCH}...`);
const t0 = Date.now();

for (let i = 0; i < files.length; i += BATCH) {
  const batch = files.slice(i, i + BATCH);
  const results = batch.map(f => {
    try {
      const out = execSync(`node src/${f} 2>&1`, { timeout: TIMEOUT }).toString();
      const passMatch = out.match(/# pass (\d+)/);
      const failMatch = out.match(/# fail (\d+)/);
      const customMatch = out.match(/(\d+) passed, (\d+) failed/);
      
      let p = 0, fl = 0;
      if (passMatch) p = parseInt(passMatch[1]);
      if (failMatch) fl = parseInt(failMatch[1]);
      if (customMatch) { p = parseInt(customMatch[1]); fl = parseInt(customMatch[2]); }
      
      return { file: f, pass: p, fail: fl, status: fl > 0 ? 'FAIL' : 'OK' };
    } catch (e) {
      if (e.killed) return { file: f, pass: 0, fail: 0, status: 'TIMEOUT' };
      const out = (e.stdout?.toString() || '') + (e.stderr?.toString() || '');
      const passMatch = out.match(/# pass (\d+)/);
      const failMatch = out.match(/# fail (\d+)/);
      const customMatch = out.match(/(\d+) passed, (\d+) failed/);
      
      let p = 0, fl = 0;
      if (passMatch) p = parseInt(passMatch[1]);
      if (failMatch) fl = parseInt(failMatch[1]);
      if (customMatch) { p = parseInt(customMatch[1]); fl = parseInt(customMatch[2]); }
      
      if (fl > 0) return { file: f, pass: p, fail: fl, status: 'FAIL' };
      return { file: f, pass: 0, fail: 0, status: 'ERR' };
    }
  });
  
  for (const r of results) {
    totalPass += r.pass;
    totalFail += r.fail;
    if (r.status === 'OK') ok++;
    else if (r.status === 'FAIL') { fail++; failures.push(`${r.file}: ${r.pass}p/${r.fail}f`); }
    else if (r.status === 'TIMEOUT') { timeout++; timeouts.push(r.file); }
    else err++;
  }
  
  if ((i + BATCH) % 100 === 0 || i + BATCH >= files.length) {
    console.log(`  ${Math.min(i + BATCH, files.length)}/${files.length} done...`);
  }
}

const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
console.log(`\n=== RESULTS (${elapsed}s) ===`);
console.log(`Files: ${ok} OK, ${fail} FAIL, ${err} ERR, ${timeout} TIMEOUT`);
console.log(`Tests: ${totalPass} passed, ${totalFail} failed`);
if (failures.length) {
  console.log(`\nFailing files:`);
  for (const f of failures) console.log(`  ${f}`);
}
if (timeouts.length) {
  console.log(`\nTimed out:`);
  for (const f of timeouts) console.log(`  ${f}`);
}
