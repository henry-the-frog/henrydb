// Run a batch of tests, save results to disk, exit cleanly
const {execSync} = require('child_process');
const {readdirSync, writeFileSync, readFileSync, existsSync} = require('fs');

const files = readdirSync('src').filter(f => f.endsWith('.test.js')).sort();
const batchSize = 50;
const resultFile = '/tmp/henrydb-results.json';

let state = existsSync(resultFile) ? JSON.parse(readFileSync(resultFile,'utf8')) : {pass:0,fail:0,errs:[],timeouts:[],processed:0};
const start = state.processed;
const end = Math.min(start + batchSize, files.length);

if (start >= files.length) {
  console.log('ALL DONE: '+JSON.stringify({pass:state.pass,fail:state.fail,errs:state.errs.length,timeouts:state.timeouts.length}));
  if(state.errs.length) state.errs.forEach(e=>console.log('  '+e));
  if(state.timeouts.length) { console.log('Timeouts:'); state.timeouts.forEach(t=>console.log('  '+t)); }
  process.exit(0);
}

for (let i = start; i < end; i++) {
  const f = files[i];
  try {
    const out = execSync('node src/'+f+' 2>&1', {timeout:15000, maxBuffer:1024*1024}).toString();
    const pm = out.match(/# pass (\d+)/);
    const fm = out.match(/# fail (\d+)/);
    let p=0,fl=0;
    if(pm) p=parseInt(pm[1]);
    if(fm) fl=parseInt(fm[1]);
    const cm = out.match(/(\d+) passed, (\d+) failed/);
    if(cm){p=parseInt(cm[1]);fl=parseInt(cm[2]);}
    state.pass+=p; state.fail+=fl;
    if(fl>0) state.errs.push(f+': '+fl+' fails');
  } catch(e) {
    if(e.killed) { state.timeouts.push(f); }
    else {
      const out = (e.stdout||Buffer.alloc(0)).toString();
      const fm = out.match(/# fail (\d+)/);
      let fl=0,p=0;
      if(fm) fl=parseInt(fm[1]);
      const cm = out.match(/(\d+) passed, (\d+) failed/);
      if(cm){p=parseInt(cm[1]);fl=parseInt(cm[2]);}
      state.pass+=p; state.fail+=fl;
      if(fl>0) state.errs.push(f+': '+fl+' fails');
      else state.errs.push(f+': crash');
    }
  }
  state.processed++;
}
writeFileSync(resultFile, JSON.stringify(state));
console.log('Batch done: '+state.processed+'/'+files.length+' P:'+state.pass+' F:'+state.fail+' E:'+state.errs.length);
process.exit(state.processed < files.length ? 1 : 0);
