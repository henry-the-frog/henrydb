#!/usr/bin/env node
// cli.js — tiny-git command-line interface

import { join, resolve } from 'path';
import { existsSync, mkdirSync } from 'fs';
import { init, commit, log, formatLog } from './commands.js';
import { addToIndex, getStatus } from './index.js';
import { checkoutNewBranch, checkout } from './checkout.js';
import { merge } from './merge.js';
import { clone } from './clone.js';
import { stashSave, stashApply, stashPop, stashList, stashDrop } from './stash.js';
import { createLightweightTag, createAnnotatedTag, listTags, deleteTag } from './tag.js';
import { diffLines, formatUnifiedDiff } from './diff.js';
import { readObject, hashObject } from './objects.js';
import { getCurrentBranch, listBranches, resolveHead, resolveRef } from './refs.js';

const args = process.argv.slice(2);
const cmd = args[0];
const cwd = process.cwd();

function findGitDir(startDir) {
  let dir = startDir;
  while (dir !== '/') {
    if (existsSync(join(dir, '.git'))) return join(dir, '.git');
    dir = resolve(dir, '..');
  }
  return null;
}

try {
  switch (cmd) {
    case 'init': {
      const gitDir = init(cwd);
      console.log(`Initialized empty tiny-git repository in ${gitDir}`);
      break;
    }
    
    case 'add': {
      const gitDir = findGitDir(cwd);
      if (!gitDir) { console.error('Not a git repository'); process.exit(1); }
      const workDir = resolve(gitDir, '..');
      for (const file of args.slice(1)) {
        addToIndex(gitDir, workDir, file);
      }
      break;
    }
    
    case 'commit': {
      const gitDir = findGitDir(cwd);
      if (!gitDir) { console.error('Not a git repository'); process.exit(1); }
      const workDir = resolve(gitDir, '..');
      const msgIdx = args.indexOf('-m');
      const message = msgIdx >= 0 ? args[msgIdx + 1] : 'No message';
      const hash = commit(gitDir, workDir, message);
      const branch = getCurrentBranch(gitDir);
      console.log(`[${branch || 'detached'} ${hash.slice(0, 7)}] ${message}`);
      break;
    }
    
    case 'log': {
      const gitDir = findGitDir(cwd);
      if (!gitDir) { console.error('Not a git repository'); process.exit(1); }
      const maxCount = args.includes('-n') ? parseInt(args[args.indexOf('-n') + 1]) : Infinity;
      const entries = log(gitDir, maxCount);
      console.log(formatLog(entries));
      break;
    }
    
    case 'status': {
      const gitDir = findGitDir(cwd);
      if (!gitDir) { console.error('Not a git repository'); process.exit(1); }
      const workDir = resolve(gitDir, '..');
      const branch = getCurrentBranch(gitDir);
      console.log(`On branch ${branch || '(detached HEAD)'}`);
      const status = getStatus(gitDir, workDir, null);
      if (status.staged.length) {
        console.log('\nChanges to be committed:');
        for (const f of status.staged) console.log(`  new file:   ${f}`);
      }
      if (status.modified.length) {
        console.log('\nChanges not staged for commit:');
        for (const f of status.modified) console.log(`  modified:   ${f}`);
      }
      if (status.deleted.length) {
        console.log('\nDeleted files:');
        for (const f of status.deleted) console.log(`  deleted:    ${f}`);
      }
      if (status.untracked.length) {
        console.log('\nUntracked files:');
        for (const f of status.untracked) console.log(`  ${f}`);
      }
      if (!status.staged.length && !status.modified.length && !status.untracked.length && !status.deleted.length) {
        console.log('nothing to commit, working tree clean');
      }
      break;
    }
    
    case 'branch': {
      const gitDir = findGitDir(cwd);
      if (!gitDir) { console.error('Not a git repository'); process.exit(1); }
      const workDir = resolve(gitDir, '..');
      if (args.length > 1) {
        checkoutNewBranch(gitDir, workDir, args[1]);
        console.log(`Created branch '${args[1]}'`);
      } else {
        const current = getCurrentBranch(gitDir);
        for (const b of listBranches(gitDir)) {
          console.log(`${b === current ? '* ' : '  '}${b}`);
        }
      }
      break;
    }
    
    case 'checkout': {
      const gitDir = findGitDir(cwd);
      if (!gitDir) { console.error('Not a git repository'); process.exit(1); }
      const workDir = resolve(gitDir, '..');
      if (args[1] === '-b') {
        checkoutNewBranch(gitDir, workDir, args[2]);
        console.log(`Switched to new branch '${args[2]}'`);
      } else {
        const result = checkout(gitDir, workDir, args[1]);
        console.log(`Switched to ${result.branch ? `branch '${result.branch}'` : `commit ${result.hash.slice(0, 7)}`}`);
      }
      break;
    }
    
    case 'merge': {
      const gitDir = findGitDir(cwd);
      if (!gitDir) { console.error('Not a git repository'); process.exit(1); }
      const workDir = resolve(gitDir, '..');
      const result = merge(gitDir, workDir, args[1]);
      if (result.type === 'fast-forward') {
        console.log(`Fast-forward to ${result.hash.slice(0, 7)}`);
      } else if (result.type === 'merge') {
        console.log(`Merge made by the 'recursive' strategy.`);
      } else if (result.type === 'conflict') {
        console.log(`CONFLICT in: ${result.conflicts.join(', ')}`);
        console.log('Fix conflicts and commit the result.');
        process.exit(1);
      } else {
        console.log('Already up to date.');
      }
      break;
    }
    
    case 'diff': {
      const gitDir = findGitDir(cwd);
      if (!gitDir) { console.error('Not a git repository'); process.exit(1); }
      const workDir = resolve(gitDir, '..');
      
      if (args.length > 2) {
        // diff <commit1> <commit2>
        const { readObject: ro, parseCommit: pc, parseTree: pt } = await import('./objects.js');
        const { flattenTree: ft } = await import('./checkout.js');
        
        const hash1 = args[1].length === 40 ? args[1] : resolveRef(gitDir, `refs/heads/${args[1]}`);
        const hash2 = args[2].length === 40 ? args[2] : resolveRef(gitDir, `refs/heads/${args[2]}`);
        
        const c1 = pc(ro(gitDir, hash1).content);
        const c2 = pc(ro(gitDir, hash2).content);
        const files1 = new Map(ft(gitDir, c1.tree).map(f => [f.path, f]));
        const files2 = new Map(ft(gitDir, c2.tree).map(f => [f.path, f]));
        
        const allPaths = new Set([...files1.keys(), ...files2.keys()]);
        for (const path of [...allPaths].sort()) {
          const f1 = files1.get(path);
          const f2 = files2.get(path);
          if (f1?.hash === f2?.hash) continue;
          
          const old = f1 ? ro(gitDir, f1.hash).content.toString() : '';
          const _new = f2 ? ro(gitDir, f2.hash).content.toString() : '';
          const ops = diffLines(old, _new);
          const output = formatUnifiedDiff(f1 ? path : '/dev/null', f2 ? path : '/dev/null', ops);
          if (output) console.log(output);
        }
      } else {
        // diff (working tree vs index)
        const { readIndex: ri } = await import('./index.js');
        const { readFileSync: rfs } = await import('fs');
        const index = ri(gitDir);
        for (const entry of index) {
          const fp = join(workDir, entry.path);
          if (!existsSync(fp)) continue;
          const current = rfs(fp, 'utf8');
          const { readObject: ro } = await import('./objects.js');
          const staged = ro(gitDir, entry.hash).content.toString();
          if (current === staged) continue;
          const ops = diffLines(staged, current);
          const output = formatUnifiedDiff(entry.path, entry.path, ops);
          if (output) console.log(output);
        }
      }
      break;
    }

    case 'clone': {
      const src = args[1];
      const dest = args[2] || src.split('/').pop();
      mkdirSync(dest, { recursive: true });
      const result = clone(src, dest);
      console.log(`Cloned into '${dest}': ${result.objects} objects, ${result.branches} branches`);
      break;
    }
    
    case 'tag': {
      const gitDir = findGitDir(cwd);
      if (!gitDir) { console.error('Not a git repository'); process.exit(1); }
      if (args[1] === '-d') {
        deleteTag(gitDir, args[2]);
        console.log(`Deleted tag '${args[2]}'`);
      } else if (args[1] === '-a') {
        const name = args[2];
        const msgIdx = args.indexOf('-m');
        const msg = msgIdx >= 0 ? args[msgIdx + 1] : name;
        createAnnotatedTag(gitDir, name, msg);
        console.log(`Created annotated tag '${name}'`);
      } else if (args[1]) {
        createLightweightTag(gitDir, args[1]);
        console.log(`Created tag '${args[1]}'`);
      } else {
        for (const tag of listTags(gitDir)) console.log(tag);
      }
      break;
    }
    
    case 'stash': {
      const gitDir = findGitDir(cwd);
      if (!gitDir) { console.error('Not a git repository'); process.exit(1); }
      const workDir = resolve(gitDir, '..');
      const subcmd = args[1] || 'save';
      
      if (subcmd === 'save' || subcmd === 'push') {
        const msg = args.slice(2).join(' ') || 'WIP';
        stashSave(gitDir, workDir, msg);
        console.log(`Saved working directory to stash: ${msg}`);
      } else if (subcmd === 'apply') {
        const idx = parseInt(args[2] || '0');
        stashApply(gitDir, workDir, idx);
        console.log('Applied stash');
      } else if (subcmd === 'pop') {
        const idx = parseInt(args[2] || '0');
        stashPop(gitDir, workDir, idx);
        console.log('Popped stash');
      } else if (subcmd === 'list') {
        const list = stashList(gitDir);
        list.forEach((s, i) => console.log(`stash@{${i}}: ${s.message}`));
      } else if (subcmd === 'drop') {
        const idx = parseInt(args[2] || '0');
        stashDrop(gitDir, idx);
        console.log(`Dropped stash@{${idx}}`);
      }
      break;
    }
    
    case 'hash-object': {
      const file = args[1];
      const { readFileSync } = await import('fs');
      const content = readFileSync(file);
      const { hash } = hashObject('blob', content);
      console.log(hash);
      break;
    }
    
    default:
      console.log('tiny-git — A git implementation from scratch');
      console.log('');
      console.log('Commands:');
      console.log('  init                Initialize a new repository');
      console.log('  add <file>          Stage a file');
      console.log('  commit -m <msg>     Create a commit');
      console.log('  log [-n <count>]    Show commit history');
      console.log('  status              Show working tree status');
      console.log('  branch [name]       List or create branches');
      console.log('  checkout <target>   Switch branches or commits');
      console.log('  checkout -b <name>  Create and switch to new branch');
      console.log('  merge <branch>      Merge a branch');
      console.log('  hash-object <file>  Compute SHA-1 hash of file');
  }
} catch (err) {
  console.error(`error: ${err.message}`);
  process.exit(1);
}
