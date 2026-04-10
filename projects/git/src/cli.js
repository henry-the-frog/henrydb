#!/usr/bin/env node
// cli.js — tiny-git command-line interface

import { join, resolve } from 'path';
import { existsSync } from 'fs';
import { init, commit, log, formatLog } from './commands.js';
import { addToIndex, getStatus } from './index.js';
import { checkoutNewBranch, checkout } from './checkout.js';
import { merge } from './merge.js';
import { diffLines, formatUnifiedDiff } from './diff.js';
import { readObject, hashObject } from './objects.js';
import { getCurrentBranch, listBranches, resolveHead } from './refs.js';

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
