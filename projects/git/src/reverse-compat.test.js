// reverse-compat.test.js — Read repos created by real git
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, readFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { execSync } from 'child_process';
import { readObject, parseCommit, parseTree } from './objects.js';
import { resolveHead, listBranches, getCurrentBranch } from './refs.js';
import { log } from './commands.js';
import { flattenTree } from './checkout.js';

function git(workDir, cmd) {
  return execSync(`git ${cmd}`, { 
    cwd: workDir, 
    encoding: 'utf8', 
    env: { 
      ...process.env, 
      GIT_AUTHOR_NAME: 'Test', GIT_AUTHOR_EMAIL: 'test@test.com',
      GIT_COMMITTER_NAME: 'Test', GIT_COMMITTER_EMAIL: 'test@test.com',
      GIT_AUTHOR_DATE: '2026-01-01T12:00:00+0000',
      GIT_COMMITTER_DATE: '2026-01-01T12:00:00+0000'
    } 
  }).trim();
}

describe('Reverse Compatibility: Read real git repos', () => {
  let tmp, workDir, gitDir;

  beforeEach(() => {
    tmp = mkdtempSync(join(tmpdir(), 'tiny-git-reverse-'));
    workDir = tmp;
    git(workDir, 'init -b main');
    gitDir = join(workDir, '.git');
  });

  afterEach(() => {
    rmSync(tmp, { recursive: true, force: true });
  });

  it('reads HEAD from a real git repo', () => {
    git(workDir, 'commit --allow-empty -m "Initial"');
    
    const head = resolveHead(gitDir);
    assert.ok(head);
    assert.strictEqual(head.length, 40);
  });

  it('reads commit objects from real git', () => {
    git(workDir, 'commit --allow-empty -m "Hello from real git"');
    
    const hash = resolveHead(gitDir);
    const obj = readObject(gitDir, hash);
    assert.strictEqual(obj.type, 'commit');
    
    const commit = parseCommit(obj.content);
    assert.strictEqual(commit.message, 'Hello from real git');
    assert.ok(commit.tree);
    assert.strictEqual(commit.parents.length, 0);
  });

  it('reads blob objects from real git', () => {
    execSync('echo "Real git content" > file.txt', { cwd: workDir });
    git(workDir, 'add file.txt');
    git(workDir, 'commit -m "Add file"');
    
    const hash = resolveHead(gitDir);
    const commit = parseCommit(readObject(gitDir, hash).content);
    const tree = parseTree(readObject(gitDir, commit.tree).content);
    
    assert.strictEqual(tree.length, 1);
    assert.strictEqual(tree[0].name, 'file.txt');
    
    const blob = readObject(gitDir, tree[0].hash);
    assert.strictEqual(blob.type, 'blob');
    assert.strictEqual(blob.content.toString().trim(), 'Real git content');
  });

  it('reads nested tree from real git', () => {
    execSync('mkdir -p src/lib', { cwd: workDir });
    execSync('echo "readme" > README.md', { cwd: workDir });
    execSync('echo "main" > src/main.js', { cwd: workDir });
    execSync('echo "util" > src/lib/util.js', { cwd: workDir });
    git(workDir, 'add .');
    git(workDir, 'commit -m "Nested"');
    
    const hash = resolveHead(gitDir);
    const commit = parseCommit(readObject(gitDir, hash).content);
    const files = flattenTree(gitDir, commit.tree);
    
    assert.strictEqual(files.length, 3);
    assert.ok(files.some(f => f.path === 'README.md'));
    assert.ok(files.some(f => f.path === 'src/main.js'));
    assert.ok(files.some(f => f.path === 'src/lib/util.js'));
  });

  it('walks commit history from real git', () => {
    execSync('echo v1 > f.txt', { cwd: workDir });
    git(workDir, 'add f.txt');
    git(workDir, 'commit -m "First"');
    
    execSync('echo v2 > f.txt', { cwd: workDir });
    git(workDir, 'add f.txt');
    git(workDir, 'commit -m "Second"');
    
    execSync('echo v3 > f.txt', { cwd: workDir });
    git(workDir, 'add f.txt');
    git(workDir, 'commit -m "Third"');
    
    const entries = log(gitDir);
    assert.strictEqual(entries.length, 3);
    assert.strictEqual(entries[0].message, 'Third');
    assert.strictEqual(entries[1].message, 'Second');
    assert.strictEqual(entries[2].message, 'First');
  });

  it('reads branches from real git', () => {
    git(workDir, 'commit --allow-empty -m "Base"');
    git(workDir, 'branch feature');
    
    const branches = listBranches(gitDir);
    assert.ok(branches.includes('main'));
    assert.ok(branches.includes('feature'));
    assert.strictEqual(getCurrentBranch(gitDir), 'main');
  });

  it('reads merge commits from real git', () => {
    execSync('echo base > f.txt', { cwd: workDir });
    git(workDir, 'add f.txt');
    git(workDir, 'commit -m "Base"');
    
    git(workDir, 'checkout -b feature');
    execSync('echo feature > g.txt', { cwd: workDir });
    git(workDir, 'add g.txt');
    git(workDir, 'commit -m "Feature"');
    
    git(workDir, 'checkout main');
    execSync('echo main > h.txt', { cwd: workDir });
    git(workDir, 'add h.txt');
    git(workDir, 'commit -m "Main"');
    
    git(workDir, 'merge feature --no-edit');
    
    const entries = log(gitDir);
    // Merge commit should have 2 parents
    assert.ok(entries[0].parents.length >= 2, `Merge commit has ${entries[0].parents.length} parents`);
    assert.ok(entries[0].message.includes('Merge'));
  });
});
