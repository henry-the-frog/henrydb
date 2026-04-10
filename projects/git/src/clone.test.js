// clone.test.js — Tests for local clone
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync, readFileSync, mkdirSync, existsSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { clone } from './clone.js';
import { init, commit, log } from './commands.js';
import { addToIndex } from './index.js';
import { checkoutNewBranch } from './checkout.js';
import { resolveHead, listBranches, getCurrentBranch } from './refs.js';

describe('Git Clone', () => {
  let srcDir, destDir;

  beforeEach(() => {
    srcDir = mkdtempSync(join(tmpdir(), 'tiny-git-src-'));
    destDir = mkdtempSync(join(tmpdir(), 'tiny-git-dest-'));
    rmSync(destDir, { recursive: true }); // clone creates it
  });

  afterEach(() => {
    rmSync(srcDir, { recursive: true, force: true });
    if (existsSync(destDir)) rmSync(destDir, { recursive: true, force: true });
  });

  it('clones a simple repository', () => {
    init(srcDir);
    writeFileSync(join(srcDir, 'hello.txt'), 'Hello World');
    addToIndex(join(srcDir, '.git'), srcDir, 'hello.txt');
    commit(join(srcDir, '.git'), srcDir, 'Initial');
    
    mkdirSync(destDir);
    const result = clone(srcDir, destDir);
    
    assert.ok(result.objects > 0);
    assert.ok(existsSync(join(destDir, '.git')));
    assert.ok(existsSync(join(destDir, 'hello.txt')));
    assert.strictEqual(readFileSync(join(destDir, 'hello.txt'), 'utf8'), 'Hello World');
  });

  it('preserves commit history', () => {
    const srcGit = init(srcDir);
    writeFileSync(join(srcDir, 'f.txt'), 'v1');
    addToIndex(srcGit, srcDir, 'f.txt');
    commit(srcGit, srcDir, 'First');
    
    writeFileSync(join(srcDir, 'f.txt'), 'v2');
    addToIndex(srcGit, srcDir, 'f.txt');
    commit(srcGit, srcDir, 'Second');
    
    mkdirSync(destDir);
    clone(srcDir, destDir);
    
    const entries = log(join(destDir, '.git'));
    assert.strictEqual(entries.length, 2);
    assert.strictEqual(entries[0].message, 'Second');
    assert.strictEqual(entries[1].message, 'First');
  });

  it('preserves branches', () => {
    const srcGit = init(srcDir);
    writeFileSync(join(srcDir, 'f.txt'), 'base');
    addToIndex(srcGit, srcDir, 'f.txt');
    commit(srcGit, srcDir, 'Base');
    
    checkoutNewBranch(srcGit, srcDir, 'feature');
    
    mkdirSync(destDir);
    clone(srcDir, destDir);
    
    const branches = listBranches(join(destDir, '.git'));
    assert.ok(branches.includes('main'));
    assert.ok(branches.includes('feature'));
  });

  it('handles nested directories', () => {
    const srcGit = init(srcDir);
    mkdirSync(join(srcDir, 'src', 'lib'), { recursive: true });
    writeFileSync(join(srcDir, 'README.md'), '# Project');
    writeFileSync(join(srcDir, 'src', 'main.js'), 'main');
    writeFileSync(join(srcDir, 'src', 'lib', 'util.js'), 'util');
    addToIndex(srcGit, srcDir, 'README.md');
    addToIndex(srcGit, srcDir, 'src');
    commit(srcGit, srcDir, 'Nested');
    
    mkdirSync(destDir);
    clone(srcDir, destDir);
    
    assert.strictEqual(readFileSync(join(destDir, 'README.md'), 'utf8'), '# Project');
    assert.strictEqual(readFileSync(join(destDir, 'src', 'main.js'), 'utf8'), 'main');
    assert.strictEqual(readFileSync(join(destDir, 'src', 'lib', 'util.js'), 'utf8'), 'util');
  });

  it('cloned repo can make new commits', () => {
    const srcGit = init(srcDir);
    writeFileSync(join(srcDir, 'f.txt'), 'original');
    addToIndex(srcGit, srcDir, 'f.txt');
    commit(srcGit, srcDir, 'Original');
    
    mkdirSync(destDir);
    clone(srcDir, destDir);
    
    const destGit = join(destDir, '.git');
    writeFileSync(join(destDir, 'f.txt'), 'modified');
    addToIndex(destGit, destDir, 'f.txt');
    commit(destGit, destDir, 'Modified in clone');
    
    const entries = log(destGit);
    assert.strictEqual(entries.length, 2);
    assert.strictEqual(entries[0].message, 'Modified in clone');
  });
});
