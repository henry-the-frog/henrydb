// stash.test.js — Tests for git stash
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync, readFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { stashSave, stashApply, stashPop, stashList, stashDrop } from './stash.js';
import { init, commit } from './commands.js';
import { addToIndex } from './index.js';

describe('Git Stash', () => {
  let tmp, workDir, gitDir;

  beforeEach(() => {
    tmp = mkdtempSync(join(tmpdir(), 'tiny-git-stash-'));
    workDir = tmp;
    gitDir = init(workDir);
    writeFileSync(join(workDir, 'file.txt'), 'initial');
    addToIndex(gitDir, workDir, 'file.txt');
    commit(gitDir, workDir, 'Initial');
  });

  afterEach(() => {
    rmSync(tmp, { recursive: true, force: true });
  });

  it('saves working tree changes', () => {
    writeFileSync(join(workDir, 'file.txt'), 'modified');
    
    const hash = stashSave(gitDir, workDir, 'WIP changes');
    assert.ok(hash);
    assert.strictEqual(hash.length, 40);
    
    // Working tree should be reset to HEAD
    assert.strictEqual(readFileSync(join(workDir, 'file.txt'), 'utf8'), 'initial');
  });

  it('applies stashed changes', () => {
    writeFileSync(join(workDir, 'file.txt'), 'modified');
    stashSave(gitDir, workDir);
    
    // Working tree is now clean
    assert.strictEqual(readFileSync(join(workDir, 'file.txt'), 'utf8'), 'initial');
    
    // Apply stash
    stashApply(gitDir, workDir);
    assert.strictEqual(readFileSync(join(workDir, 'file.txt'), 'utf8'), 'modified');
  });

  it('pops stash (apply + remove)', () => {
    writeFileSync(join(workDir, 'file.txt'), 'modified');
    stashSave(gitDir, workDir);
    
    stashPop(gitDir, workDir);
    assert.strictEqual(readFileSync(join(workDir, 'file.txt'), 'utf8'), 'modified');
    
    // Stash list should be empty
    assert.strictEqual(stashList(gitDir).length, 0);
  });

  it('lists stashes', () => {
    writeFileSync(join(workDir, 'file.txt'), 'change 1');
    stashSave(gitDir, workDir, 'First stash');
    
    writeFileSync(join(workDir, 'file.txt'), 'change 2');
    addToIndex(gitDir, workDir, 'file.txt');
    commit(gitDir, workDir, 'Commit 2');
    writeFileSync(join(workDir, 'file.txt'), 'change 3');
    stashSave(gitDir, workDir, 'Second stash');
    
    const list = stashList(gitDir);
    assert.strictEqual(list.length, 2);
    assert.strictEqual(list[0].message, 'Second stash');
    assert.strictEqual(list[1].message, 'First stash');
  });

  it('drops a specific stash', () => {
    writeFileSync(join(workDir, 'file.txt'), 'stash1');
    stashSave(gitDir, workDir, 'Stash 1');
    
    stashDrop(gitDir, 0);
    assert.strictEqual(stashList(gitDir).length, 0);
  });
});
