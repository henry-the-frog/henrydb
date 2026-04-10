// cherry-pick.test.js — Tests for git cherry-pick
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync, readFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { cherryPick } from './cherry-pick.js';
import { init, commit, log } from './commands.js';
import { addToIndex } from './index.js';
import { checkoutNewBranch, checkout } from './checkout.js';
import { resolveHead } from './refs.js';

describe('Git Cherry-Pick', () => {
  let tmp, workDir, gitDir;

  beforeEach(() => {
    tmp = mkdtempSync(join(tmpdir(), 'tiny-git-cp-'));
    workDir = tmp;
    gitDir = init(workDir);
  });

  afterEach(() => {
    rmSync(tmp, { recursive: true, force: true });
  });

  it('applies a commit from another branch', () => {
    writeFileSync(join(workDir, 'base.txt'), 'base');
    addToIndex(gitDir, workDir, 'base.txt');
    commit(gitDir, workDir, 'Base');
    
    checkoutNewBranch(gitDir, workDir, 'feature');
    writeFileSync(join(workDir, 'feature.txt'), 'feature only');
    addToIndex(gitDir, workDir, 'feature.txt');
    const featureCommit = commit(gitDir, workDir, 'Add feature');
    
    checkout(gitDir, workDir, 'main');
    const result = cherryPick(gitDir, workDir, featureCommit);
    
    assert.strictEqual(result.type, 'success');
    assert.strictEqual(readFileSync(join(workDir, 'feature.txt'), 'utf8'), 'feature only');
    
    const entries = log(gitDir);
    assert.strictEqual(entries[0].message, 'Add feature');
  });

  it('preserves existing files', () => {
    writeFileSync(join(workDir, 'existing.txt'), 'keep me');
    addToIndex(gitDir, workDir, 'existing.txt');
    commit(gitDir, workDir, 'Base');
    
    checkoutNewBranch(gitDir, workDir, 'feature');
    writeFileSync(join(workDir, 'new.txt'), 'new');
    addToIndex(gitDir, workDir, 'new.txt');
    const featureCommit = commit(gitDir, workDir, 'Add new');
    
    checkout(gitDir, workDir, 'main');
    cherryPick(gitDir, workDir, featureCommit);
    
    assert.strictEqual(readFileSync(join(workDir, 'existing.txt'), 'utf8'), 'keep me');
    assert.strictEqual(readFileSync(join(workDir, 'new.txt'), 'utf8'), 'new');
  });

  it('applies modification commits', () => {
    writeFileSync(join(workDir, 'file.txt'), 'v1');
    addToIndex(gitDir, workDir, 'file.txt');
    commit(gitDir, workDir, 'V1');
    
    checkoutNewBranch(gitDir, workDir, 'feature');
    writeFileSync(join(workDir, 'file.txt'), 'v2-feature');
    addToIndex(gitDir, workDir, 'file.txt');
    const featureCommit = commit(gitDir, workDir, 'Update in feature');
    
    checkout(gitDir, workDir, 'main');
    // HEAD still has v1
    assert.strictEqual(readFileSync(join(workDir, 'file.txt'), 'utf8'), 'v1');
    
    cherryPick(gitDir, workDir, featureCommit);
    assert.strictEqual(readFileSync(join(workDir, 'file.txt'), 'utf8'), 'v2-feature');
  });

  it('detects conflicts', () => {
    writeFileSync(join(workDir, 'file.txt'), 'base');
    addToIndex(gitDir, workDir, 'file.txt');
    commit(gitDir, workDir, 'Base');
    
    checkoutNewBranch(gitDir, workDir, 'feature');
    writeFileSync(join(workDir, 'file.txt'), 'feature version');
    addToIndex(gitDir, workDir, 'file.txt');
    const featureCommit = commit(gitDir, workDir, 'Feature change');
    
    checkout(gitDir, workDir, 'main');
    writeFileSync(join(workDir, 'file.txt'), 'main version');
    addToIndex(gitDir, workDir, 'file.txt');
    commit(gitDir, workDir, 'Main change');
    
    const result = cherryPick(gitDir, workDir, featureCommit);
    assert.strictEqual(result.type, 'conflict');
    assert.ok(result.conflicts.includes('file.txt'));
  });
});
