// compat.test.js — Compatibility tests with real git
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync, mkdirSync, readFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { execSync } from 'child_process';
import { init, commit, log } from './commands.js';
import { addToIndex } from './index.js';
import { checkoutNewBranch } from './checkout.js';
import { merge } from './merge.js';
import { writeHead } from './refs.js';

function git(workDir, cmd) {
  return execSync(`git ${cmd}`, { cwd: workDir, encoding: 'utf8', env: { ...process.env, GIT_AUTHOR_NAME: 'Test', GIT_AUTHOR_EMAIL: 'test@test.com', GIT_COMMITTER_NAME: 'Test', GIT_COMMITTER_EMAIL: 'test@test.com' } }).trim();
}

describe('Real Git Compatibility', () => {
  let tmp, workDir, gitDir;

  beforeEach(() => {
    tmp = mkdtempSync(join(tmpdir(), 'tiny-git-compat-'));
    workDir = tmp;
    gitDir = init(workDir);
  });

  afterEach(() => {
    rmSync(tmp, { recursive: true, force: true });
  });

  it('git log reads our commits', () => {
    writeFileSync(join(workDir, 'file.txt'), 'hello');
    addToIndex(gitDir, workDir, 'file.txt');
    commit(gitDir, workDir, 'First commit');
    
    const output = git(workDir, 'log --oneline');
    assert.ok(output.includes('First commit'));
  });

  it('git cat-file reads our objects', () => {
    writeFileSync(join(workDir, 'data.txt'), 'test data');
    addToIndex(gitDir, workDir, 'data.txt');
    const hash = commit(gitDir, workDir, 'Test');
    
    const type = git(workDir, `cat-file -t ${hash}`);
    assert.strictEqual(type, 'commit');
    
    const content = git(workDir, `cat-file -p ${hash}`);
    assert.ok(content.includes('tree '));
    assert.ok(content.includes('Test'));
  });

  it('git cat-file reads our tree', () => {
    writeFileSync(join(workDir, 'a.txt'), 'aaa');
    writeFileSync(join(workDir, 'b.txt'), 'bbb');
    addToIndex(gitDir, workDir, 'a.txt');
    addToIndex(gitDir, workDir, 'b.txt');
    commit(gitDir, workDir, 'Two files');
    
    const treeOutput = git(workDir, 'cat-file -p HEAD^{tree}');
    assert.ok(treeOutput.includes('a.txt'));
    assert.ok(treeOutput.includes('b.txt'));
  });

  it('git cat-file reads our blob', () => {
    const content = 'specific content for blob test';
    writeFileSync(join(workDir, 'blob.txt'), content);
    addToIndex(gitDir, workDir, 'blob.txt');
    commit(gitDir, workDir, 'Blob test');
    
    const blobContent = git(workDir, 'show HEAD:blob.txt');
    assert.strictEqual(blobContent, content);
  });

  it('git diff works between our commits', () => {
    writeFileSync(join(workDir, 'file.txt'), 'version 1');
    addToIndex(gitDir, workDir, 'file.txt');
    commit(gitDir, workDir, 'V1');
    
    writeFileSync(join(workDir, 'file.txt'), 'version 2');
    addToIndex(gitDir, workDir, 'file.txt');
    commit(gitDir, workDir, 'V2');
    
    const diff = git(workDir, 'diff HEAD~1 HEAD -- file.txt');
    assert.ok(diff.includes('-version 1'));
    assert.ok(diff.includes('+version 2'));
  });

  it('git log shows correct parent chain', () => {
    writeFileSync(join(workDir, 'f.txt'), 'v1');
    addToIndex(gitDir, workDir, 'f.txt');
    commit(gitDir, workDir, 'First');
    
    writeFileSync(join(workDir, 'f.txt'), 'v2');
    addToIndex(gitDir, workDir, 'f.txt');
    commit(gitDir, workDir, 'Second');
    
    writeFileSync(join(workDir, 'f.txt'), 'v3');
    addToIndex(gitDir, workDir, 'f.txt');
    commit(gitDir, workDir, 'Third');
    
    const output = git(workDir, 'log --oneline');
    const lines = output.split('\n');
    assert.strictEqual(lines.length, 3);
    assert.ok(lines[0].includes('Third'));
    assert.ok(lines[1].includes('Second'));
    assert.ok(lines[2].includes('First'));
  });

  it('git branch reads our branches', () => {
    writeFileSync(join(workDir, 'f.txt'), 'base');
    addToIndex(gitDir, workDir, 'f.txt');
    commit(gitDir, workDir, 'Base');
    
    checkoutNewBranch(gitDir, workDir, 'feature');
    
    const branches = git(workDir, 'branch');
    assert.ok(branches.includes('feature'));
    assert.ok(branches.includes('main'));
  });

  it('git reads nested tree structures', () => {
    mkdirSync(join(workDir, 'src'));
    mkdirSync(join(workDir, 'src', 'lib'));
    writeFileSync(join(workDir, 'README.md'), '# Project');
    writeFileSync(join(workDir, 'src', 'main.js'), 'index');
    writeFileSync(join(workDir, 'src', 'lib', 'util.js'), 'util');
    addToIndex(gitDir, workDir, 'README.md');
    addToIndex(gitDir, workDir, 'src');
    commit(gitDir, workDir, 'Nested');
    
    const readme = git(workDir, 'show HEAD:README.md');
    assert.strictEqual(readme, '# Project');
    
    const util = git(workDir, 'show HEAD:src/lib/util.js');
    assert.strictEqual(util, 'util');
  });

  it('git reads merge commits', () => {
    writeFileSync(join(workDir, 'base.txt'), 'base');
    addToIndex(gitDir, workDir, 'base.txt');
    commit(gitDir, workDir, 'Base');
    
    checkoutNewBranch(gitDir, workDir, 'feature');
    writeFileSync(join(workDir, 'feat.txt'), 'feature');
    addToIndex(gitDir, workDir, 'feat.txt');
    commit(gitDir, workDir, 'Feature');
    
    // Hack: switch back to main without checkout (just update HEAD)
    writeHead(gitDir, { type: 'ref', ref: 'refs/heads/main' });
    
    writeFileSync(join(workDir, 'main.txt'), 'main');
    addToIndex(gitDir, workDir, 'main.txt');
    commit(gitDir, workDir, 'Main');
    
    merge(gitDir, workDir, 'feature');
    
    const output = git(workDir, 'log --oneline --all');
    assert.ok(output.includes('Merge'));
    assert.ok(output.includes('Feature'));
    assert.ok(output.includes('Main'));
  });
});
