// tag.test.js — Tests for git tags
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { createLightweightTag, createAnnotatedTag, listTags, resolveTag, deleteTag } from './tag.js';
import { init, commit } from './commands.js';
import { addToIndex } from './index.js';
import { resolveHead } from './refs.js';
import { readObject } from './objects.js';

describe('Git Tags', () => {
  let tmp, workDir, gitDir;

  beforeEach(() => {
    tmp = mkdtempSync(join(tmpdir(), 'tiny-git-tag-'));
    workDir = tmp;
    gitDir = init(workDir);
    writeFileSync(join(workDir, 'file.txt'), 'content');
    addToIndex(gitDir, workDir, 'file.txt');
    commit(gitDir, workDir, 'Initial');
  });

  afterEach(() => {
    rmSync(tmp, { recursive: true, force: true });
  });

  it('creates lightweight tag', () => {
    const hash = createLightweightTag(gitDir, 'v1.0');
    assert.strictEqual(hash, resolveHead(gitDir));
    assert.ok(listTags(gitDir).includes('v1.0'));
  });

  it('creates annotated tag', () => {
    const tagHash = createAnnotatedTag(gitDir, 'v2.0', 'Release 2.0');
    const obj = readObject(gitDir, tagHash);
    assert.strictEqual(obj.type, 'tag');
    assert.ok(obj.content.toString().includes('Release 2.0'));
  });

  it('lists tags', () => {
    createLightweightTag(gitDir, 'v1.0');
    createAnnotatedTag(gitDir, 'v2.0', 'Two');
    const tags = listTags(gitDir);
    assert.ok(tags.includes('v1.0'));
    assert.ok(tags.includes('v2.0'));
  });

  it('resolves lightweight tag to commit', () => {
    createLightweightTag(gitDir, 'v1.0');
    const hash = resolveTag(gitDir, 'v1.0');
    assert.strictEqual(hash, resolveHead(gitDir));
  });

  it('resolves annotated tag to commit', () => {
    createAnnotatedTag(gitDir, 'v2.0', 'Release');
    const hash = resolveTag(gitDir, 'v2.0');
    assert.strictEqual(hash, resolveHead(gitDir));
  });

  it('deletes a tag', () => {
    createLightweightTag(gitDir, 'temp');
    assert.ok(listTags(gitDir).includes('temp'));
    deleteTag(gitDir, 'temp');
    assert.ok(!listTags(gitDir).includes('temp'));
  });

  it('tags specific commit', () => {
    const c1 = resolveHead(gitDir);
    writeFileSync(join(workDir, 'file.txt'), 'v2');
    addToIndex(gitDir, workDir, 'file.txt');
    commit(gitDir, workDir, 'Second');
    
    createLightweightTag(gitDir, 'first', c1);
    assert.strictEqual(resolveTag(gitDir, 'first'), c1);
    assert.notStrictEqual(resolveTag(gitDir, 'first'), resolveHead(gitDir));
  });
});
