// stash.js — Git stash (save/apply working tree changes)
// A stash is just a commit object that's not on any branch
// Stored as refs/stash with reflog-like behavior

import { readFileSync, writeFileSync, existsSync, mkdirSync, unlinkSync } from 'fs';
import { join, dirname } from 'path';
import { createBlob, createTree, createCommit, readObject, parseCommit, parseTree } from './objects.js';
import { readIndex, writeIndex, addToIndex, buildTreeFromEntries, getStatus } from './index.js';
import { resolveHead, resolveRef, writeRef } from './refs.js';
import { flattenTree } from './checkout.js';

/**
 * Save working tree changes to the stash.
 * Returns the stash commit hash.
 */
export function stashSave(gitDir, workDir, message = 'WIP') {
  const headHash = resolveHead(gitDir);
  if (!headHash) throw new Error('Cannot stash: no commits yet');
  
  // Build tree from current working tree (including unstaged changes)
  const index = readIndex(gitDir);
  if (index.length === 0) throw new Error('Nothing to stash');
  
  // Re-add all tracked files to capture working tree state
  const savedIndex = [...index];
  for (const entry of savedIndex) {
    const fullPath = join(workDir, entry.path);
    if (existsSync(fullPath)) {
      addToIndex(gitDir, workDir, entry.path);
    }
  }
  
  // Build tree from updated index
  const workTreeIndex = readIndex(gitDir);
  const treeHash = buildTreeFromEntries(gitDir, workTreeIndex);
  
  // Create stash commit (parent = HEAD)
  const now = Math.floor(Date.now() / 1000);
  const author = `Stash <stash@tiny-git> ${now} +0000`;
  const stashHash = createCommit(gitDir, treeHash, [headHash], author, author, message);
  
  // Save stash ref
  const stashPath = join(gitDir, 'refs', 'stash');
  const existingStashes = readStashList(gitDir);
  existingStashes.unshift({ hash: stashHash, message });
  writeFileSync(stashPath, existingStashes.map(s => `${s.hash} ${s.message}`).join('\n') + '\n');
  
  // Restore index to original state (before working tree changes)
  writeIndex(gitDir, savedIndex);
  
  // Reset working tree to HEAD
  const headCommit = parseCommit(readObject(gitDir, headHash).content);
  const headFiles = flattenTree(gitDir, headCommit.tree);
  for (const file of headFiles) {
    const fullPath = join(workDir, file.path);
    mkdirSync(dirname(fullPath), { recursive: true });
    writeFileSync(fullPath, readObject(gitDir, file.hash).content);
  }
  
  return stashHash;
}

/**
 * Apply the latest stash (or a specific one by index).
 */
export function stashApply(gitDir, workDir, index = 0) {
  const stashes = readStashList(gitDir);
  if (index >= stashes.length) throw new Error(`No stash at index ${index}`);
  
  const stash = stashes[index];
  const stashCommit = parseCommit(readObject(gitDir, stash.hash).content);
  const stashFiles = flattenTree(gitDir, stashCommit.tree);
  
  // Apply stash files to working tree
  for (const file of stashFiles) {
    const fullPath = join(workDir, file.path);
    mkdirSync(dirname(fullPath), { recursive: true });
    writeFileSync(fullPath, readObject(gitDir, file.hash).content);
  }
  
  return stash;
}

/**
 * Pop the latest stash (apply + remove).
 */
export function stashPop(gitDir, workDir, index = 0) {
  const result = stashApply(gitDir, workDir, index);
  stashDrop(gitDir, index);
  return result;
}

/**
 * Drop a stash entry.
 */
export function stashDrop(gitDir, index = 0) {
  const stashes = readStashList(gitDir);
  if (index >= stashes.length) throw new Error(`No stash at index ${index}`);
  stashes.splice(index, 1);
  const stashPath = join(gitDir, 'refs', 'stash');
  if (stashes.length === 0) {
    try { unlinkSync(stashPath); } catch {}
  } else {
    writeFileSync(stashPath, stashes.map(s => `${s.hash} ${s.message}`).join('\n') + '\n');
  }
}

/**
 * List all stashes.
 */
export function stashList(gitDir) {
  return readStashList(gitDir);
}

function readStashList(gitDir) {
  const stashPath = join(gitDir, 'refs', 'stash');
  if (!existsSync(stashPath)) return [];
  const content = readFileSync(stashPath, 'utf8').trim();
  if (!content) return [];
  return content.split('\n').map(line => {
    const spaceIdx = line.indexOf(' ');
    return { hash: line.slice(0, spaceIdx), message: line.slice(spaceIdx + 1) };
  });
}
