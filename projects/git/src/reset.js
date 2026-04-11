// reset.js — Git reset (move HEAD, update index/working tree)

import { readFileSync, writeFileSync, existsSync, mkdirSync, unlinkSync } from 'fs';
import { join, dirname } from 'path';
import { readObject, parseCommit } from './objects.js';
import { readIndex, writeIndex } from './index.js';
import { resolveHead, updateHead, resolveRef } from './refs.js';
import { flattenTree } from './checkout.js';

/**
 * Git reset: move HEAD to a target commit.
 * Modes:
 * - soft: only move HEAD
 * - mixed (default): move HEAD + reset index
 * - hard: move HEAD + reset index + reset working tree
 */
export function reset(gitDir, workDir, target, mode = 'mixed') {
  // Resolve target
  let targetHash;
  if (target === 'HEAD~1' || target === 'HEAD^') {
    const headHash = resolveHead(gitDir);
    if (!headHash) throw new Error('No commits');
    const commit = parseCommit(readObject(gitDir, headHash).content);
    if (commit.parents.length === 0) throw new Error('Cannot reset: no parent commit');
    targetHash = commit.parents[0];
  } else if (target.startsWith('HEAD~')) {
    const n = parseInt(target.slice(5));
    let hash = resolveHead(gitDir);
    for (let i = 0; i < n; i++) {
      const commit = parseCommit(readObject(gitDir, hash).content);
      if (commit.parents.length === 0) throw new Error(`Cannot go back ${n} commits`);
      hash = commit.parents[0];
    }
    targetHash = hash;
  } else if (target.length === 40) {
    targetHash = target;
  } else {
    targetHash = resolveRef(gitDir, `refs/heads/${target}`) || resolveRef(gitDir, `refs/tags/${target}`);
  }
  
  if (!targetHash) throw new Error(`Unknown target: ${target}`);
  
  // Move HEAD
  updateHead(gitDir, targetHash);
  
  if (mode === 'soft') return { hash: targetHash, mode };
  
  // Reset index
  const commit = parseCommit(readObject(gitDir, targetHash).content);
  const files = flattenTree(gitDir, commit.tree);
  
  // Read current index BEFORE overwriting (needed for hard reset to know what to remove)
  const currentIndex = (mode === 'hard') ? readIndex(gitDir) : null;
  
  writeIndex(gitDir, files.map(f => ({
    path: f.path, mode: f.mode, hash: f.hash, size: 0, mtime: Date.now()
  })));
  
  if (mode === 'mixed') return { hash: targetHash, mode };
  
  // Hard reset: update working tree
  // Remove files not in target
  const newPaths = new Set(files.map(f => f.path));
  
  for (const entry of currentIndex) {
    if (!newPaths.has(entry.path)) {
      const fullPath = join(workDir, entry.path);
      if (existsSync(fullPath)) unlinkSync(fullPath);
    }
  }
  
  // Write target files
  for (const file of files) {
    const fullPath = join(workDir, file.path);
    mkdirSync(dirname(fullPath), { recursive: true });
    writeFileSync(fullPath, readObject(gitDir, file.hash).content);
  }
  
  return { hash: targetHash, mode };
}
