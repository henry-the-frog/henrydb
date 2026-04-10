// cherry-pick.js — Apply a commit's changes to the current branch

import { readObject, parseCommit, createBlob, createCommit } from './objects.js';
import { readIndex, writeIndex, buildTreeFromEntries } from './index.js';
import { resolveHead, updateHead } from './refs.js';
import { flattenTree } from './checkout.js';
import { diffLines } from './diff.js';
import { writeFileSync, mkdirSync } from 'fs';
import { join, dirname } from 'path';

/**
 * Cherry-pick: apply a commit's diff to the current branch.
 */
export function cherryPick(gitDir, workDir, commitHash, authorName = 'User', authorEmail = 'user@example.com') {
  const targetCommit = parseCommit(readObject(gitDir, commitHash).content);
  
  // Get the target's parent tree (what the commit changed FROM)
  let parentFiles = new Map();
  if (targetCommit.parents.length > 0) {
    const parentCommit = parseCommit(readObject(gitDir, targetCommit.parents[0]).content);
    parentFiles = new Map(flattenTree(gitDir, parentCommit.tree).map(f => [f.path, f]));
  }
  
  // Get the target's tree (what the commit changed TO)
  const targetFiles = new Map(flattenTree(gitDir, targetCommit.tree).map(f => [f.path, f]));
  
  // Get current HEAD tree
  const headHash = resolveHead(gitDir);
  if (!headHash) throw new Error('Cannot cherry-pick: no commits on current branch');
  const headCommit = parseCommit(readObject(gitDir, headHash).content);
  const headFiles = new Map(flattenTree(gitDir, headCommit.tree).map(f => [f.path, f]));
  
  // Apply the diff: for each change in the target commit, apply to HEAD
  const mergedEntries = new Map(headFiles);
  const conflicts = [];
  
  // Find what changed in the target commit
  const allPaths = new Set([...parentFiles.keys(), ...targetFiles.keys()]);
  
  for (const path of allPaths) {
    const parent = parentFiles.get(path);
    const target = targetFiles.get(path);
    
    if (parent?.hash === target?.hash) continue; // No change in this commit
    
    if (!parent && target) {
      // New file added by the commit
      if (headFiles.has(path)) {
        // Conflict: file exists in HEAD but was added in the cherry-picked commit
        if (headFiles.get(path).hash !== target.hash) {
          conflicts.push(path);
          continue;
        }
      }
      mergedEntries.set(path, target);
    } else if (parent && !target) {
      // File deleted by the commit
      mergedEntries.delete(path);
    } else {
      // File modified by the commit
      const headFile = headFiles.get(path);
      if (!headFile) {
        // File doesn't exist in HEAD — just add it
        mergedEntries.set(path, target);
      } else if (headFile.hash === parent.hash) {
        // HEAD has the same base as the commit's parent — clean apply
        mergedEntries.set(path, target);
      } else {
        // Both HEAD and the commit modified this file — potential conflict
        conflicts.push(path);
      }
    }
  }
  
  if (conflicts.length > 0) {
    return { type: 'conflict', conflicts };
  }
  
  // Build tree and create commit
  const entries = [...mergedEntries.values()];
  const treeHash = buildTreeFromEntries(gitDir, entries);
  
  const now = Math.floor(Date.now() / 1000);
  const author = `${authorName} <${authorEmail}> ${now} +0000`;
  const newCommitHash = createCommit(gitDir, treeHash, [headHash], author, author, targetCommit.message);
  
  // Update working tree
  for (const entry of entries) {
    const fullPath = join(workDir, entry.path);
    mkdirSync(dirname(fullPath), { recursive: true });
    writeFileSync(fullPath, readObject(gitDir, entry.hash).content);
  }
  
  // Update index and HEAD
  writeIndex(gitDir, entries.map(e => ({
    path: e.path, mode: e.mode, hash: e.hash, size: 0, mtime: Date.now()
  })));
  updateHead(gitDir, newCommitHash);
  
  return { type: 'success', hash: newCommitHash, message: targetCommit.message };
}
