// clone.js — Local clone using pack format

import { mkdirSync, existsSync, writeFileSync } from 'fs';
import { join } from 'path';
import { createPack, unpack, enumerateObjects } from './pack.js';
import { readObject, parseCommit } from './objects.js';
import { resolveHead, resolveRef, writeRef, writeHead, listBranches } from './refs.js';
import { writeIndex } from './index.js';
import { flattenTree } from './checkout.js';
import { init } from './commands.js';

/**
 * Clone a local repository.
 * @param {string} srcDir - Path to source repository (working directory)
 * @param {string} destDir - Path to destination directory
 */
export function clone(srcDir, destDir) {
  const srcGit = join(srcDir, '.git');
  if (!existsSync(srcGit)) {
    throw new Error(`Not a git repository: ${srcDir}`);
  }
  
  // Initialize destination
  const destGit = init(destDir);
  
  // Get all branch heads from source
  const branches = listBranches(srcGit);
  const headHash = resolveHead(srcGit);
  
  if (!headHash) {
    return { branches: [], objects: 0 };
  }
  
  // Enumerate all reachable objects from all branches
  const branchHashes = branches.map(b => resolveRef(srcGit, `refs/heads/${b}`)).filter(Boolean);
  const allObjects = enumerateObjects(srcGit, branchHashes);
  
  // Create pack from source
  const pack = createPack(srcGit, allObjects);
  
  // Unpack into destination
  unpack(destGit, pack);
  
  // Copy refs
  for (const branch of branches) {
    const hash = resolveRef(srcGit, `refs/heads/${branch}`);
    if (hash) writeRef(destGit, `refs/heads/${branch}`, hash);
  }
  
  // Set HEAD to main/master
  const currentBranch = branches.includes('main') ? 'main' : branches[0];
  writeHead(destGit, { type: 'ref', ref: `refs/heads/${currentBranch}` });
  
  // Checkout working tree
  const commitData = parseCommit(readObject(destGit, headHash).content);
  const files = flattenTree(destGit, commitData.tree);
  
  for (const file of files) {
    const fullPath = join(destDir, file.path);
    mkdirSync(join(fullPath, '..'), { recursive: true });
    writeFileSync(fullPath, readObject(destGit, file.hash).content);
  }
  
  // Update index
  writeIndex(destGit, files.map(f => ({
    path: f.path, mode: f.mode, hash: f.hash, size: 0, mtime: Date.now()
  })));
  
  return { branches: branches.length, objects: allObjects.length };
}
