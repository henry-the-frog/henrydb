// tag.js — Git tags (lightweight and annotated)

import { existsSync, readFileSync, writeFileSync, mkdirSync, readdirSync, unlinkSync } from 'fs';
import { join } from 'path';
import { createTag as createTagObject, readObject } from './objects.js';
import { resolveHead, writeRef, resolveRef } from './refs.js';

/**
 * Create a lightweight tag.
 */
export function createLightweightTag(gitDir, name, targetHash) {
  if (!targetHash) targetHash = resolveHead(gitDir);
  if (!targetHash) throw new Error('Cannot tag: no commits');
  writeRef(gitDir, `refs/tags/${name}`, targetHash);
  return targetHash;
}

/**
 * Create an annotated tag.
 */
export function createAnnotatedTag(gitDir, name, message, taggerName = 'User', taggerEmail = 'user@example.com') {
  const targetHash = resolveHead(gitDir);
  if (!targetHash) throw new Error('Cannot tag: no commits');
  
  const now = Math.floor(Date.now() / 1000);
  const tagger = `${taggerName} <${taggerEmail}> ${now} +0000`;
  
  const tagHash = createTagObject(gitDir, targetHash, 'commit', name, tagger, message);
  writeRef(gitDir, `refs/tags/${name}`, tagHash);
  return tagHash;
}

/**
 * List all tags.
 */
export function listTags(gitDir) {
  const tagsDir = join(gitDir, 'refs', 'tags');
  if (!existsSync(tagsDir)) return [];
  return readdirSync(tagsDir).filter(f => !f.startsWith('.'));
}

/**
 * Resolve a tag to a commit hash.
 */
export function resolveTag(gitDir, name) {
  const hash = resolveRef(gitDir, `refs/tags/${name}`);
  if (!hash) return null;
  
  // Check if it's an annotated tag (tag object) or lightweight (direct commit hash)
  try {
    const obj = readObject(gitDir, hash);
    if (obj.type === 'tag') {
      // Parse tag to get target
      const content = obj.content.toString();
      const match = content.match(/^object ([0-9a-f]{40})/);
      return match ? match[1] : hash;
    }
  } catch {}
  
  return hash;
}

/**
 * Delete a tag.
 */
export function deleteTag(gitDir, name) {
  unlinkSync(join(gitDir, 'refs', 'tags', name));
}
