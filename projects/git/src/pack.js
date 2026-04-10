// pack.js — Git pack format (simplified)
// Pack files allow efficient transfer of objects between repositories
// Format: PACK + version(4) + num_objects(4) + entries... + checksum(20)

import { createHash } from 'crypto';
import { deflateSync, inflateSync } from 'zlib';
import { readObject, writeObject, hashObject, objectExists, parseCommit, parseTree } from './objects.js';

/**
 * Create a pack containing the given object hashes.
 * Returns a Buffer containing the pack data.
 */
export function createPack(gitDir, hashes) {
  const objects = [];
  
  for (const hash of hashes) {
    const obj = readObject(gitDir, hash);
    objects.push({ hash, type: obj.type, content: obj.content });
  }
  
  // Pack header: PACK + version(2) + num_objects
  const header = Buffer.alloc(12);
  header.write('PACK', 0);
  header.writeUInt32BE(2, 4); // Version 2
  header.writeUInt32BE(objects.length, 8);
  
  const parts = [header];
  
  for (const obj of objects) {
    // Encode object type and size
    const typeNum = typeToNum(obj.type);
    const size = obj.content.length;
    
    // Variable-length encoding: first byte has type + 4 bits of size
    // Subsequent bytes have 7 bits of size each
    const sizeBytes = encodeMSB(typeNum, size);
    parts.push(sizeBytes);
    
    // Compress the content
    parts.push(deflateSync(obj.content));
  }
  
  const data = Buffer.concat(parts);
  
  // SHA-1 checksum of entire pack
  const checksum = createHash('sha1').update(data).digest();
  
  return Buffer.concat([data, checksum]);
}

/**
 * Unpack a pack file into an object store.
 * Returns an array of { hash, type } for all objects unpacked.
 */
export function unpack(gitDir, packData) {
  let offset = 0;
  
  // Parse header
  const magic = packData.toString('ascii', 0, 4);
  if (magic !== 'PACK') throw new Error('Invalid pack file');
  
  const version = packData.readUInt32BE(4);
  if (version !== 2) throw new Error(`Unsupported pack version: ${version}`);
  
  const numObjects = packData.readUInt32BE(8);
  offset = 12;
  
  const results = [];
  
  for (let i = 0; i < numObjects; i++) {
    const { type, size, bytesRead } = decodeMSB(packData, offset);
    offset += bytesRead;
    
    // Find the end of the compressed data by trying to inflate
    let content;
    let compressedEnd = offset;
    for (let tryLen = size; tryLen <= packData.length - offset; tryLen += 64) {
      try {
        content = inflateSync(packData.subarray(offset, offset + tryLen));
        compressedEnd = offset + tryLen;
        break;
      } catch {
        continue;
      }
    }
    
    if (!content) {
      // Try inflating from offset to end of pack data (before checksum)
      content = inflateSync(packData.subarray(offset, packData.length - 20));
      compressedEnd = packData.length - 20;
    }
    
    const typeName = numToType(type);
    const hash = writeObject(gitDir, typeName, content);
    results.push({ hash, type: typeName });
    
    // Move past the compressed data
    // zlib inflate consumed exactly what it needed; we need to find how much
    const compressed = deflateSync(content);
    offset += compressed.length;
  }
  
  return results;
}

/**
 * Enumerate all objects reachable from the given commit hashes.
 * Used for pack generation — returns all hashes needed.
 */
export function enumerateObjects(gitDir, commitHashes) {
  const visited = new Set();
  const queue = [...commitHashes];
  
  while (queue.length > 0) {
    const hash = queue.shift();
    if (visited.has(hash)) continue;
    if (!objectExists(gitDir, hash)) continue;
    visited.add(hash);
    
    const obj = readObject(gitDir, hash);
    
    if (obj.type === 'commit') {
      const commitData = parseCommit(obj.content);
      if (commitData.tree) queue.push(commitData.tree);
      for (const parent of commitData.parents) queue.push(parent);
    } else if (obj.type === 'tree') {
      const entries = parseTree(obj.content);
      for (const entry of entries) queue.push(entry.hash);
    }
    // Blobs and tags don't reference other objects
  }
  
  return [...visited];
}

function typeToNum(type) {
  switch (type) {
    case 'commit': return 1;
    case 'tree': return 2;
    case 'blob': return 3;
    case 'tag': return 4;
    default: throw new Error(`Unknown object type: ${type}`);
  }
}

function numToType(num) {
  switch (num) {
    case 1: return 'commit';
    case 2: return 'tree';
    case 3: return 'blob';
    case 4: return 'tag';
    default: throw new Error(`Unknown type number: ${num}`);
  }
}

/**
 * Encode type and size into variable-length MSB format.
 */
function encodeMSB(type, size) {
  const bytes = [];
  
  // First byte: MSB=continuation, bits 6-4=type, bits 3-0=size[3:0]
  let firstByte = (type << 4) | (size & 0x0F);
  size >>= 4;
  
  if (size > 0) firstByte |= 0x80;
  bytes.push(firstByte);
  
  while (size > 0) {
    let byte = size & 0x7F;
    size >>= 7;
    if (size > 0) byte |= 0x80;
    bytes.push(byte);
  }
  
  return Buffer.from(bytes);
}

/**
 * Decode type and size from variable-length MSB format.
 */
function decodeMSB(buf, offset) {
  let byte = buf[offset];
  const type = (byte >> 4) & 0x07;
  let size = byte & 0x0F;
  let shift = 4;
  let bytesRead = 1;
  
  while (byte & 0x80) {
    byte = buf[offset + bytesRead];
    size |= (byte & 0x7F) << shift;
    shift += 7;
    bytesRead++;
  }
  
  return { type, size, bytesRead };
}
