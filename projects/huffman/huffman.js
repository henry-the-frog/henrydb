// huffman/huffman.js — Huffman coding from scratch
// ─── Priority Queue (min-heap) ───
class MinHeap {
  constructor() { this.data = []; }

  push(item) {
    this.data.push(item);
    this._bubbleUp(this.data.length - 1);
  }

  pop() {
    if (this.data.length === 0) return null;
    const min = this.data[0];
    const last = this.data.pop();
    if (this.data.length > 0) {
      this.data[0] = last;
      this._sinkDown(0);
    }
    return min;
  }

  get size() { return this.data.length; }

  _bubbleUp(i) {
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (this.data[i].freq < this.data[parent].freq) {
        [this.data[i], this.data[parent]] = [this.data[parent], this.data[i]];
        i = parent;
      } else break;
    }
  }

  _sinkDown(i) {
    const n = this.data.length;
    while (true) {
      let smallest = i;
      const left = 2 * i + 1, right = 2 * i + 2;
      if (left < n && this.data[left].freq < this.data[smallest].freq) smallest = left;
      if (right < n && this.data[right].freq < this.data[smallest].freq) smallest = right;
      if (smallest !== i) {
        [this.data[i], this.data[smallest]] = [this.data[smallest], this.data[i]];
        i = smallest;
      } else break;
    }
  }
}

// ─── Huffman Tree ───
// Leaf: { char, freq }
// Internal: { left, right, freq }

function buildFrequencyTable(text) {
  const freq = new Map();
  for (const ch of text) {
    freq.set(ch, (freq.get(ch) || 0) + 1);
  }
  return freq;
}

function buildTree(freqTable) {
  if (freqTable.size === 0) return null;

  const heap = new MinHeap();
  for (const [char, freq] of freqTable) {
    heap.push({ char, freq, left: null, right: null });
  }

  // Special case: single character
  if (heap.size === 1) {
    const node = heap.pop();
    return { char: null, freq: node.freq, left: node, right: null };
  }

  while (heap.size > 1) {
    const left = heap.pop();
    const right = heap.pop();
    heap.push({
      char: null,
      freq: left.freq + right.freq,
      left,
      right,
    });
  }

  return heap.pop();
}

function generateCodes(tree) {
  const codes = new Map();
  if (!tree) return codes;

  function walk(node, prefix) {
    if (node.char !== null) {
      codes.set(node.char, prefix || '0'); // Single char gets code '0'
      return;
    }
    if (node.left) walk(node.left, prefix + '0');
    if (node.right) walk(node.right, prefix + '1');
  }

  walk(tree, '');
  return codes;
}

// ─── Encode ───
function encode(text) {
  if (text.length === 0) return { bits: '', tree: null, freqTable: new Map() };

  const freqTable = buildFrequencyTable(text);
  const tree = buildTree(freqTable);
  const codes = generateCodes(tree);

  let bits = '';
  for (const ch of text) {
    bits += codes.get(ch);
  }

  return { bits, tree, freqTable, codes };
}

// ─── Decode ───
function decode(bits, tree) {
  if (!tree) return '';
  if (bits.length === 0) return '';

  // Single character tree
  if (tree.left && tree.left.char !== null && !tree.right) {
    return tree.left.char.repeat(bits.length);
  }

  let result = '';
  let node = tree;
  for (const bit of bits) {
    node = bit === '0' ? node.left : node.right;
    if (node.char !== null) {
      result += node.char;
      node = tree;
    }
  }
  return result;
}

// ─── Serialize / Deserialize Tree ───
function serializeTree(tree) {
  if (!tree) return '';
  if (tree.char !== null) return `1${tree.char}`;
  return `0${serializeTree(tree.left)}${serializeTree(tree.right)}`;
}

function deserializeTree(data) {
  let pos = 0;
  function read() {
    if (pos >= data.length) return null;
    const marker = data[pos++];
    if (marker === '1') {
      const char = data[pos++];
      return { char, freq: 0, left: null, right: null };
    }
    const left = read();
    const right = read();
    return { char: null, freq: 0, left, right };
  }
  return read();
}

// ─── Compression Stats ───
function compressionRatio(original, encoded) {
  const originalBits = original.length * 8;
  const encodedBits = encoded.bits.length;
  return originalBits > 0 ? encodedBits / originalBits : 0;
}

// ─── Full Compress/Decompress (with serialized tree) ───
function compress(text) {
  const { bits, tree } = encode(text);
  const serialized = serializeTree(tree);
  return { bits, treeSerialized: serialized, originalLength: text.length };
}

function decompress({ bits, treeSerialized }) {
  const tree = deserializeTree(treeSerialized);
  return decode(bits, tree);
}

export { MinHeap, buildFrequencyTable, buildTree, generateCodes, encode, decode,
         serializeTree, deserializeTree, compressionRatio, compress, decompress };
