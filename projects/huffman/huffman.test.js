import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  MinHeap, buildFrequencyTable, buildTree, generateCodes,
  encode, decode, serializeTree, deserializeTree,
  compressionRatio, compress, decompress,
} from './huffman.js';

describe('MinHeap', () => {
  it('pushes and pops in order', () => {
    const h = new MinHeap();
    h.push({ freq: 5 });
    h.push({ freq: 2 });
    h.push({ freq: 8 });
    h.push({ freq: 1 });
    assert.equal(h.pop().freq, 1);
    assert.equal(h.pop().freq, 2);
    assert.equal(h.pop().freq, 5);
    assert.equal(h.pop().freq, 8);
  });

  it('handles single element', () => {
    const h = new MinHeap();
    h.push({ freq: 42 });
    assert.equal(h.pop().freq, 42);
    assert.equal(h.pop(), null);
  });

  it('handles duplicate priorities', () => {
    const h = new MinHeap();
    h.push({ freq: 3, ch: 'a' });
    h.push({ freq: 3, ch: 'b' });
    h.push({ freq: 3, ch: 'c' });
    assert.equal(h.pop().freq, 3);
    assert.equal(h.pop().freq, 3);
    assert.equal(h.pop().freq, 3);
  });
});

describe('Frequency Table', () => {
  it('counts character frequencies', () => {
    const freq = buildFrequencyTable('hello');
    assert.equal(freq.get('h'), 1);
    assert.equal(freq.get('e'), 1);
    assert.equal(freq.get('l'), 2);
    assert.equal(freq.get('o'), 1);
  });

  it('single character', () => {
    const freq = buildFrequencyTable('aaaa');
    assert.equal(freq.get('a'), 4);
    assert.equal(freq.size, 1);
  });

  it('empty string', () => {
    const freq = buildFrequencyTable('');
    assert.equal(freq.size, 0);
  });
});

describe('Tree Building', () => {
  it('builds tree from frequency table', () => {
    const freq = buildFrequencyTable('aabbc');
    const tree = buildTree(freq);
    assert(tree !== null);
    assert.equal(tree.freq, 5);
  });

  it('single character tree', () => {
    const freq = buildFrequencyTable('aaaa');
    const tree = buildTree(freq);
    assert(tree !== null);
  });
});

describe('Code Generation', () => {
  it('generates codes for each character', () => {
    const freq = buildFrequencyTable('aabbc');
    const tree = buildTree(freq);
    const codes = generateCodes(tree);
    assert(codes.has('a'));
    assert(codes.has('b'));
    assert(codes.has('c'));
    for (const code of codes.values()) {
      assert(/^[01]+$/.test(code));
    }
  });

  it('more frequent chars get shorter or equal codes', () => {
    const freq = buildFrequencyTable('aaaaabc');
    const tree = buildTree(freq);
    const codes = generateCodes(tree);
    assert(codes.get('a').length <= codes.get('b').length);
    assert(codes.get('a').length <= codes.get('c').length);
  });

  it('all codes are prefix-free', () => {
    const freq = buildFrequencyTable('abcdefgh');
    const tree = buildTree(freq);
    const codes = generateCodes(tree);
    const values = [...codes.values()];
    for (let i = 0; i < values.length; i++) {
      for (let j = 0; j < values.length; j++) {
        if (i !== j) {
          assert(!values[j].startsWith(values[i]),
            `"${values[j]}" is prefixed by "${values[i]}"`);
        }
      }
    }
  });
});

describe('Encode/Decode', () => {
  it('round-trip: hello', () => {
    const freq = buildFrequencyTable('hello');
    const tree = buildTree(freq);
    const codes = generateCodes(tree);
    const encoded = encode('hello', codes);
    const decoded = decode(encoded.bits, tree);
    assert.equal(decoded, 'hello');
  });

  it('round-trip: repeated characters', () => {
    const text = 'aaabbbccc';
    const freq = buildFrequencyTable(text);
    const tree = buildTree(freq);
    const codes = generateCodes(tree);
    const encoded = encode(text, codes);
    const decoded = decode(encoded.bits, tree);
    assert.equal(decoded, text);
  });

  it('round-trip: long text', () => {
    const text = 'the quick brown fox jumps over the lazy dog';
    const freq = buildFrequencyTable(text);
    const tree = buildTree(freq);
    const codes = generateCodes(tree);
    const encoded = encode(text, codes);
    const decoded = decode(encoded.bits, tree);
    assert.equal(decoded, text);
  });

  it('encoded bits shorter than 8-bit ASCII', () => {
    const text = 'aaaaaaaaaabbc';
    const freq = buildFrequencyTable(text);
    const tree = buildTree(freq);
    const codes = generateCodes(tree);
    const encoded = encode(text, codes);
    assert(encoded.bits.length < text.length * 8);
  });
});

describe('Tree Serialization', () => {
  it('round-trip serialization', () => {
    const freq = buildFrequencyTable('hello world');
    const tree = buildTree(freq);
    const serialized = serializeTree(tree);
    const deserialized = deserializeTree(serialized);
    const codes1 = generateCodes(tree);
    const codes2 = generateCodes(deserialized);
    assert.deepEqual(codes1, codes2);
  });
});

describe('Compress/Decompress', () => {
  it('round-trip', () => {
    const text = 'hello world, this is a test of huffman compression!';
    assert.equal(decompress(compress(text)), text);
  });

  it('handles special characters', () => {
    const text = 'Hello, World! 123 @#$';
    assert.equal(decompress(compress(text)), text);
  });

  it('handles repeated text', () => {
    const text = 'ab'.repeat(50);
    assert.equal(decompress(compress(text)), text);
  });

  it('single character', () => {
    assert.equal(decompress(compress('aaaa')), 'aaaa');
  });

  it('two characters', () => {
    assert.equal(decompress(compress('ab')), 'ab');
  });
});
