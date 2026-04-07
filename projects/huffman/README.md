# Huffman Coding

Huffman compression built from scratch in JavaScript — frequency analysis, priority queue, optimal prefix-free codes, encode/decode with tree serialization.

**Zero dependencies. Pure information theory.**

## How It Works

1. **Frequency analysis** — Count character occurrences
2. **Build Huffman tree** — Min-heap priority queue merges least frequent nodes
3. **Generate codes** — Walk tree: left = '0', right = '1'. Prefix-free by construction
4. **Encode** — Replace each character with its binary code
5. **Decode** — Walk tree bit by bit, emit character at leaves

## Usage

```javascript
const { compress, decompress, encode, compressionRatio } = require('./huffman.js');

// Simple compress/decompress
const compressed = compress('hello world');
const original = decompress(compressed);  // 'hello world'

// Detailed encoding
const { bits, tree, codes } = encode('hello world');
console.log(codes);  // Map { 'h' → '1100', 'e' → '1101', ... }
console.log(compressionRatio('hello world', { bits }));  // 0.55
```

## Compression Results

| Input | Ratio |
|-------|-------|
| English text (450 chars) | **55.8%** |
| Skewed distribution (99% 'a') | **12.5%** |
| Single character repeated | **< 2%** |
| Uniform distribution (all ASCII) | **~100%** (can't compress) |

## Tests

```
36 tests | 0 failures
```

Covers: min-heap, frequency table, tree construction, prefix-free code generation, encode/decode roundtrip (simple, repeated, single char, pangram, ASCII, lorem ipsum, special chars, unicode), tree serialization, compression quality, and edge cases.

## Files

```
huffman.js  — MinHeap, tree, codes, encode/decode, serialize
test.js     — 36 tests
README.md   — This file
```
