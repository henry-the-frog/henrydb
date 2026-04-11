// huffman/test.js — Comprehensive test suite
import { MinHeap, buildFrequencyTable, buildTree, generateCodes,
        encode, decode, serializeTree, deserializeTree,
        compressionRatio, compress, decompress } from './huffman.js';

let passed = 0, failed = 0, total = 0;

function test(name, fn) {
  total++;
  try {
    fn();
    passed++;
  } catch (e) {
    failed++;
    console.log(`  FAIL: ${name}`);
    console.log(`    ${e.message}`);
  }
}

function eq(a, b, msg = '') {
  if (JSON.stringify(a) !== JSON.stringify(b)) {
    throw new Error(`Expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}${msg ? ' — ' + msg : ''}`);
  }
}

function assert(cond, msg = '') {
  if (!cond) throw new Error(`Assertion failed${msg ? ': ' + msg : ''}`);
}

// ═══════════════════════════════════════════
// MinHeap
// ═══════════════════════════════════════════
console.log('── MinHeap ──');

test('heap push/pop order', () => {
  const h = new MinHeap();
  h.push({ freq: 5 });
  h.push({ freq: 1 });
  h.push({ freq: 3 });
  eq(h.pop().freq, 1);
  eq(h.pop().freq, 3);
  eq(h.pop().freq, 5);
});

test('heap single element', () => {
  const h = new MinHeap();
  h.push({ freq: 42 });
  eq(h.pop().freq, 42);
  eq(h.pop(), null);
});

test('heap size', () => {
  const h = new MinHeap();
  eq(h.size, 0);
  h.push({ freq: 1 });
  eq(h.size, 1);
  h.push({ freq: 2 });
  eq(h.size, 2);
  h.pop();
  eq(h.size, 1);
});

test('heap many elements', () => {
  const h = new MinHeap();
  const vals = [10, 4, 15, 20, 0, 7, 13, 2, 8];
  for (const v of vals) h.push({ freq: v });
  const sorted = [];
  while (h.size > 0) sorted.push(h.pop().freq);
  eq(sorted, [...vals].sort((a, b) => a - b));
});

// ═══════════════════════════════════════════
// Frequency Table
// ═══════════════════════════════════════════
console.log('── Frequency Table ──');

test('frequency table basic', () => {
  const freq = buildFrequencyTable('aabbc');
  eq(freq.get('a'), 2);
  eq(freq.get('b'), 2);
  eq(freq.get('c'), 1);
  eq(freq.size, 3);
});

test('frequency table single char', () => {
  const freq = buildFrequencyTable('aaaa');
  eq(freq.get('a'), 4);
  eq(freq.size, 1);
});

test('frequency table empty', () => {
  const freq = buildFrequencyTable('');
  eq(freq.size, 0);
});

// ═══════════════════════════════════════════
// Tree Construction
// ═══════════════════════════════════════════
console.log('── Tree Construction ──');

test('tree from frequencies', () => {
  const freq = buildFrequencyTable('aabbc');
  const tree = buildTree(freq);
  assert(tree !== null);
  eq(tree.freq, 5);
  eq(tree.char, null);
});

test('tree single char', () => {
  const freq = buildFrequencyTable('aaaa');
  const tree = buildTree(freq);
  assert(tree !== null);
  eq(tree.freq, 4);
  // Single char should have the char as left child
  assert(tree.left !== null);
  eq(tree.left.char, 'a');
});

test('tree null for empty', () => {
  eq(buildTree(new Map()), null);
});

// ═══════════════════════════════════════════
// Code Generation
// ═══════════════════════════════════════════
console.log('── Code Generation ──');

test('codes are prefix-free', () => {
  const freq = buildFrequencyTable('aaabbbccddddee');
  const tree = buildTree(freq);
  const codes = generateCodes(tree);
  const codeList = [...codes.values()];
  // No code should be a prefix of another
  for (let i = 0; i < codeList.length; i++) {
    for (let j = 0; j < codeList.length; j++) {
      if (i !== j) {
        assert(!codeList[j].startsWith(codeList[i]),
          `Code '${codeList[i]}' is prefix of '${codeList[j]}'`);
      }
    }
  }
});

test('codes are all 0s and 1s', () => {
  const freq = buildFrequencyTable('hello world');
  const tree = buildTree(freq);
  const codes = generateCodes(tree);
  for (const code of codes.values()) {
    assert(/^[01]+$/.test(code), `Invalid code: ${code}`);
  }
});

test('single char code', () => {
  const freq = buildFrequencyTable('aaaa');
  const tree = buildTree(freq);
  const codes = generateCodes(tree);
  eq(codes.get('a'), '0');
});

test('two chars get 0 and 1', () => {
  const freq = buildFrequencyTable('ab');
  const tree = buildTree(freq);
  const codes = generateCodes(tree);
  eq(codes.size, 2);
  const vals = [...codes.values()].sort();
  eq(vals, ['0', '1']);
});

// ═══════════════════════════════════════════
// Encode / Decode Roundtrip
// ═══════════════════════════════════════════
console.log('── Encode/Decode ──');

test('roundtrip simple', () => {
  const text = 'hello';
  const { bits, tree } = encode(text);
  eq(decode(bits, tree), text);
});

test('roundtrip repeated chars', () => {
  const text = 'aaabbbccc';
  const { bits, tree } = encode(text);
  eq(decode(bits, tree), text);
});

test('roundtrip single char', () => {
  const text = 'aaaaaa';
  const { bits, tree } = encode(text);
  eq(decode(bits, tree), text);
});

test('roundtrip empty', () => {
  const { bits, tree } = encode('');
  eq(decode(bits, tree), '');
});

test('roundtrip pangram', () => {
  const text = 'the quick brown fox jumps over the lazy dog';
  const { bits, tree } = encode(text);
  eq(decode(bits, tree), text);
});

test('roundtrip all printable ASCII', () => {
  let text = '';
  for (let i = 32; i < 127; i++) text += String.fromCharCode(i);
  const { bits, tree } = encode(text);
  eq(decode(bits, tree), text);
});

test('roundtrip long text', () => {
  const text = 'abcdefghij'.repeat(100);
  const { bits, tree } = encode(text);
  eq(decode(bits, tree), text);
});

test('roundtrip Lorem ipsum', () => {
  const text = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.';
  const { bits, tree } = encode(text);
  eq(decode(bits, tree), text);
});

test('roundtrip binary-like text', () => {
  const text = '0110100101101001';
  const { bits, tree } = encode(text);
  eq(decode(bits, tree), text);
});

test('roundtrip special characters', () => {
  const text = 'hello\nworld\ttab!@#$%^&*()';
  const { bits, tree } = encode(text);
  eq(decode(bits, tree), text);
});

// ═══════════════════════════════════════════
// Tree Serialization
// ═══════════════════════════════════════════
console.log('── Tree Serialization ──');

test('serialize/deserialize roundtrip', () => {
  const text = 'hello world';
  const { tree } = encode(text);
  const serialized = serializeTree(tree);
  const restored = deserializeTree(serialized);
  // Decode with restored tree should work
  const { bits } = encode(text);
  eq(decode(bits, restored), text);
});

test('serialize empty tree', () => {
  eq(serializeTree(null), '');
});

test('full compress/decompress', () => {
  const text = 'the quick brown fox jumps over the lazy dog';
  const compressed = compress(text);
  eq(decompress(compressed), text);
});

test('full compress/decompress single char', () => {
  eq(decompress(compress('aaaa')), 'aaaa');
});

// ═══════════════════════════════════════════
// Compression Quality
// ═══════════════════════════════════════════
console.log('── Compression Quality ──');

test('compression ratio for repeated text', () => {
  const text = 'a'.repeat(1000);
  const { bits } = encode(text);
  const ratio = compressionRatio(text, { bits });
  assert(ratio < 0.2, `Expected good compression for repeated char, got ratio ${ratio}`);
});

test('compression ratio for English text', () => {
  const text = 'the quick brown fox jumps over the lazy dog. '.repeat(10);
  const { bits } = encode(text);
  const ratio = compressionRatio(text, { bits });
  assert(ratio < 1.0, `Expected compression, got ratio ${ratio}`);
  console.log(`    English text compression ratio: ${(ratio * 100).toFixed(1)}%`);
});

test('uniform distribution has no compression', () => {
  // All chars equally likely — Huffman can't do better than ~log2(n) bits per char
  let text = '';
  for (let i = 0; i < 256; i++) text += String.fromCharCode(i);
  const { bits } = encode(text);
  const ratio = compressionRatio(text, { bits });
  // Should be close to 1.0 (8 bits per char ≈ log2(256))
  assert(ratio > 0.8, `Expected ~1.0 ratio for uniform dist, got ${ratio}`);
});

test('highly skewed distribution compresses well', () => {
  // 99% 'a', 1% 'b'
  const text = 'a'.repeat(990) + 'b'.repeat(10);
  const { bits } = encode(text);
  const ratio = compressionRatio(text, { bits });
  assert(ratio < 0.2, `Expected good compression for skewed dist, got ${ratio}`);
  console.log(`    Skewed (99/1) compression ratio: ${(ratio * 100).toFixed(1)}%`);
});

// ═══════════════════════════════════════════
// Edge Cases
// ═══════════════════════════════════════════
console.log('── Edge Cases ──');

test('two character alphabet', () => {
  const text = 'ababababab';
  const { bits, tree, codes } = encode(text);
  eq(codes.size, 2);
  eq(decode(bits, tree), text);
});

test('unicode characters', () => {
  const text = '日本語テスト';
  const { bits, tree } = encode(text);
  eq(decode(bits, tree), text);
});

test('single character string', () => {
  const text = 'x';
  const { bits, tree } = encode(text);
  eq(decode(bits, tree), text);
});

test('frequency proportional to code length', () => {
  // More frequent chars should get shorter codes
  const text = 'a'.repeat(100) + 'b'.repeat(50) + 'c'.repeat(10) + 'd'.repeat(1);
  const { codes } = encode(text);
  assert(codes.get('a').length <= codes.get('b').length, 'a should have shorter or equal code than b');
  assert(codes.get('b').length <= codes.get('c').length, 'b should have shorter or equal code than c');
  assert(codes.get('c').length <= codes.get('d').length, 'c should have shorter or equal code than d');
});

// ═══════════════════════════════════════════

console.log(`\n══════════════════════════════`);
console.log(`  ${passed}/${total} passed, ${failed} failed`);
console.log(`══════════════════════════════`);
process.exit(failed > 0 ? 1 : 0);
