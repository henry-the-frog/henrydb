// regex-engine/test.js — Comprehensive test suite
'use strict';

const { Regex, parse, astToNfa, nfaToDfa, minimizeDfa, nfaMatch, Parser } = require('./regex.js');

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

function assert(cond, msg = '') {
  if (!cond) throw new Error(`Assertion failed${msg ? ': ' + msg : ''}`);
}

function eq(a, b, msg = '') {
  if (a !== b) throw new Error(`Expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}${msg ? ' — ' + msg : ''}`);
}

// ═══════════════════════════════════════════
// Parser Tests
// ═══════════════════════════════════════════
console.log('── Parser ──');

test('parse literal', () => {
  const ast = parse('abc');
  eq(ast.type, 'cat');
  eq(ast.left.type, 'cat');
  eq(ast.left.left.type, 'lit');
  eq(ast.left.left.ch, 'a');
});

test('parse alternation', () => {
  const ast = parse('a|b');
  eq(ast.type, 'alt');
  eq(ast.left.ch, 'a');
  eq(ast.right.ch, 'b');
});

test('parse star', () => {
  const ast = parse('a*');
  eq(ast.type, 'star');
  eq(ast.child.ch, 'a');
});

test('parse plus', () => {
  const ast = parse('a+');
  eq(ast.type, 'plus');
  eq(ast.child.ch, 'a');
});

test('parse optional', () => {
  const ast = parse('a?');
  eq(ast.type, 'opt');
  eq(ast.child.ch, 'a');
});

test('parse group', () => {
  const ast = parse('(ab)');
  eq(ast.type, 'group');
  eq(ast.index, 1);
  eq(ast.child.type, 'cat');
});

test('parse dot', () => {
  const ast = parse('.');
  eq(ast.type, 'dot');
});

test('parse character class', () => {
  const ast = parse('[abc]');
  eq(ast.type, 'class');
  eq(ast.negated, false);
  eq(ast.ranges.length, 3);
});

test('parse negated class', () => {
  const ast = parse('[^abc]');
  eq(ast.type, 'class');
  eq(ast.negated, true);
});

test('parse range class', () => {
  const ast = parse('[a-z]');
  eq(ast.type, 'class');
  eq(ast.ranges[0][0], 'a');
  eq(ast.ranges[0][1], 'z');
});

test('parse anchor ^', () => {
  const ast = parse('^a');
  eq(ast.type, 'cat');
  eq(ast.left.type, 'anchor');
  eq(ast.left.kind, 'start');
});

test('parse anchor $', () => {
  const ast = parse('a$');
  eq(ast.type, 'cat');
  eq(ast.right.type, 'anchor');
  eq(ast.right.kind, 'end');
});

test('parse escape \\d', () => {
  const ast = parse('\\d');
  eq(ast.type, 'class');
  eq(ast.negated, false);
});

test('parse escape \\w', () => {
  const ast = parse('\\w');
  eq(ast.type, 'class');
});

test('parse escape \\s', () => {
  const ast = parse('\\s');
  eq(ast.type, 'class');
});

test('parse escaped dot', () => {
  const ast = parse('\\.');
  eq(ast.type, 'lit');
  eq(ast.ch, '.');
});

test('parse counted repetition {3}', () => {
  const ast = parse('a{3}');
  eq(ast.type, 'rep');
  eq(ast.min, 3);
  eq(ast.max, 3);
});

test('parse counted repetition {2,5}', () => {
  const ast = parse('a{2,5}');
  eq(ast.type, 'rep');
  eq(ast.min, 2);
  eq(ast.max, 5);
});

test('parse counted repetition {3,}', () => {
  const ast = parse('a{3,}');
  eq(ast.type, 'rep');
  eq(ast.min, 3);
  eq(ast.max, Infinity);
});

test('parse precedence: cat before alt', () => {
  const ast = parse('ab|cd');
  eq(ast.type, 'alt');
  eq(ast.left.type, 'cat');
  eq(ast.right.type, 'cat');
});

test('parse lazy quantifiers', () => {
  eq(parse('a*?').lazy, true);
  eq(parse('a+?').lazy, true);
  eq(parse('a??').lazy, true);
});

test('parse nested groups', () => {
  const ast = parse('((a)(b))');
  eq(ast.type, 'group');
  eq(ast.index, 1);
  eq(ast.child.type, 'cat');
});

test('parser group count', () => {
  const p = new Parser('(a)(b)(c)');
  p.parse();
  eq(p.groupCount, 3);
});

test('parse empty alternation branch', () => {
  const ast = parse('a|');
  eq(ast.type, 'alt');
  eq(ast.right.type, 'empty');
});

// ═══════════════════════════════════════════
// NFA Match Tests — Basic
// ═══════════════════════════════════════════
console.log('── NFA Match (basic) ──');

test('literal match', () => {
  const r = new Regex('abc');
  assert(r.test('abc'));
  assert(!r.test('ab'));
  assert(!r.test('abcd'));
  assert(!r.test('xyz'));
});

test('empty pattern', () => {
  const r = new Regex('');
  assert(r.test(''));
  assert(!r.test('a'));
});

test('dot matches any', () => {
  const r = new Regex('a.c');
  assert(r.test('abc'));
  assert(r.test('axc'));
  assert(r.test('a c'));
  assert(!r.test('ac'));
  assert(!r.test('a\nc'), 'dot should not match newline');
});

test('alternation', () => {
  const r = new Regex('cat|dog');
  assert(r.test('cat'));
  assert(r.test('dog'));
  assert(!r.test('ca'));
  assert(!r.test('catdog'));
});

test('Kleene star', () => {
  const r = new Regex('ab*c');
  assert(r.test('ac'));
  assert(r.test('abc'));
  assert(r.test('abbc'));
  assert(r.test('abbbbbc'));
  assert(!r.test('abbb'));
});

test('plus', () => {
  const r = new Regex('ab+c');
  assert(!r.test('ac'));
  assert(r.test('abc'));
  assert(r.test('abbc'));
});

test('optional', () => {
  const r = new Regex('colou?r');
  assert(r.test('color'));
  assert(r.test('colour'));
  assert(!r.test('colouur'));
});

test('grouping', () => {
  const r = new Regex('(ab)+');
  assert(r.test('ab'));
  assert(r.test('abab'));
  assert(r.test('ababab'));
  assert(!r.test(''));
  assert(!r.test('a'));
});

test('nested alternation in group', () => {
  const r = new Regex('(a|b)(c|d)');
  assert(r.test('ac'));
  assert(r.test('ad'));
  assert(r.test('bc'));
  assert(r.test('bd'));
  assert(!r.test('ab'));
});

// ═══════════════════════════════════════════
// Character Classes
// ═══════════════════════════════════════════
console.log('── Character Classes ──');

test('simple class [abc]', () => {
  const r = new Regex('[abc]');
  assert(r.test('a'));
  assert(r.test('b'));
  assert(r.test('c'));
  assert(!r.test('d'));
  assert(!r.test(''));
});

test('range class [a-z]', () => {
  const r = new Regex('[a-z]');
  assert(r.test('a'));
  assert(r.test('m'));
  assert(r.test('z'));
  assert(!r.test('A'));
  assert(!r.test('0'));
});

test('negated class [^abc]', () => {
  const r = new Regex('[^abc]');
  assert(!r.test('a'));
  assert(!r.test('b'));
  assert(r.test('d'));
  assert(r.test('z'));
});

test('combined ranges [a-zA-Z0-9]', () => {
  const r = new Regex('[a-zA-Z0-9]');
  assert(r.test('a'));
  assert(r.test('Z'));
  assert(r.test('5'));
  assert(!r.test(' '));
  assert(!r.test('!'));
});

test('shorthand \\d', () => {
  const r = new Regex('\\d+');
  assert(r.test('123'));
  assert(r.test('0'));
  assert(!r.test('abc'));
  assert(!r.test(''));
});

test('shorthand \\w', () => {
  const r = new Regex('\\w+');
  assert(r.test('hello'));
  assert(r.test('a1_b'));
  assert(!r.test(' '));
  assert(!r.test(''));
});

test('shorthand \\s', () => {
  const r = new Regex('\\s+');
  assert(r.test(' '));
  assert(r.test('\t\n'));
  assert(!r.test('a'));
});

test('negated shorthand \\D', () => {
  const r = new Regex('\\D+');
  assert(r.test('abc'));
  assert(!r.test('123'));
});

test('negated shorthand \\W', () => {
  const r = new Regex('\\W');
  assert(r.test(' '));
  assert(r.test('!'));
  assert(!r.test('a'));
});

// ═══════════════════════════════════════════
// Anchors
// ═══════════════════════════════════════════
console.log('── Anchors ──');

test('start anchor ^', () => {
  const r = new Regex('^abc');
  assert(r.test('abc'));
  assert(!r.test('xabc'));
});

test('end anchor $', () => {
  const r = new Regex('abc$');
  assert(r.test('abc'));
  assert(!r.test('abcx'));
});

test('both anchors ^...$', () => {
  const r = new Regex('^abc$');
  assert(r.test('abc'));
  assert(!r.test('xabc'));
  assert(!r.test('abcx'));
});

test('anchored alternation', () => {
  const r = new Regex('^(yes|no)$');
  assert(r.test('yes'));
  assert(r.test('no'));
  assert(!r.test('maybe'));
});

// ═══════════════════════════════════════════
// Counted Repetition
// ═══════════════════════════════════════════
console.log('── Counted Repetition ──');

test('{3} exact', () => {
  const r = new Regex('a{3}');
  assert(!r.test('aa'));
  assert(r.test('aaa'));
  assert(!r.test('aaaa'));
});

test('{2,4} range', () => {
  const r = new Regex('a{2,4}');
  assert(!r.test('a'));
  assert(r.test('aa'));
  assert(r.test('aaa'));
  assert(r.test('aaaa'));
  assert(!r.test('aaaaa'));
});

test('{2,} unbounded', () => {
  const r = new Regex('a{2,}');
  assert(!r.test('a'));
  assert(r.test('aa'));
  assert(r.test('aaaa'));
  assert(r.test('aaaaaaaaa'));
});

test('{0,1} same as ?', () => {
  const r = new Regex('a{0,1}');
  assert(r.test(''));
  assert(r.test('a'));
  assert(!r.test('aa'));
});

// ═══════════════════════════════════════════
// Escape Sequences
// ═══════════════════════════════════════════
console.log('── Escape Sequences ──');

test('escaped special chars', () => {
  const r = new Regex('\\(a\\+b\\)');
  assert(r.test('(a+b)'));
  assert(!r.test('aab'));
});

test('escaped dot', () => {
  const r = new Regex('a\\.b');
  assert(r.test('a.b'));
  assert(!r.test('axb'));
});

test('escaped backslash', () => {
  const r = new Regex('a\\\\b');
  assert(r.test('a\\b'));
});

test('\\n \\t escapes', () => {
  const r = new Regex('a\\nb');
  assert(r.test('a\nb'));
  const r2 = new Regex('a\\tb');
  assert(r2.test('a\tb'));
});

// ═══════════════════════════════════════════
// DFA Construction & Minimization
// ═══════════════════════════════════════════
console.log('── DFA ──');

test('DFA matches same as NFA', () => {
  const patterns = ['abc', 'a|b|c', 'ab*c', '(a|b)*', '[a-z]+', 'a{2,4}'];
  const inputs = ['abc', 'a', 'b', 'c', 'ac', 'abbc', '', 'ababab', 'hello', 'aaa'];
  for (const pat of patterns) {
    const r = new Regex(pat);
    for (const inp of inputs) {
      const nfa = nfaMatch(r._nfa, inp);
      const result = r.test(inp);
      eq(result, nfa, `pattern=${pat}, input=${inp}`);
    }
  }
});

test('DFA minimization reduces states', () => {
  // a|b should minimize well
  const ast = parse('a|b');
  const nfa = astToNfa(ast);
  const dfa = nfaToDfa(nfa);
  const min = minimizeDfa(dfa);
  assert(min.states.length <= dfa.states.length, 'minimized should have <= states');
});

test('DFA handles complex patterns', () => {
  const r = new Regex('(ab|cd)*(ef|gh)');
  assert(r.test('ef'));
  assert(r.test('abef'));
  assert(r.test('cdgh'));
  assert(r.test('abcdabef'));
  assert(!r.test('ab'));
  assert(r.test('abcdef'));  // (ab)(cd)(ef) matches
});

// ═══════════════════════════════════════════
// Search & Find
// ═══════════════════════════════════════════
console.log('── Search & Find ──');

test('search finds first match', () => {
  const r = new Regex('\\d+');
  const m = r.search('abc 123 def');
  eq(m.match, '123');
  eq(m.index, 4);
});

test('search returns null if no match', () => {
  const r = new Regex('\\d+');
  eq(r.search('abc def'), null);
});

test('findAll', () => {
  const r = new Regex('\\d+');
  const matches = r.findAll('a1b23c456');
  eq(matches.length, 3);
  eq(matches[0].match, '1');
  eq(matches[1].match, '23');
  eq(matches[2].match, '456');
});

test('findAll no overlapping', () => {
  const r = new Regex('[a-z]+');
  const matches = r.findAll('hello world');
  eq(matches.length, 2);
  eq(matches[0].match, 'hello');
  eq(matches[1].match, 'world');
});

// ═══════════════════════════════════════════
// Replace
// ═══════════════════════════════════════════
console.log('── Replace ──');

test('replace first', () => {
  const r = new Regex('\\d+');
  eq(r.replace('a1b2c3', 'X'), 'aXb2c3');
});

test('replace all', () => {
  const r = new Regex('\\d+');
  eq(r.replace('a1b2c3', 'X', true), 'aXbXcX');
});

test('replace no match', () => {
  const r = new Regex('\\d+');
  eq(r.replace('hello', 'X'), 'hello');
});

// ═══════════════════════════════════════════
// Real-World Patterns
// ═══════════════════════════════════════════
console.log('── Real-World Patterns ──');

test('email-like pattern', () => {
  const r = new Regex('[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]+');
  assert(r.test('user@example.com'));
  assert(r.test('test123@mail.org'));
  assert(!r.test('user@'));
  assert(!r.test('@example.com'));
});

test('IP address pattern', () => {
  const r = new Regex('\\d+\\.\\d+\\.\\d+\\.\\d+');
  assert(r.test('192.168.1.1'));
  assert(r.test('10.0.0.1'));
  assert(!r.test('192.168.1'));
});

test('hex color', () => {
  const r = new Regex('#[0-9a-fA-F]{6}');
  assert(r.test('#ff00ff'));
  assert(r.test('#AABB00'));
  assert(!r.test('#fff'));
  assert(!r.test('ff00ff'));
});

test('phone-like pattern', () => {
  const r = new Regex('\\d{3}-\\d{3}-\\d{4}');
  assert(r.test('555-123-4567'));
  assert(!r.test('55-123-4567'));
});

test('URL-like pattern', () => {
  const r = new Regex('https?://[a-zA-Z0-9.]+(/[a-zA-Z0-9._-]*)*');
  assert(r.test('http://example.com'));
  assert(r.test('https://foo.bar.com/path/to'));
  assert(!r.test('ftp://example.com'));
});

test('identifier', () => {
  const r = new Regex('[a-zA-Z_][a-zA-Z0-9_]*');
  assert(r.test('hello'));
  assert(r.test('_private'));
  assert(r.test('camelCase123'));
  assert(!r.test('123abc'));
});

// ═══════════════════════════════════════════
// Edge Cases
// ═══════════════════════════════════════════
console.log('── Edge Cases ──');

test('star on empty matches empty', () => {
  const r = new Regex('(a|)*');
  assert(r.test(''));
  assert(r.test('a'));
  assert(r.test('aaa'));
});

test('nested quantifiers', () => {
  const r = new Regex('(a*)*');
  assert(r.test(''));
  assert(r.test('aaa'));
});

test('alternation with empty', () => {
  const r = new Regex('a|');
  assert(r.test(''));
  assert(r.test('a'));
  assert(!r.test('b'));
});

test('single character', () => {
  const r = new Regex('x');
  assert(r.test('x'));
  assert(!r.test('y'));
  assert(!r.test(''));
  assert(!r.test('xx'));
});

test('complex nesting', () => {
  const r = new Regex('((a|b)(c|d))+');
  assert(r.test('ac'));
  assert(r.test('bdac'));
  assert(!r.test('ab'));
  assert(!r.test(''));
});

test('class with dash at start (literal)', () => {
  const r = new Regex('[-az]');
  assert(r.test('a'));
  assert(r.test('-'));
  assert(r.test('z'));
  assert(!r.test('m'));
});

test('empty class bracket literal', () => {
  // ] as first char in class is literal
  const r = new Regex('[]]');
  assert(r.test(']'));
  assert(!r.test('a'));
});

test('multiple alternations', () => {
  const r = new Regex('a|b|c|d|e');
  assert(r.test('a'));
  assert(r.test('c'));
  assert(r.test('e'));
  assert(!r.test('f'));
});

test('quantifier on group with alternation', () => {
  const r = new Regex('(ab|cd){2}');
  assert(r.test('abab'));
  assert(r.test('abcd'));
  assert(r.test('cdab'));
  assert(r.test('cdcd'));
  assert(!r.test('ab'));
  assert(!r.test('ababab'));
});

test('deeply nested groups', () => {
  const r = new Regex('(((a)))');
  assert(r.test('a'));
  assert(!r.test('b'));
});

test('star of class', () => {
  const r = new Regex('[abc]*');
  assert(r.test(''));
  assert(r.test('a'));
  assert(r.test('abc'));
  assert(r.test('cba'));
  assert(!r.test('abcd'));
});

// ═══════════════════════════════════════════
// Performance sanity
// ═══════════════════════════════════════════
console.log('── Performance ──');

test('pathological pattern (a?)^n a^n with NFA', () => {
  // This is the classic exponential backtracking case
  // NFA simulation should handle it in O(n) per character
  const n = 25;
  const pattern = 'a?'.repeat(n) + 'a'.repeat(n);
  const input = 'a'.repeat(n);
  const r = new Regex(pattern);
  const start = Date.now();
  assert(r.test(input));
  const elapsed = Date.now() - start;
  assert(elapsed < 5000, `Should complete in <5s, took ${elapsed}ms`);
});

test('long literal string', () => {
  const s = 'abcdefghij'.repeat(10);
  const r = new Regex(s);
  assert(r.test(s));
  assert(!r.test(s + 'x'));
});

// ═══════════════════════════════════════════

// ═══════════════════════════════════════════
// Capturing Groups
// ═══════════════════════════════════════════
console.log('── Capturing Groups ──');

test('simple capture group', () => {
  const r = new Regex('(a)(b)(c)');
  const m = r.match('abc');
  assert(m !== null, 'should match');
  eq(m[0], 'a');
  eq(m[1], 'b');
  eq(m[2], 'c');
});

test('capture group with alternation', () => {
  const r = new Regex('(cat|dog) (food|water)');
  const m = r.match('cat food');
  assert(m !== null);
  eq(m[0], 'cat');
  eq(m[1], 'food');
});

test('capture group with quantifier', () => {
  const r = new Regex('(ab)+c');
  const m = r.match('ababc');
  assert(m !== null);
  // Last repetition captured
  eq(m[0], 'ab');
});

test('nested capture groups', () => {
  const r = new Regex('((a)(b))');
  const m = r.match('ab');
  assert(m !== null);
  eq(m[0], 'ab');
  eq(m[1], 'a');
  eq(m[2], 'b');
});

test('optional capture group', () => {
  const r = new Regex('a(b)?c');
  const m = r.match('ac');
  assert(m !== null);
  eq(m[0], undefined);
});

test('optional capture group present', () => {
  const r = new Regex('a(b)?c');
  const m = r.match('abc');
  assert(m !== null);
  eq(m[0], 'b');
});

test('capture with star', () => {
  const r = new Regex('(\\w+)@(\\w+)');
  const m = r.match('user@host');
  assert(m !== null);
  eq(m[0], 'user');
  eq(m[1], 'host');
});

test('match returns null on no match', () => {
  const r = new Regex('(a)b(c)');
  eq(r.match('xyz'), null);
});

test('no groups returns empty array', () => {
  const r = new Regex('abc');
  const m = r.match('abc');
  assert(Array.isArray(m));
  eq(m.length, 0);
});

// ═══════════════════════════════════════════
// More Edge Cases
// ═══════════════════════════════════════════
console.log('── More Edge Cases ──');

test('empty input empty pattern', () => {
  const r = new Regex('');
  assert(r.test(''));
});

test('star matches zero', () => {
  const r = new Regex('a*');
  assert(r.test(''));
  assert(r.test('a'));
  assert(r.test('aaa'));
});

test('complex character class', () => {
  const r = new Regex('[a-zA-Z_][a-zA-Z0-9_]*');
  assert(r.test('_foo'));
  assert(r.test('camelCase'));
  assert(!r.test('123'));
});

test('dot star', () => {
  const r = new Regex('a.*b');
  assert(r.test('ab'));
  assert(r.test('aXb'));
  assert(r.test('aXYZb'));
  assert(!r.test('a'));
  assert(!r.test('b'));
});

test('alternation of different lengths', () => {
  const r = new Regex('a|bb|ccc');
  assert(r.test('a'));
  assert(r.test('bb'));
  assert(r.test('ccc'));
  assert(!r.test('b'));
  assert(!r.test('cc'));
});

test('repeated group', () => {
  const r = new Regex('(abc){3}');
  assert(!r.test('abc'));
  assert(!r.test('abcabc'));
  assert(r.test('abcabcabc'));
});

test('\\d{4}-\\d{2}-\\d{2} date pattern', () => {
  const r = new Regex('\\d{4}-\\d{2}-\\d{2}');
  assert(r.test('2026-04-06'));
  assert(!r.test('26-4-6'));
  assert(!r.test('2026-4-06'));
});

test('complex URL pattern', () => {
  const r = new Regex('https?://[a-zA-Z0-9.-]+(/[a-zA-Z0-9._/-]*)?');
  assert(r.test('http://example.com'));
  assert(r.test('https://foo.bar.com/path'));
  assert(r.test('https://a.b.c.d/x/y/z'));
});

// ═══════════════════════════════════════════
// Performance Comparison
// ═══════════════════════════════════════════
console.log('── Performance vs Native ──');

test('performance: email-like pattern', () => {
  const pattern = '[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]+';
  const r = new Regex(pattern);
  const native = /^[a-zA-Z0-9]+@[a-zA-Z0-9]+\.[a-zA-Z]+$/;
  const inputs = ['user@example.com', 'invalid', 'a@b.c', 'test123@mail.org'];

  const start1 = performance.now();
  for (let i = 0; i < 1000; i++) for (const inp of inputs) r.test(inp);
  const custom = performance.now() - start1;

  const start2 = performance.now();
  for (let i = 0; i < 1000; i++) for (const inp of inputs) native.test(inp);
  const nativeTime = performance.now() - start2;

  console.log(`    Custom: ${custom.toFixed(1)}ms, Native: ${nativeTime.toFixed(1)}ms, Ratio: ${(custom/nativeTime).toFixed(1)}x`);
  assert(true); // informational
});

test('performance: pathological NFA vs backtracking', () => {
  // a?^n a^n — NFA handles in O(n²), backtracking takes O(2^n)
  const n = 20;
  const pat = 'a?'.repeat(n) + 'a'.repeat(n);
  const inp = 'a'.repeat(n);
  const r = new Regex(pat);
  const start = performance.now();
  const result = r.test(inp);
  const elapsed = performance.now() - start;
  assert(result);
  console.log(`    a?^${n} a^${n}: ${elapsed.toFixed(1)}ms (NFA simulation — no exponential blowup)`);
});

test('performance: long string literal', () => {
  const s = 'abcdefghijklmnopqrstuvwxyz'.repeat(20);
  const r = new Regex(s);
  const start = performance.now();
  for (let i = 0; i < 100; i++) r.test(s);
  const elapsed = performance.now() - start;
  console.log(`    520-char literal x100: ${elapsed.toFixed(1)}ms`);
  assert(true);
});

// ═══════════════════════════════════════════
// Additional pattern tests
// ═══════════════════════════════════════════
console.log('── Additional Patterns ──');

test('\\w+ matches words', () => {
  const r = new Regex('\\w+');
  const matches = r.findAll('hello, world! foo123');
  eq(matches.length, 3);
  eq(matches[0].match, 'hello');
  eq(matches[1].match, 'world');
  eq(matches[2].match, 'foo123');
});

test('search with anchored pattern', () => {
  const r = new Regex('^hello');
  assert(r.search('hello world') !== null);
  eq(r.search('say hello'), null);
});

test('replace with pattern', () => {
  const r = new Regex('[aeiou]');
  eq(r.replace('hello', '*', true), 'h*ll*');
});

test('consecutive classes', () => {
  const r = new Regex('[A-Z][a-z]+');
  assert(r.test('Hello'));
  assert(!r.test('hello'));
  assert(!r.test('HELLO'));
});

test('mixed class and literal', () => {
  const r = new Regex('test[0-9]+\\.js');
  assert(r.test('test1.js'));
  assert(r.test('test123.js'));
  assert(!r.test('test.js'));
  assert(!r.test('testx.js'));
});

test('negated class \\S+', () => {
  const r = new Regex('\\S+');
  const matches = r.findAll('hello world  test');
  eq(matches.length, 3);
});

test('counted zero min {0,3}', () => {
  const r = new Regex('a{0,3}b');
  assert(r.test('b'));
  assert(r.test('ab'));
  assert(r.test('aab'));
  assert(r.test('aaab'));
  assert(!r.test('aaaab'));
});

// ═══════════════════════════════════════════

console.log(`\n══════════════════════════════`);
console.log(`  ${passed}/${total} passed, ${failed} failed`);
console.log(`══════════════════════════════`);
process.exit(failed > 0 ? 1 : 0);
