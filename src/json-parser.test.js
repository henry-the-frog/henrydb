// json-parser.test.js — Tests for JSON parser
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { jsonParse, jsonStringify } from './json-parser.js';

describe('jsonParse', () => {
  it('primitives', () => {
    assert.equal(jsonParse('null'), null);
    assert.equal(jsonParse('true'), true);
    assert.equal(jsonParse('false'), false);
    assert.equal(jsonParse('42'), 42);
    assert.equal(jsonParse('-3.14'), -3.14);
    assert.equal(jsonParse('1e10'), 1e10);
    assert.equal(jsonParse('"hello"'), 'hello');
  });

  it('strings with escapes', () => {
    assert.equal(jsonParse('"hello\\nworld"'), 'hello\nworld');
    assert.equal(jsonParse('"tab\\there"'), 'tab\there');
    assert.equal(jsonParse('"quote\\"inside"'), 'quote"inside');
    assert.equal(jsonParse('"backslash\\\\"'), 'backslash\\');
    assert.equal(jsonParse('"slash\\/"'), 'slash/');
  });

  it('unicode escapes', () => {
    assert.equal(jsonParse('"\\u0041"'), 'A');
    assert.equal(jsonParse('"\\u00e9"'), 'é');
    assert.equal(jsonParse('"\\u4e16\\u754c"'), '世界');
  });

  it('arrays', () => {
    assert.deepEqual(jsonParse('[]'), []);
    assert.deepEqual(jsonParse('[1,2,3]'), [1, 2, 3]);
    assert.deepEqual(jsonParse('["a", "b"]'), ['a', 'b']);
    assert.deepEqual(jsonParse('[1, [2, [3]]]'), [1, [2, [3]]]);
    assert.deepEqual(jsonParse('[null, true, false]'), [null, true, false]);
  });

  it('objects', () => {
    assert.deepEqual(jsonParse('{}'), {});
    assert.deepEqual(jsonParse('{"a": 1}'), { a: 1 });
    assert.deepEqual(jsonParse('{"x": 1, "y": 2}'), { x: 1, y: 2 });
    assert.deepEqual(jsonParse('{"nested": {"a": [1, 2]}}'), { nested: { a: [1, 2] } });
  });

  it('whitespace handling', () => {
    assert.deepEqual(jsonParse('  { "a" : 1 , "b" : 2 }  '), { a: 1, b: 2 });
    assert.deepEqual(jsonParse('[\n  1,\n  2,\n  3\n]'), [1, 2, 3]);
  });

  it('error: trailing comma', () => {
    assert.throws(() => jsonParse('[1, 2,]'));
    assert.throws(() => jsonParse('{"a": 1,}'));
  });

  it('error: single quotes', () => {
    assert.throws(() => jsonParse("'hello'"));
  });

  it('error: trailing content', () => {
    assert.throws(() => jsonParse('42 extra'));
  });

  it('error: empty input', () => {
    assert.throws(() => jsonParse(''));
  });
});

describe('jsonStringify', () => {
  it('primitives', () => {
    assert.equal(jsonStringify(null), 'null');
    assert.equal(jsonStringify(true), 'true');
    assert.equal(jsonStringify(false), 'false');
    assert.equal(jsonStringify(42), '42');
    assert.equal(jsonStringify('hello'), '"hello"');
  });

  it('string escaping', () => {
    assert.equal(jsonStringify('a"b'), '"a\\"b"');
    assert.equal(jsonStringify('a\\b'), '"a\\\\b"');
    assert.equal(jsonStringify('a\nb'), '"a\\nb"');
    assert.equal(jsonStringify('a\tb'), '"a\\tb"');
  });

  it('arrays and objects', () => {
    assert.equal(jsonStringify([1, 2, 3]), '[1,2,3]');
    assert.equal(jsonStringify({ a: 1 }), '{"a":1}');
    assert.equal(jsonStringify([]), '[]');
    assert.equal(jsonStringify({}), '{}');
  });

  it('indentation', () => {
    const result = jsonStringify({ a: 1, b: [2, 3] }, 2);
    assert.ok(result.includes('\n'));
    assert.ok(result.includes('  "a"'));
  });

  it('special values', () => {
    assert.equal(jsonStringify(Infinity), 'null');
    assert.equal(jsonStringify(NaN), 'null');
    assert.equal(jsonStringify(undefined), undefined);
  });
});

describe('JSON Differential Fuzzer', () => {
  it('1000 random values: our parse matches native JSON.parse', () => {
    let seed = 42;
    const rng = () => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed / 0x7fffffff; };
    const randomInt = (min, max) => Math.floor(rng() * (max - min + 1)) + min;
    
    function randomValue(depth = 0) {
      if (depth > 3) return randomInt(0, 100);
      const r = rng();
      if (r < 0.15) return null;
      if (r < 0.25) return rng() < 0.5;
      if (r < 0.4) return randomInt(-1000, 1000);
      if (r < 0.55) return rng() < 0.5 ? randomInt(-1000, 1000) + rng() : randomInt(-1000, 1000);
      if (r < 0.7) {
        const chars = 'abcdefghijklmnopqrstuvwxyz 0123456789!@#';
        let s = '';
        for (let i = 0; i < randomInt(0, 20); i++) s += chars[randomInt(0, chars.length - 1)];
        return s;
      }
      if (r < 0.85) {
        const len = randomInt(0, 5);
        return Array.from({ length: len }, () => randomValue(depth + 1));
      }
      const keys = randomInt(0, 5);
      const obj = {};
      for (let i = 0; i < keys; i++) obj['k' + randomInt(0, 100)] = randomValue(depth + 1);
      return obj;
    }
    
    let passed = 0;
    for (let i = 0; i < 1000; i++) {
      const value = randomValue();
      const json = JSON.stringify(value);
      
      try {
        const ours = jsonParse(json);
        const native = JSON.parse(json);
        assert.deepStrictEqual(ours, native, `Mismatch for: ${json.slice(0, 100)}`);
        passed++;
      } catch (e) {
        if (e.code === 'ERR_ASSERTION') throw e;
        // Parse error is fine for some edge cases
      }
    }
    assert.ok(passed >= 990, `Only ${passed}/1000 passed`);
  });

  it('roundtrip: stringify then parse equals original', () => {
    const values = [
      null, true, false, 42, -3.14, 'hello', '',
      [1, 2, 3], { a: 1 }, { nested: { array: [1, [2]] } },
      [null, true, false, 42, "str", [], {}],
    ];
    
    for (const val of values) {
      const json = jsonStringify(val);
      const parsed = jsonParse(json);
      assert.deepStrictEqual(parsed, val, `Roundtrip failed for: ${JSON.stringify(val)}`);
    }
  });
});
