/**
 * Type-Level Strings
 * 
 * String literal types: "hello" is both a value AND a type.
 * Operations at the type level: concatenation, template literals.
 * Used in TypeScript, Flow, and dependent type languages.
 */

class TLitStr { constructor(value) { this.tag = 'TLitStr'; this.value = value; } toString() { return `"${this.value}"`; } }
class TStr { constructor() { this.tag = 'TStr'; } toString() { return 'string'; } }
class TConcat { constructor(left, right) { this.tag = 'TConcat'; this.left = left; this.right = right; } }
class TTemplate { constructor(parts) { this.tag = 'TTemplate'; this.parts = parts; } }

const tStr = new TStr();

function reduce(type) {
  switch (type.tag) {
    case 'TConcat': {
      const l = reduce(type.left);
      const r = reduce(type.right);
      if (l.tag === 'TLitStr' && r.tag === 'TLitStr') return new TLitStr(l.value + r.value);
      return new TConcat(l, r);
    }
    case 'TTemplate': {
      const parts = type.parts.map(reduce);
      if (parts.every(p => p.tag === 'TLitStr')) return new TLitStr(parts.map(p => p.value).join(''));
      return new TTemplate(parts);
    }
    default: return type;
  }
}

function isSubtype(t1, t2) {
  t1 = reduce(t1); t2 = reduce(t2);
  if (t2.tag === 'TStr') return t1.tag === 'TLitStr' || t1.tag === 'TStr';
  if (t1.tag === 'TLitStr' && t2.tag === 'TLitStr') return t1.value === t2.value;
  return false;
}

function startsWith(type, prefix) {
  type = reduce(type);
  if (type.tag !== 'TLitStr') return null;
  return type.value.startsWith(prefix);
}

function inferTemplate(strings, ...exprs) {
  const parts = [];
  for (let i = 0; i < strings.length; i++) {
    if (strings[i]) parts.push(new TLitStr(strings[i]));
    if (i < exprs.length) parts.push(exprs[i]);
  }
  return reduce(new TTemplate(parts));
}

function split(type, sep) {
  type = reduce(type);
  if (type.tag !== 'TLitStr') return null;
  return type.value.split(sep).map(s => new TLitStr(s));
}

function length(type) {
  type = reduce(type);
  if (type.tag !== 'TLitStr') return null;
  return type.value.length;
}

export { TLitStr, TStr, TConcat, TTemplate, tStr, reduce, isSubtype, startsWith, inferTemplate, split, length };
