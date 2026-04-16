/**
 * Occurrence Typing (TypeScript-style type narrowing)
 * 
 * When a type test appears in a condition, narrow the type in branches:
 *   if (typeof x === "number") { x: number } else { x: string }
 * 
 * Features:
 * - typeof checks narrow union types
 * - Truthiness narrows nullable types
 * - Logical operators (&&, ||) combine narrowings
 * - Negation reverses narrowing
 * 
 * Based on: Tobin-Hochstadt & Felleisen (2010) "Logical Types for Untyped Languages"
 */

// Types
class TInt { constructor() { this.tag = 'TInt'; } toString() { return 'int'; } }
class TStr { constructor() { this.tag = 'TStr'; } toString() { return 'str'; } }
class TBool { constructor() { this.tag = 'TBool'; } toString() { return 'bool'; } }
class TNull { constructor() { this.tag = 'TNull'; } toString() { return 'null'; } }
class TUnion {
  constructor(types) { this.tag = 'TUnion'; this.types = types; }
  toString() { return this.types.join(' | '); }
}

const tInt = new TInt();
const tStr = new TStr();
const tBool = new TBool();
const tNull = new TNull();

function typeEquals(a, b) {
  if (a.tag !== b.tag) return false;
  if (a.tag === 'TUnion') {
    return a.types.length === b.types.length && 
      a.types.every((t, i) => typeEquals(t, b.types[i]));
  }
  return true;
}

function union(...types) {
  const flat = types.flatMap(t => t.tag === 'TUnion' ? t.types : [t]);
  const unique = [];
  for (const t of flat) {
    if (!unique.some(u => typeEquals(u, t))) unique.push(t);
  }
  if (unique.length === 1) return unique[0];
  return new TUnion(unique);
}

// ============================================================
// Type Tests
// ============================================================

class TypeofTest {
  constructor(varName, typeName) { this.tag = 'typeof'; this.varName = varName; this.typeName = typeName; }
}

class TruthyTest {
  constructor(varName) { this.tag = 'truthy'; this.varName = varName; }
}

class NotTest {
  constructor(test) { this.tag = 'not'; this.test = test; }
}

class AndTest {
  constructor(left, right) { this.tag = 'and'; this.left = left; this.right = right; }
}

class OrTest {
  constructor(left, right) { this.tag = 'or'; this.left = left; this.right = right; }
}

// ============================================================
// Narrowing
// ============================================================

function narrow(type, test) {
  switch (test.tag) {
    case 'typeof': {
      const testType = typeNameToType(test.typeName);
      if (!testType) return type;
      
      if (type.tag === 'TUnion') {
        // Filter union to only matching types
        const matching = type.types.filter(t => typeEquals(t, testType));
        if (matching.length === 0) return type; // No match → keep original (could be never)
        return matching.length === 1 ? matching[0] : new TUnion(matching);
      }
      
      // Non-union: if matches, keep; otherwise could be never
      return typeEquals(type, testType) ? type : type;
    }
    
    case 'truthy': {
      // Truthy narrows out null and false
      if (type.tag === 'TUnion') {
        const nonNull = type.types.filter(t => t.tag !== 'TNull');
        if (nonNull.length === 0) return tNull; // Shouldn't happen
        return nonNull.length === 1 ? nonNull[0] : new TUnion(nonNull);
      }
      return type;
    }
    
    case 'not':
      return narrowNegate(type, test.test);
    
    case 'and': {
      const left = narrow(type, test.left);
      return narrow(left, test.right);
    }
    
    case 'or': {
      const left = narrow(type, test.left);
      const right = narrow(type, test.right);
      return union(left, right);
    }
    
    default:
      return type;
  }
}

function narrowNegate(type, test) {
  switch (test.tag) {
    case 'typeof': {
      const testType = typeNameToType(test.typeName);
      if (!testType) return type;
      
      if (type.tag === 'TUnion') {
        // Filter union to EXCLUDE matching types
        const nonMatching = type.types.filter(t => !typeEquals(t, testType));
        if (nonMatching.length === 0) return type;
        return nonMatching.length === 1 ? nonMatching[0] : new TUnion(nonMatching);
      }
      return type;
    }
    
    case 'truthy': {
      // Not truthy = null
      if (type.tag === 'TUnion') {
        const nullTypes = type.types.filter(t => t.tag === 'TNull');
        if (nullTypes.length > 0) return tNull;
      }
      return tNull;
    }
    
    case 'not':
      return narrow(type, test.test); // Double negation
    
    default:
      return type;
  }
}

function typeNameToType(name) {
  switch (name) {
    case 'number': case 'int': return tInt;
    case 'string': case 'str': return tStr;
    case 'boolean': case 'bool': return tBool;
    case 'null': return tNull;
    default: return null;
  }
}

// ============================================================
// Environment narrowing
// ============================================================

function narrowEnv(env, test) {
  const result = new Map(env);
  const varName = getTestVar(test);
  if (varName && env.has(varName)) {
    result.set(varName, narrow(env.get(varName), test));
  }
  return result;
}

function narrowEnvNegate(env, test) {
  const result = new Map(env);
  const varName = getTestVar(test);
  if (varName && env.has(varName)) {
    result.set(varName, narrowNegate(env.get(varName), test));
  }
  return result;
}

function getTestVar(test) {
  switch (test.tag) {
    case 'typeof': return test.varName;
    case 'truthy': return test.varName;
    case 'not': return getTestVar(test.test);
    case 'and': return getTestVar(test.left) || getTestVar(test.right);
    case 'or': return getTestVar(test.left) || getTestVar(test.right);
    default: return null;
  }
}

export {
  TInt, TStr, TBool, TNull, TUnion,
  tInt, tStr, tBool, tNull,
  typeEquals, union,
  TypeofTest, TruthyTest, NotTest, AndTest, OrTest,
  narrow, narrowNegate, narrowEnv, narrowEnvNegate,
};
