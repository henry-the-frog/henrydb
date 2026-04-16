/**
 * Dependent Pattern Matching: Pattern match with type refinement
 * 
 * In dependently-typed languages, pattern matching refines the type
 * of the scrutinee AND related variables in each branch.
 */

class DVar { constructor(name) { this.tag = 'DVar'; this.name = name; } }
class DCon { constructor(name, args) { this.tag = 'DCon'; this.name = name; this.args = args; } }
class DWild { constructor() { this.tag = 'DWild'; } }

// Match result with type information
class MatchResult {
  constructor(bindings, refinements) {
    this.bindings = bindings; // Map<name, value>
    this.refinements = refinements; // Map<typeVar, concreteType>
  }
}

function matchPattern(pattern, value, typeInfo = {}) {
  switch (pattern.tag) {
    case 'DVar': return new MatchResult(new Map([[pattern.name, value]]), new Map());
    case 'DWild': return new MatchResult(new Map(), new Map());
    case 'DCon': {
      if (value.tag !== 'DCon' || value.name !== pattern.name) return null;
      if (pattern.args.length !== value.args.length) return null;
      const bindings = new Map();
      const refinements = new Map();
      // Refine type based on constructor
      if (typeInfo[pattern.name]) {
        for (const [k, v] of Object.entries(typeInfo[pattern.name])) {
          refinements.set(k, v);
        }
      }
      for (let i = 0; i < pattern.args.length; i++) {
        const sub = matchPattern(pattern.args[i], value.args[i], typeInfo);
        if (!sub) return null;
        for (const [k, v] of sub.bindings) bindings.set(k, v);
        for (const [k, v] of sub.refinements) refinements.set(k, v);
      }
      return new MatchResult(bindings, refinements);
    }
  }
}

// Exhaustiveness check
function isExhaustive(patterns, constructors) {
  if (patterns.some(p => p.tag === 'DWild' || p.tag === 'DVar')) return true;
  const covered = new Set(patterns.filter(p => p.tag === 'DCon').map(p => p.name));
  return constructors.every(c => covered.has(c));
}

// Redundancy check
function findRedundant(patterns) {
  const seen = new Set();
  const redundant = [];
  for (let i = 0; i < patterns.length; i++) {
    const key = patternKey(patterns[i]);
    if (key !== '_' && seen.has(key)) redundant.push(i);
    seen.add(key);
  }
  return redundant;
}

function patternKey(p) {
  if (p.tag === 'DVar' || p.tag === 'DWild') return '_';
  return `${p.name}(${p.args.map(patternKey).join(',')})`;
}

export { DVar, DCon, DWild, MatchResult, matchPattern, isExhaustive, findRedundant };
