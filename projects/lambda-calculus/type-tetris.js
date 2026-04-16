/**
 * Type Tetris: Fun puzzle - what functions have this type?
 * 
 * Given a type, enumerate all possible inhabitants (terms of that type).
 * A ↦ B × C implies functions that return pairs.
 * A → A is inhabited by id.
 * (A → B → C) → (B → A → C) is inhabited by flip.
 */

const EXAMPLES = [
  { type: 'A → A', names: ['id'], impl: x => x },
  { type: 'A → B → A', names: ['const', 'K'], impl: x => y => x },
  { type: 'A → B → B', names: ['const\''], impl: x => y => y },
  { type: '(A → B) → A → B', names: ['apply', '$'], impl: f => x => f(x) },
  { type: '(A → B → C) → B → A → C', names: ['flip', 'C'], impl: f => b => a => f(a)(b) },
  { type: '(B → C) → (A → B) → A → C', names: ['compose', '.', 'B'], impl: f => g => x => f(g(x)) },
  { type: '(A → A → B) → A → B', names: ['dup', 'W'], impl: f => x => f(x)(x) },
  { type: '((A → B) → A) → A', names: ['peirce'], impl: null }, // Not inhabited in constructive logic!
  { type: 'A → Void', names: [], impl: null }, // Impossible (can't produce Void)
];

function countTypeVars(typeStr) {
  const vars = new Set(typeStr.match(/[A-Z]/g) || []);
  return vars.size;
}

function countArrows(typeStr) {
  return (typeStr.match(/→/g) || []).length;
}

function isContradiction(typeStr) {
  return typeStr.includes('Void') && !typeStr.startsWith('Void');
}

function verifyInhabitant(example, testInputs) {
  if (!example.impl) return { inhabited: false, type: example.type };
  try {
    // Test that the function works for any input
    let result = example.impl;
    for (const input of testInputs) {
      if (typeof result === 'function') result = result(input);
    }
    return { inhabited: true, type: example.type, result };
  } catch {
    return { inhabited: false, type: example.type, error: 'crashed' };
  }
}

function typeComplexity(typeStr) {
  return {
    vars: countTypeVars(typeStr),
    arrows: countArrows(typeStr),
    parens: (typeStr.match(/\(/g) || []).length,
    total: typeStr.length
  };
}

export { EXAMPLES, countTypeVars, countArrows, isContradiction, verifyInhabitant, typeComplexity };
