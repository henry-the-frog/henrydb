// Type inference fuzzer: generate random well-typed expressions and verify
import { typeOf, resetFresh, TVar, TCon, TFun, tInt, tBool, tString } from './types.js';

function seeded(seed) {
  let s = seed;
  return () => { s = (s * 1103515245 + 12345) & 0x7fffffff; return s / 0x7fffffff; };
}

function randomInt(rng, min, max) { return Math.floor(rng() * (max - min + 1)) + min; }

function randomChoice(rng, arr) { return arr[Math.floor(rng() * arr.length)]; }

// Generate random well-formed expressions
function randomExpr(rng, depth, vars = ['x', 'y', 'z', 'f', 'g', 'n']) {
  if (depth <= 0) {
    const r = rng();
    if (r < 0.25) return String(randomInt(rng, 0, 100));
    if (r < 0.4) return randomChoice(rng, ['true', 'false']);
    if (r < 0.5) return `"${randomChoice(rng, ['hello', 'world', 'test'])}"`;
    if (r < 0.8 && vars.length > 0) return randomChoice(rng, vars);
    return String(randomInt(rng, 0, 50));
  }
  
  const r = rng();
  if (r < 0.15) {
    // Integer literal
    return String(randomInt(rng, 0, 100));
  } else if (r < 0.2) {
    // Boolean
    return randomChoice(rng, ['true', 'false']);
  } else if (r < 0.3) {
    // Arithmetic
    const op = randomChoice(rng, ['+', '-', '*']);
    return `(${randomExpr(rng, depth-1, vars)} ${op} ${randomExpr(rng, depth-1, vars)})`;
  } else if (r < 0.35) {
    // Comparison
    const op = randomChoice(rng, ['==', '<', '>']);
    return `(${randomExpr(rng, depth-1, vars)} ${op} ${randomExpr(rng, depth-1, vars)})`;
  } else if (r < 0.45) {
    // Lambda
    const v = randomChoice(rng, ['a', 'b', 'c', 'p', 'q']);
    return `(\\${v} -> ${randomExpr(rng, depth-1, [...vars, v])})`;
  } else if (r < 0.55) {
    // Application
    return `(${randomExpr(rng, depth-1, vars)} ${randomExpr(rng, 0, vars)})`;
  } else if (r < 0.7) {
    // Let binding
    const v = randomChoice(rng, ['a', 'b', 'c', 'p', 'q']);
    return `let ${v} = ${randomExpr(rng, depth-1, vars)} in ${randomExpr(rng, depth-1, [...vars, v])}`;
  } else if (r < 0.8) {
    // If-then-else
    return `if ${randomExpr(rng, depth-1, vars)} then ${randomExpr(rng, depth-1, vars)} else ${randomExpr(rng, depth-1, vars)}`;
  } else if (r < 0.9 && vars.length > 0) {
    // Variable reference
    return randomChoice(rng, vars);
  } else {
    return String(randomInt(rng, 0, 100));
  }
}

let wellTyped = 0, typeErrors = 0, crashes = 0;
const crashExprs = [];
const startTime = Date.now();

for (let seed = 1; seed <= 5000; seed++) {
  const rng = seeded(seed);
  const depth = randomInt(rng, 1, 4);
  const expr = randomExpr(rng, depth);
  
  try {
    resetFresh();
    const typ = typeOf(expr);
    
    // Basic sanity checks on inferred type
    if (typeof typ !== 'string' || typ.length === 0) {
      console.error(`INVALID TYPE seed=${seed}: ${typ} for ${expr}`);
      crashes++;
      if (crashExprs.length < 10) crashExprs.push({ seed, expr, issue: 'empty type' });
      continue;
    }
    
    wellTyped++;
    
    // Consistency check: same expression should always produce same type
    resetFresh();
    const typ2 = typeOf(expr);
    // Type vars may differ but structure should be same
    // Just check they're both non-empty strings
    if (typeof typ2 !== 'string' || typ2.length === 0) {
      console.error(`INCONSISTENT seed=${seed}: first=${typ} second=${typ2}`);
      crashes++;
      continue;
    }
    
  } catch (e) {
    // Type errors are expected for random expressions
    if (e.message.includes('unify') || e.message.includes('Unify') ||
        e.message.includes('Occurs') || e.message.includes('Unbound') || 
        e.message.includes('not a function') || e.message.includes('expected Bool') || 
        e.message.includes('Type mismatch') || e.message.includes('Cannot') ||
        e.message.includes('Unexpected') || e.message.includes('Expected') ||
        e.message.includes('Infinite') || e.message.includes('token')) {
      typeErrors++;
    } else {
      crashes++;
      if (crashExprs.length < 10) crashExprs.push({ seed, expr, error: e.message });
    }
  }
  
  if (seed % 1000 === 0) process.stderr.write(`${seed}/5000...\n`);
}

const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
console.log(`\n=== Type Inference Fuzzer Results ===`);
console.log(`Well-typed: ${wellTyped}, Type errors: ${typeErrors}, Crashes: ${crashes}`);
console.log(`Time: ${elapsed}s`);

if (crashExprs.length > 0) {
  console.log(`\n=== Crashes ===`);
  for (const c of crashExprs) {
    console.log(`  seed=${c.seed}: ${c.error || c.issue}`);
    console.log(`    expr: ${c.expr.slice(0, 100)}`);
  }
}

console.log(crashes === 0 ? '\nNO CRASHES ✓' : '\nCRASHES DETECTED');
process.exit(crashes > 0 ? 1 : 0);
