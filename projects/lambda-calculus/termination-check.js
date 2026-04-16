/**
 * Termination Checking: Structural recursion analysis
 * 
 * Ensures recursive calls are on structurally smaller arguments.
 * Like Agda/Coq's termination checker.
 */

// Size relation: is arg structurally smaller than param?
function structurallySmaller(arg, param, destructors) {
  if (destructors.has(arg)) {
    const d = destructors.get(arg);
    return d.source === param;
  }
  return false;
}

class CallGraph {
  constructor() { this.edges = []; }
  
  addCall(caller, callee, args) {
    this.edges.push({ caller, callee, args });
  }
  
  // Check if all recursive calls decrease
  checkTermination(fnName, params) {
    const recursiveCalls = this.edges.filter(e => e.callee === fnName);
    
    for (const call of recursiveCalls) {
      let decreased = false;
      for (let i = 0; i < call.args.length; i++) {
        const arg = call.args[i];
        if (arg.smaller && arg.than === params[i]) {
          decreased = true;
          break;
        }
      }
      if (!decreased) return { terminates: false, reason: `Call from ${call.caller}: no argument decreases`, call };
    }
    return { terminates: true };
  }
}

// Analyze a function definition for termination
function analyzeRecursion(fnDef) {
  const { name, params, body } = fnDef;
  const calls = [];
  
  function scan(expr, destructed = new Map()) {
    if (!expr || typeof expr !== 'object') return;
    
    if (expr.tag === 'Call' && expr.fn === name) {
      calls.push({
        args: expr.args.map((a, i) => ({
          name: a.name || a.value,
          smaller: destructed.has(a.name),
          than: destructed.get(a.name) || null
        }))
      });
    }
    
    if (expr.tag === 'Match') {
      for (const branch of (expr.branches || [])) {
        const newDestructed = new Map(destructed);
        if (branch.bindings) {
          for (const b of branch.bindings) {
            newDestructed.set(b.name, expr.scrutinee);
          }
        }
        scan(branch.body, newDestructed);
      }
      return;
    }
    
    for (const key of Object.keys(expr)) {
      if (typeof expr[key] === 'object') scan(expr[key], destructed);
    }
  }
  
  scan(body);
  
  const cg = new CallGraph();
  for (const c of calls) cg.addCall(name, name, c.args);
  
  return cg.checkTermination(name, params);
}

// Well-founded ordering check
function isWellFounded(relation, domain) {
  // Check: no infinite descending chains
  // Simple: verify relation is irreflexive and finite
  for (const [a, b] of relation) {
    if (a === b) return false; // Reflexive → not well-founded
  }
  // Check for cycles
  const adj = new Map();
  for (const [a, b] of relation) {
    if (!adj.has(a)) adj.set(a, []);
    adj.get(a).push(b);
  }
  const visited = new Set();
  const stack = new Set();
  function hasCycle(node) {
    if (stack.has(node)) return true;
    if (visited.has(node)) return false;
    visited.add(node); stack.add(node);
    for (const next of (adj.get(node) || [])) { if (hasCycle(next)) return true; }
    stack.delete(node);
    return false;
  }
  for (const [a] of relation) { if (hasCycle(a)) return false; }
  return true;
}

export { CallGraph, analyzeRecursion, isWellFounded };
