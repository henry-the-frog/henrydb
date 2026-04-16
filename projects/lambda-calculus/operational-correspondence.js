/**
 * Module #145: Operational Correspondence
 * 
 * Prove that source and target languages agree on behavior.
 * If source reduces to v, then compiled target reduces to v' where v ~ v'.
 * The foundation of compiler correctness.
 */

class SNum { constructor(n) { this.tag = 'SNum'; this.n = n; } }
class SAdd { constructor(l, r) { this.tag = 'SAdd'; this.left = l; this.right = r; } }
class SMul { constructor(l, r) { this.tag = 'SMul'; this.left = l; this.right = r; } }

// Target: stack machine instructions
class Push { constructor(n) { this.tag = 'Push'; this.n = n; } }
class Add { constructor() { this.tag = 'Add'; } }
class Mul { constructor() { this.tag = 'Mul'; } }

// Source eval
function evalSource(expr) {
  switch (expr.tag) {
    case 'SNum': return expr.n;
    case 'SAdd': return evalSource(expr.left) + evalSource(expr.right);
    case 'SMul': return evalSource(expr.left) * evalSource(expr.right);
  }
}

// Compile: source → stack machine
function compile(expr) {
  switch (expr.tag) {
    case 'SNum': return [new Push(expr.n)];
    case 'SAdd': return [...compile(expr.left), ...compile(expr.right), new Add()];
    case 'SMul': return [...compile(expr.left), ...compile(expr.right), new Mul()];
  }
}

// Target eval (stack machine)
function evalTarget(instrs) {
  const stack = [];
  for (const instr of instrs) {
    switch (instr.tag) {
      case 'Push': stack.push(instr.n); break;
      case 'Add': { const b = stack.pop(), a = stack.pop(); stack.push(a + b); break; }
      case 'Mul': { const b = stack.pop(), a = stack.pop(); stack.push(a * b); break; }
    }
  }
  return stack[0];
}

// Verify correspondence: evalSource(e) === evalTarget(compile(e))
function verifyCorrespondence(expr) {
  const sourceResult = evalSource(expr);
  const targetResult = evalTarget(compile(expr));
  return {
    source: sourceResult,
    target: targetResult,
    correct: sourceResult === targetResult
  };
}

// Generate random expressions for property testing
function randomExpr(depth = 3) {
  if (depth <= 0) return new SNum(Math.floor(Math.random() * 100));
  const ops = [SAdd, SMul];
  const Op = ops[Math.floor(Math.random() * ops.length)];
  return new Op(randomExpr(depth - 1), randomExpr(depth - 1));
}

// Bisimulation: two systems step in lockstep
function bisimulation(steps1, steps2) {
  if (steps1.length !== steps2.length) return { bisimilar: false, reason: 'different step count' };
  return { bisimilar: steps1.every((s, i) => s === steps2[i]), steps: steps1.length };
}

export { SNum, SAdd, SMul, Push, Add, Mul, evalSource, compile, evalTarget, verifyCorrespondence, randomExpr, bisimulation };
