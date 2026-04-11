// optimizer.js — AST and bytecode optimization passes for the compiler
import { OP } from './vm.js';

/**
 * AST-level constant folding.
 * Evaluates compile-time-computable expressions.
 * e.g., 3 + 4 → 7, true → 1, 2 * 3 + 1 → 7
 */
export function constantFold(ast) {
  return foldNode(ast);
}

function foldNode(node) {
  if (!node || typeof node !== 'object') return node;
  
  switch (node.type) {
    case 'Program':
      return { ...node, body: node.body.map(foldNode) };
    case 'Block':
      return { ...node, body: node.body.map(foldNode) };
    case 'Let':
      return { ...node, value: foldNode(node.value) };
    case 'Assign':
      return { ...node, value: foldNode(node.value) };
    case 'If':
      return foldIf(node);
    case 'While':
      return { ...node, condition: foldNode(node.condition), body: foldNode(node.body) };
    case 'Function':
      return { ...node, body: foldNode(node.body) };
    case 'Return':
      return { ...node, value: foldNode(node.value) };
    case 'Print':
      return { ...node, value: foldNode(node.value) };
    case 'ExprStatement':
      return { ...node, expr: foldNode(node.expr) };
    case 'Call':
      return { ...node, args: node.args.map(foldNode) };
      
    case 'BinaryOp': {
      const left = foldNode(node.left);
      const right = foldNode(node.right);
      
      // Both constants → evaluate at compile time
      if (left.type === 'Number' && right.type === 'Number') {
        const result = evalBinaryOp(node.op, left.value, right.value);
        if (result !== null) return { type: 'Number', value: result };
      }
      
      // Algebraic simplifications
      // x + 0 → x, x - 0 → x, x * 1 → x, x * 0 → 0
      if (right.type === 'Number') {
        if (node.op === '+' && right.value === 0) return left;
        if (node.op === '-' && right.value === 0) return left;
        if (node.op === '*' && right.value === 1) return left;
        if (node.op === '*' && right.value === 0) return { type: 'Number', value: 0 };
      }
      if (left.type === 'Number') {
        if (node.op === '+' && left.value === 0) return right;
        if (node.op === '*' && left.value === 1) return right;
        if (node.op === '*' && left.value === 0) return { type: 'Number', value: 0 };
      }
      
      return { ...node, left, right };
    }
      
    case 'UnaryOp': {
      const operand = foldNode(node.operand);
      if (operand.type === 'Number' && node.op === '-') {
        return { type: 'Number', value: -operand.value };
      }
      return { ...node, operand };
    }
      
    default:
      return node;
  }
}

function foldIf(node) {
  const condition = foldNode(node.condition);
  const then = foldNode(node.then);
  const elseBody = node.else ? foldNode(node.else) : null;
  
  // Dead code elimination: if (true) → then, if (false) → else
  if (condition.type === 'Number') {
    if (condition.value !== 0) return then; // truthy → then branch only
    if (elseBody) return elseBody;          // falsy → else branch only
    return { type: 'Block', body: [] };     // falsy, no else → empty
  }
  
  return { ...node, condition, then, else: elseBody };
}

function evalBinaryOp(op, a, b) {
  switch (op) {
    case '+': return a + b;
    case '-': return a - b;
    case '*': return a * b;
    case '/': return b !== 0 ? Math.trunc(a / b) : null;
    case '%': return b !== 0 ? a % b : null;
    case '==': return a === b ? 1 : 0;
    case '!=': return a !== b ? 1 : 0;
    case '<': return a < b ? 1 : 0;
    case '>': return a > b ? 1 : 0;
    case '<=': return a <= b ? 1 : 0;
    case '>=': return a >= b ? 1 : 0;
    default: return null;
  }
}

/**
 * Bytecode-level peephole optimization.
 * Removes redundant patterns like PUSH-POP, double NEG, etc.
 */
export function peepholeOptimize(bytecode) {
  let code = [...bytecode];
  let changed = true;
  
  while (changed) {
    changed = false;
    const newCode = [];
    let i = 0;
    
    while (i < code.length) {
      // PUSH x, POP → remove both (dead value)
      if (code[i] === OP.PUSH && i + 2 < code.length && code[i + 2] === OP.POP) {
        i += 3; // skip PUSH, value, POP
        changed = true;
        continue;
      }
      
      // NEG, NEG → remove both (double negation)
      if (code[i] === OP.NEG && i + 1 < code.length && code[i + 1] === OP.NEG) {
        i += 2;
        changed = true;
        continue;
      }
      
      // NOT, NOT → remove both (double negation)
      if (code[i] === OP.NOT && i + 1 < code.length && code[i + 1] === OP.NOT) {
        i += 2;
        changed = true;
        continue;
      }
      
      // PUSH 0, ADD → remove (adding zero)
      if (code[i] === OP.PUSH && code[i + 1] === 0 && i + 2 < code.length && code[i + 2] === OP.ADD) {
        i += 3;
        changed = true;
        continue;
      }
      
      // PUSH 1, MUL → remove (multiply by one)
      if (code[i] === OP.PUSH && code[i + 1] === 1 && i + 2 < code.length && code[i + 2] === OP.MUL) {
        i += 3;
        changed = true;
        continue;
      }
      
      newCode.push(code[i]);
      i++;
    }
    
    // Need to fixup jump addresses after removing instructions
    // For now, only apply peephole when no jumps are involved
    if (changed && hasJumps(code)) {
      // Don't apply peephole when jumps exist (address fixup is complex)
      return code;
    }
    
    code = newCode;
  }
  
  return code;
}

function hasJumps(code) {
  for (let i = 0; i < code.length; i++) {
    if ([OP.JMP, OP.JZ, OP.JNZ, OP.CALL].includes(code[i])) return true;
  }
  return false;
}

/**
 * Count AST nodes (for optimization stats).
 */
export function countNodes(ast) {
  if (!ast || typeof ast !== 'object') return 0;
  let count = 1;
  for (const key of Object.keys(ast)) {
    if (Array.isArray(ast[key])) {
      for (const item of ast[key]) count += countNodes(item);
    } else if (typeof ast[key] === 'object' && ast[key] !== null) {
      count += countNodes(ast[key]);
    }
  }
  return count;
}
