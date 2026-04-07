// pipeline-compiler.js — Push-based pipeline compilation for HenryDB
// Compiles query plan segments into tight JS functions,
// eliminating virtual dispatch overhead of the Volcano iterator model.

import { Iterator, SeqScan, Filter, Project, Limit, Sort, HashJoin,
         HashAggregate, IndexScan, NestedLoopJoin, Window, Distinct } from './volcano.js';

// Pipeline breakers — operators that must materialize their input
const BREAKERS = new Set(['Sort', 'HashAggregate', 'Window', 'HashJoin', 'Distinct']);

/**
 * Identify pipeline segments in a query plan.
 * Returns array of { root, operators, child (either null or a breaker/pipeline) }
 */
export function identifyPipelines(root) {
  const pipelines = [];
  
  function walk(node) {
    if (!node) return null;
    
    const name = node.constructor.name;
    
    if (BREAKERS.has(name)) {
      // This node is a pipeline breaker.
      // Its children form separate pipelines.
      const childPipelines = [];
      
      if (node._child) childPipelines.push(walk(node._child));
      if (node._left) childPipelines.push(walk(node._left));
      if (node._right) childPipelines.push(walk(node._right));
      if (node._input) childPipelines.push(walk(node._input));
      
      // The breaker itself starts a new pipeline above it
      return { type: 'breaker', node, children: childPipelines.filter(Boolean) };
    }
    
    // This node is pipeline-compatible.
    // Walk children to find where the pipeline starts.
    const operators = [node];
    let source = null;
    
    // Find the data source (scan or breaker child)
    const child = node._child || node._input || node._left;
    if (child) {
      const childResult = walk(child);
      if (childResult && childResult.type === 'breaker') {
        source = childResult;
      } else if (childResult && childResult.type === 'pipeline') {
        // Merge into this pipeline
        operators.push(...childResult.operators);
        source = childResult.source;
      }
    }
    
    return { type: 'pipeline', operators, source, root: node };
  }
  
  return walk(root);
}

/**
 * Compile a pipeline segment (SeqScan → Filter → Project) into a single JS function.
 * The generated function does all work in a tight loop with no virtual dispatch.
 */
export function compilePipeline(operators, options = {}) {
  // Analyze the operator chain
  const scan = operators.find(op => op instanceof SeqScan || op instanceof IndexScan);
  const filters = operators.filter(op => op instanceof Filter);
  const projects = operators.filter(op => op instanceof Project);
  const limit = operators.find(op => op instanceof Limit);
  
  if (!scan) {
    // Can't compile without a scan — fall back to Volcano
    return null;
  }
  
  // Generate a compiled function that does scan+filter+project in one pass
  const compiledFn = function* execute() {
    let count = 0;
    const maxRows = limit ? limit._limit : Infinity;
    
    // Scan source
    scan.open();
    let row;
    while ((row = scan.next()) !== null && count < maxRows) {
      // Apply filters
      let pass = true;
      for (const filter of filters) {
        if (!filter._predicate(row)) {
          pass = false;
          break;
        }
      }
      if (!pass) continue;
      
      // Apply projections
      let result = row;
      for (const project of projects) {
        if (project._projections) {
          const out = {};
          for (const { name, expr } of project._projections) {
            out[name] = expr(result);
          }
          result = out;
        } else if (project._transform) {
          result = project._transform(result);
        }
      }
      
      count++;
      yield result;
    }
    scan.close();
  };
  
  return {
    type: 'compiled',
    operators: operators.map(op => op.constructor.name),
    execute: compiledFn,
    description: `Compiled(${operators.map(op => op.constructor.name).join(' → ')})`
  };
}

/**
 * CompiledIterator — wraps a compiled pipeline as an Iterator for compatibility
 */
export class CompiledIterator extends Iterator {
  constructor(compiledPipeline) {
    super();
    this._compiled = compiledPipeline;
    this._gen = null;
  }
  
  open() {
    this._gen = this._compiled.execute();
  }
  
  next() {
    const result = this._gen.next();
    return result.done ? null : result.value;
  }
  
  close() {
    this._gen = null;
  }
  
  describe() {
    return {
      type: 'CompiledPipeline',
      children: [],
      details: { ops: this._compiled.description }
    };
  }
}

/**
 * Compile a full query plan: replace pipeline-compatible segments with compiled functions.
 * Returns a new iterator tree with compiled segments.
 */
export function compileQueryPlan(root) {
  // For now, compile simple chains: SeqScan → Filter → Project → Limit
  const chain = [];
  let current = root;
  
  // Walk down the single-child chain
  while (current) {
    const name = current.constructor.name;
    
    if (BREAKERS.has(name)) {
      // Hit a breaker — stop, can't compile past it
      break;
    }
    
    if (current instanceof SeqScan || current instanceof IndexScan ||
        current instanceof Filter || current instanceof Project ||
        current instanceof Limit) {
      chain.push(current);
    } else {
      // Unknown operator — can't compile
      break;
    }
    
    current = current._child || current._input;
  }
  
  if (chain.length < 2) {
    // Not worth compiling a single operator
    return root;
  }
  
  const compiled = compilePipeline(chain);
  if (!compiled) return root;
  
  return new CompiledIterator(compiled);
}

/**
 * Generate a specialized predicate function from a SQL-like expression.
 * Uses Function() constructor for maximum performance.
 * 
 * Example: compilePredicate({column: 'age', op: '>', value: 18})
 * Returns: (row) => row.age > 18
 */
export function compilePredicate(expr) {
  if (typeof expr === 'function') return expr;
  
  if (expr.op === '=' || expr.op === '==') {
    const col = expr.column;
    const val = expr.value;
    return new Function('row', `return row.${col} === ${JSON.stringify(val)}`);
  }
  if (expr.op === '>' || expr.op === '<' || expr.op === '>=' || expr.op === '<=') {
    return new Function('row', `return row.${expr.column} ${expr.op} ${JSON.stringify(expr.value)}`);
  }
  if (expr.op === 'BETWEEN') {
    return new Function('row', `return row.${expr.column} >= ${JSON.stringify(expr.low)} && row.${expr.column} <= ${JSON.stringify(expr.high)}`);
  }
  
  throw new Error(`Cannot compile predicate: ${JSON.stringify(expr)}`);
}

/**
 * Generate a specialized projection function.
 * Example: compileProjection(['name', 'age']) 
 * Returns: (row) => ({ name: row.name, age: row.age })
 */
export function compileProjection(columns) {
  if (typeof columns === 'function') return columns;
  
  const body = columns.map(c => `${c}: row.${c}`).join(', ');
  return new Function('row', `return { ${body} }`);
}
