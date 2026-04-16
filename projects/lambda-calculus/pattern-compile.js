/**
 * Pattern Matching Compilation
 * 
 * Compiles pattern match expressions into efficient decision trees.
 * No backtracking — each value examined at most once.
 * 
 * Patterns:
 * - Wildcard (_): matches anything
 * - Variable (x): matches anything, binds to x
 * - Constructor (Con(p1, p2, ...)): matches constructor, recurse on fields
 * - Literal (42, "hello"): matches exact value
 * 
 * Decision tree nodes:
 * - Leaf(bindings, body): matched, execute body with bindings
 * - Switch(scrutinee, cases): test constructor tag, branch
 * - Fail: no match (should not reach if exhaustive)
 * 
 * Based on: Maranget (2008) "Compiling Pattern Matching to Good Decision Trees"
 */

// ============================================================
// Patterns
// ============================================================

class PWild { constructor() { this.tag = 'PWild'; } toString() { return '_'; } }
class PVar { constructor(name) { this.tag = 'PVar'; this.name = name; } toString() { return this.name; } }
class PCon { constructor(name, args) { this.tag = 'PCon'; this.name = name; this.args = args; } toString() { return `${this.name}(${this.args.join(', ')})`; } }
class PLit { constructor(value) { this.tag = 'PLit'; this.value = value; } toString() { return `${this.value}`; } }

// ============================================================
// Match clauses
// ============================================================

class Clause {
  constructor(patterns, body) {
    this.patterns = patterns;  // Array of patterns (for multi-column matching)
    this.body = body;          // Body expression
  }
  toString() { return `${this.patterns.join(', ')} → ${this.body}`; }
}

// ============================================================
// Decision tree
// ============================================================

class DLeaf {
  constructor(bindings, body) { this.tag = 'DLeaf'; this.bindings = bindings; this.body = body; }
  toString() {
    const binds = this.bindings.length > 0 ? `[${this.bindings.map(b => `${b.name}=${b.expr}`).join(', ')}] ` : '';
    return `Leaf(${binds}${this.body})`;
  }
}

class DSwitch {
  constructor(scrutinee, cases, defaultCase) {
    this.tag = 'DSwitch';
    this.scrutinee = scrutinee;
    this.cases = cases;       // Map<conName, DecisionTree>
    this.defaultCase = defaultCase;
  }
  toString() {
    const cases = [...this.cases.entries()].map(([k, v]) => `${k} → ${v}`).join('; ');
    return `Switch(${this.scrutinee}, {${cases}}${this.defaultCase ? `, default: ${this.defaultCase}` : ''})`;
  }
}

class DFail {
  constructor() { this.tag = 'DFail'; }
  toString() { return 'FAIL'; }
}

// ============================================================
// Values (for testing)
// ============================================================

class VCon { constructor(name, args) { this.tag = 'VCon'; this.name = name; this.args = args; } }
class VLit { constructor(value) { this.tag = 'VLit'; this.value = value; } }

// ============================================================
// Pattern Match Compiler
// ============================================================

class PatternCompiler {
  /**
   * Compile a list of clauses into a decision tree.
   * scrutinees: array of expression identifiers being matched
   * clauses: array of Clause objects
   */
  compile(scrutinees, clauses) {
    if (clauses.length === 0) return new DFail();
    
    // Check if first clause is all wildcards/variables
    const firstClause = clauses[0];
    const allWild = firstClause.patterns.every(p => p.tag === 'PWild' || p.tag === 'PVar');
    
    if (allWild) {
      // Leaf: bind variables and return body
      const bindings = [];
      for (let i = 0; i < firstClause.patterns.length; i++) {
        if (firstClause.patterns[i].tag === 'PVar') {
          bindings.push({ name: firstClause.patterns[i].name, expr: scrutinees[i] });
        }
      }
      return new DLeaf(bindings, firstClause.body);
    }
    
    // Find first column with a constructor/literal pattern
    let col = -1;
    for (let i = 0; i < scrutinees.length; i++) {
      if (clauses.some(c => c.patterns[i].tag === 'PCon' || c.patterns[i].tag === 'PLit')) {
        col = i;
        break;
      }
    }
    
    if (col === -1) return new DLeaf([], firstClause.body);
    
    // Group clauses by constructor/literal at this column
    const groups = new Map(); // conName → clauses with that constructor
    const wildcardClauses = [];  // clauses with wildcard at this column
    
    for (const clause of clauses) {
      const pat = clause.patterns[col];
      if (pat.tag === 'PCon') {
        if (!groups.has(pat.name)) groups.set(pat.name, []);
        groups.get(pat.name).push(clause);
      } else if (pat.tag === 'PLit') {
        const key = `lit:${pat.value}`;
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push(clause);
      } else {
        // Wild/Var: applies to all cases
        wildcardClauses.push(clause);
      }
    }
    
    // Build switch cases
    const cases = new Map();
    for (const [conName, conClauses] of groups) {
      // For constructor patterns: expand subpatterns
      const expandedClauses = [...conClauses, ...wildcardClauses].map(clause => {
        const pat = clause.patterns[col];
        if (pat.tag === 'PCon') {
          // Replace constructor pattern with its subpatterns
          const newPatterns = [
            ...clause.patterns.slice(0, col),
            ...pat.args,
            ...clause.patterns.slice(col + 1)
          ];
          return new Clause(newPatterns, clause.body);
        }
        if (pat.tag === 'PLit') {
          // Literal matched: remove this column
          const newPatterns = [
            ...clause.patterns.slice(0, col),
            ...clause.patterns.slice(col + 1)
          ];
          return new Clause(newPatterns, clause.body);
        }
        // Wild/Var: expand to wildcards for subpatterns
        const numArgs = conClauses[0]?.patterns[col].tag === 'PCon' 
          ? conClauses[0].patterns[col].args.length : 0;
        const newPatterns = [
          ...clause.patterns.slice(0, col),
          ...Array(numArgs).fill(new PWild()),
          ...clause.patterns.slice(col + 1)
        ];
        return new Clause(newPatterns, clause.body);
      });
      
      const conArgs = conClauses[0]?.patterns[col].tag === 'PCon'
        ? conClauses[0].patterns[col].args.map((_, i) => `${scrutinees[col]}.${i}`)
        : [];
      const newScrutinees = [
        ...scrutinees.slice(0, col),
        ...conArgs,
        ...scrutinees.slice(col + 1)
      ];
      
      cases.set(conName, this.compile(newScrutinees, expandedClauses));
    }
    
    // Default case: wildcard clauses
    const defaultTree = wildcardClauses.length > 0
      ? this.compile(
          [...scrutinees.slice(0, col), ...scrutinees.slice(col + 1)],
          wildcardClauses.map(c => new Clause(
            [...c.patterns.slice(0, col), ...c.patterns.slice(col + 1)],
            c.body)))
      : new DFail();
    
    return new DSwitch(scrutinees[col], cases, defaultTree);
  }
}

// ============================================================
// Decision Tree Evaluator
// ============================================================

function evalDecisionTree(tree, env) {
  switch (tree.tag) {
    case 'DLeaf': {
      const fullEnv = new Map(env);
      for (const b of tree.bindings) {
        fullEnv.set(b.name, env.get(b.expr) ?? b.expr);
      }
      return { matched: true, bindings: tree.bindings, body: tree.body };
    }
    case 'DSwitch': {
      const val = env.get(tree.scrutinee);
      if (!val) return { matched: false };
      
      const tag = val.tag === 'VCon' ? val.name : `lit:${val.value}`;
      
      if (tree.cases.has(tag)) {
        // Set up sub-scrutinee bindings
        const newEnv = new Map(env);
        if (val.tag === 'VCon') {
          for (let i = 0; i < val.args.length; i++) {
            newEnv.set(`${tree.scrutinee}.${i}`, val.args[i]);
          }
        }
        return evalDecisionTree(tree.cases.get(tag), newEnv);
      }
      
      if (tree.defaultCase) {
        return evalDecisionTree(tree.defaultCase, env);
      }
      
      return { matched: false };
    }
    case 'DFail':
      return { matched: false };
    default:
      throw new Error(`Unknown tree: ${tree.tag}`);
  }
}

export {
  PWild, PVar, PCon, PLit, Clause,
  DLeaf, DSwitch, DFail,
  VCon, VLit,
  PatternCompiler, evalDecisionTree
};
