/**
 * GADTs (Generalized Algebraic Data Types)
 * 
 * Like regular ADTs, but constructors can refine the type parameter.
 * 
 * Example:
 *   type Expr a where
 *     IntLit  : Int → Expr Int
 *     BoolLit : Bool → Expr Bool
 *     Add     : Expr Int → Expr Int → Expr Int
 *     Eq      : Expr Int → Expr Int → Expr Bool
 *     If      : Expr Bool → Expr a → Expr a → Expr a
 * 
 * The evaluator is total: eval : Expr a → a
 * Pattern matching on IntLit refines 'a' to Int, etc.
 */

// GADT definition
class GADTDef {
  constructor(name, constructors) {
    this.name = name;
    this.constructors = constructors; // [{name, argTypes, returnType}]
  }
}

// Type language
class TVar { constructor(name) { this.tag = 'TVar'; this.name = name; } toString() { return this.name; } }
class TCon { constructor(name) { this.tag = 'TCon'; this.name = name; } toString() { return this.name; } }
class TApp { constructor(con, arg) { this.tag = 'TApp'; this.con = con; this.arg = arg; } toString() { return `${this.con}<${this.arg}>`; } }

const tInt = new TCon('Int');
const tBool = new TCon('Bool');
const tStr = new TCon('Str');

// ============================================================
// GADT Values
// ============================================================

class GVal {
  constructor(conName, args) { this.tag = 'GVal'; this.conName = conName; this.args = args; }
  toString() { return `${this.conName}(${this.args.join(', ')})`; }
}

// ============================================================
// Type-safe GADT evaluator (the killer feature)
// ============================================================

class ExprGADT {
  // Define the Expr GADT
  static IntLit(n) { return new GVal('IntLit', [n]); }
  static BoolLit(b) { return new GVal('BoolLit', [b]); }
  static Add(l, r) { return new GVal('Add', [l, r]); }
  static Eq(l, r) { return new GVal('Eq', [l, r]); }
  static If(cond, then, els) { return new GVal('If', [cond, then, els]); }
  static Pair(a, b) { return new GVal('Pair', [a, b]); }
  static Fst(p) { return new GVal('Fst', [p]); }
  static Snd(p) { return new GVal('Snd', [p]); }
}

/**
 * Total, type-safe evaluation of Expr GADT.
 * The pattern match on constructor refines the type:
 *   IntLit(n) → we know result is Int
 *   BoolLit(b) → we know result is Bool
 *   Add(l,r) → we know l and r are Expr Int
 */
function evalExpr(expr) {
  switch (expr.conName) {
    case 'IntLit': return expr.args[0];
    case 'BoolLit': return expr.args[0];
    case 'Add': return evalExpr(expr.args[0]) + evalExpr(expr.args[1]);
    case 'Eq': return evalExpr(expr.args[0]) === evalExpr(expr.args[1]);
    case 'If': return evalExpr(expr.args[0]) ? evalExpr(expr.args[1]) : evalExpr(expr.args[2]);
    case 'Pair': return [evalExpr(expr.args[0]), evalExpr(expr.args[1])];
    case 'Fst': return evalExpr(expr.args[0])[0];
    case 'Snd': return evalExpr(expr.args[0])[1];
    default: throw new Error(`Unknown GADT constructor: ${expr.conName}`);
  }
}

// ============================================================
// GADT Type Checker
// ============================================================

class GADTTypeChecker {
  constructor(gadtDef) {
    this.def = gadtDef;
    this.constructorMap = new Map(gadtDef.constructors.map(c => [c.name, c]));
  }

  /**
   * Check that a GADT value has the expected type
   */
  check(value, expectedType) {
    const conInfo = this.constructorMap.get(value.conName);
    if (!conInfo) return { ok: false, error: `Unknown constructor: ${value.conName}` };
    
    // Check return type matches expected
    if (!this._unifyReturn(conInfo.returnType, expectedType)) {
      return { ok: false, error: `Constructor ${value.conName} returns ${conInfo.returnType}, expected ${expectedType}` };
    }
    
    // Check argument count
    if (value.args.length !== conInfo.argTypes.length) {
      return { ok: false, error: `${value.conName} expects ${conInfo.argTypes.length} args, got ${value.args.length}` };
    }
    
    return { ok: true };
  }

  _unifyReturn(returnType, expected) {
    if (returnType.tag === 'TVar') return true; // Type variable unifies with anything
    if (returnType.tag === expected.tag) {
      if (returnType.tag === 'TCon') return returnType.name === expected.name;
      if (returnType.tag === 'TApp') {
        return this._unifyReturn(returnType.con, expected.con) && this._unifyReturn(returnType.arg, expected.arg);
      }
    }
    return false;
  }
}

// ============================================================
// Define the Expr GADT formally
// ============================================================

const ExprGADTDef = new GADTDef('Expr', [
  { name: 'IntLit', argTypes: [tInt], returnType: new TApp(new TCon('Expr'), tInt) },
  { name: 'BoolLit', argTypes: [tBool], returnType: new TApp(new TCon('Expr'), tBool) },
  { name: 'Add', argTypes: [new TApp(new TCon('Expr'), tInt), new TApp(new TCon('Expr'), tInt)], returnType: new TApp(new TCon('Expr'), tInt) },
  { name: 'Eq', argTypes: [new TApp(new TCon('Expr'), tInt), new TApp(new TCon('Expr'), tInt)], returnType: new TApp(new TCon('Expr'), tBool) },
  { name: 'If', argTypes: [new TApp(new TCon('Expr'), tBool), new TApp(new TCon('Expr'), new TVar('a')), new TApp(new TCon('Expr'), new TVar('a'))], returnType: new TApp(new TCon('Expr'), new TVar('a')) },
]);

export {
  GADTDef, GADTTypeChecker, ExprGADTDef,
  TVar, TCon, TApp, tInt, tBool, tStr,
  GVal, ExprGADT, evalExpr
};
