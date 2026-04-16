/**
 * Datatype-Generic Programming
 * 
 * Program over the SHAPE of data types, not their specific constructors.
 * 
 * Key idea: every algebraic data type can be decomposed into:
 * - Sum of products (constructor choices × fields)
 * - A "pattern functor" F such that the type T = Fix F
 * 
 * Then generic operations like fold, map, show, eq work for ANY type
 * by operating on its shape.
 */

// Shape functors (building blocks)
class FUnit { constructor() { this.tag = 'FUnit'; } } // 1 (no data)
class FConst { constructor(value) { this.tag = 'FConst'; this.value = value; } } // Constant
class FId { constructor(value) { this.tag = 'FId'; this.value = value; } } // Recursive position
class FProd { constructor(left, right) { this.tag = 'FProd'; this.left = left; this.right = right; } } // Product (×)
class FSum { constructor(tag, value) { this.tag = 'FSum'; this.which = tag; this.value = value; } } // Sum (+)

// Fixed point: T = F(T)
class Fix { constructor(unfix) { this.tag = 'Fix'; this.unfix = unfix; } }

function fix(f) { return new Fix(f); }
function unfix(t) { return t.unfix; }

// ============================================================
// Generic fold (catamorphism)
// ============================================================

function cata(algebra, term) {
  const layer = unfix(term);
  const mapped = fmap(x => cata(algebra, x), layer);
  return algebra(mapped);
}

function fmap(f, layer) {
  switch (layer.tag) {
    case 'FUnit': return layer;
    case 'FConst': return layer;
    case 'FId': return new FId(f(layer.value));
    case 'FProd': return new FProd(fmap(f, layer.left), fmap(f, layer.right));
    case 'FSum': return new FSum(layer.which, fmap(f, layer.value));
    default: return layer;
  }
}

// ============================================================
// Generic unfold (anamorphism)
// ============================================================

function ana(coalgebra, seed) {
  const layer = coalgebra(seed);
  return fix(fmap(x => ana(coalgebra, x), layer));
}

// ============================================================
// Example: Lists as Fix (1 + a × rec)
// ============================================================

function mkNil() { return fix(new FSum('nil', new FUnit())); }
function mkCons(head, tail) { return fix(new FSum('cons', new FProd(new FConst(head), new FId(tail)))); }

function listFromArray(arr) {
  let result = mkNil();
  for (let i = arr.length - 1; i >= 0; i--) result = mkCons(arr[i], result);
  return result;
}

// Fold a list to compute sum
function listSum(list) {
  return cata(layer => {
    if (layer.which === 'nil') return 0;
    return layer.value.left.value + layer.value.right.value;
  }, list);
}

// Fold a list to compute length
function listLength(list) {
  return cata(layer => {
    if (layer.which === 'nil') return 0;
    return 1 + layer.value.right.value;
  }, list);
}

// Fold a list to array
function listToArray(list) {
  return cata(layer => {
    if (layer.which === 'nil') return [];
    return [layer.value.left.value, ...layer.value.right.value];
  }, list);
}

// ============================================================
// Example: Trees as Fix (a + rec × rec)
// ============================================================

function mkLeaf(n) { return fix(new FSum('leaf', new FConst(n))); }
function mkBranch(left, right) { return fix(new FSum('branch', new FProd(new FId(left), new FId(right)))); }

function treeSum(tree) {
  return cata(layer => {
    if (layer.which === 'leaf') return layer.value.value;
    return layer.value.left.value + layer.value.right.value;
  }, tree);
}

function treeDepth(tree) {
  return cata(layer => {
    if (layer.which === 'leaf') return 0;
    return 1 + Math.max(layer.value.left.value, layer.value.right.value);
  }, tree);
}

export {
  FUnit, FConst, FId, FProd, FSum, Fix,
  fix, unfix, cata, ana, fmap,
  mkNil, mkCons, listFromArray, listSum, listLength, listToArray,
  mkLeaf, mkBranch, treeSum, treeDepth
};
