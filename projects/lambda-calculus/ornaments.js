/**
 * Ornaments: Structured transformations between datatypes
 * 
 * An ornament describes how one datatype relates to another by:
 * - Adding fields (Nat → List: add element at each Succ)
 * - Deleting fields (List → Nat: forget elements)
 * - Renaming constructors
 * 
 * Ornaments give you "forgetful functions" for free (McBride 2011).
 */

class Datatype {
  constructor(name, constructors) {
    this.name = name;
    this.constructors = constructors; // Map<name, {fields: string[]}>
  }
}

class Ornament {
  constructor(source, target, mapping) {
    this.source = source;
    this.target = target;
    this.mapping = mapping; // Map<targetCtor, {from: sourceCtor, addedFields: string[], droppedFields: string[]}>
  }
}

// Derive forgetful function from ornament
function forget(ornament) {
  return (value) => {
    const m = ornament.mapping.get(value.tag);
    if (!m) throw new Error(`No mapping for ${value.tag}`);
    const result = { tag: m.from };
    // Copy fields that exist in source, drop added fields
    const sourceFields = ornament.source.constructors.get(m.from).fields;
    for (const f of sourceFields) {
      if (f in value) result[f] = value[f];
    }
    // Recursively forget nested structures
    if (result.tail !== undefined && typeof result.tail === 'object') {
      result.tail = forget(ornament)(result.tail);
    }
    return result;
  };
}

// Standard example: Nat ←ornament→ List
const Nat = new Datatype('Nat', new Map([['Zero', { fields: [] }], ['Succ', { fields: ['pred'] }]]));
const List = new Datatype('List', new Map([['Nil', { fields: [] }], ['Cons', { fields: ['head', 'tail'] }]]));

const listToNat = new Ornament(Nat, List, new Map([
  ['Nil', { from: 'Zero', addedFields: [], droppedFields: [] }],
  ['Cons', { from: 'Succ', addedFields: ['head'], droppedFields: [] }],
]));

// Length function derived from ornament
function length(list) {
  return forget(listToNat)(list);
}

// Count a Nat value
function natToNum(nat) {
  if (nat.tag === 'Zero') return 0;
  return 1 + natToNum(nat.pred || nat.tail);
}

// Build List and Nat values
function mkList(...elems) {
  let result = { tag: 'Nil' };
  for (let i = elems.length - 1; i >= 0; i--) result = { tag: 'Cons', head: elems[i], tail: result };
  return result;
}

function mkNat(n) {
  let result = { tag: 'Zero' };
  for (let i = 0; i < n; i++) result = { tag: 'Succ', pred: result };
  return result;
}

export { Datatype, Ornament, forget, Nat, List, listToNat, length, natToNum, mkList, mkNat };
