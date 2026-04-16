/**
 * Type-Level Lists: Operations on type-level lists
 * 
 * TypeScript-style type-level programming with lists:
 * Head<[A, B, C]> = A
 * Tail<[A, B, C]> = [B, C]
 * Length<[A, B, C]> = 3
 * Concat<[A], [B, C]> = [A, B, C]
 */

class TList { constructor(elements) { this.tag = 'TList'; this.elements = elements; } toString() { return `[${this.elements.join(', ')}]`; } }
class TNil { constructor() { this.tag = 'TNil'; } toString() { return '[]'; } }

const tNil = new TNil();
function tList(...elems) { return new TList(elems); }

function head(list) {
  if (list.tag === 'TNil') return null;
  return list.elements[0];
}

function tail(list) {
  if (list.tag === 'TNil') return tNil;
  return list.elements.length <= 1 ? tNil : new TList(list.elements.slice(1));
}

function length(list) {
  if (list.tag === 'TNil') return 0;
  return list.elements.length;
}

function concat(a, b) {
  if (a.tag === 'TNil') return b;
  if (b.tag === 'TNil') return a;
  return new TList([...a.elements, ...b.elements]);
}

function reverse(list) {
  if (list.tag === 'TNil') return tNil;
  return new TList([...list.elements].reverse());
}

function map(f, list) {
  if (list.tag === 'TNil') return tNil;
  return new TList(list.elements.map(f));
}

function filter(pred, list) {
  if (list.tag === 'TNil') return tNil;
  const filtered = list.elements.filter(pred);
  return filtered.length === 0 ? tNil : new TList(filtered);
}

function zip(a, b) {
  if (a.tag === 'TNil' || b.tag === 'TNil') return tNil;
  const pairs = a.elements.map((el, i) => i < b.elements.length ? [el, b.elements[i]] : null).filter(Boolean);
  return pairs.length === 0 ? tNil : new TList(pairs);
}

function flatten(listOfLists) {
  if (listOfLists.tag === 'TNil') return tNil;
  const all = listOfLists.elements.flatMap(l => l.tag === 'TNil' ? [] : l.elements);
  return all.length === 0 ? tNil : new TList(all);
}

function includes(list, elem) {
  if (list.tag === 'TNil') return false;
  return list.elements.includes(elem);
}

function unique(list) {
  if (list.tag === 'TNil') return tNil;
  return new TList([...new Set(list.elements)]);
}

export { TList, TNil, tNil, tList, head, tail, length, concat, reverse, map, filter, zip, flatten, includes, unique };
