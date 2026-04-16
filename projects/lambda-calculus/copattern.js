/**
 * Copattern Matching: Define codata by how it responds to destructors
 * 
 * Pattern matching: define data by constructors (how to BUILD)
 * Copattern matching: define codata by destructors (how to OBSERVE)
 * 
 * stream.head = 1
 * stream.tail.head = 2
 * stream.tail.tail = repeat 3
 */

class Copattern {
  constructor(destructor, result) { this.destructor = destructor; this.result = result; }
}

class CoObject {
  constructor(name, copatterns) {
    this.name = name;
    this.copatterns = new Map(copatterns.map(cp => [cp.destructor, cp.result]));
  }
  
  observe(destructor) {
    if (!this.copatterns.has(destructor)) throw new Error(`No copattern for ${destructor} on ${this.name}`);
    const result = this.copatterns.get(destructor);
    return typeof result === 'function' ? result() : result;
  }
}

// Stream: infinite lazy sequence
function coStream(headFn, tailFn) {
  return new CoObject('Stream', [
    new Copattern('head', headFn),
    new Copattern('tail', tailFn),
  ]);
}

function repeat(value) {
  return coStream(() => value, () => repeat(value));
}

function iterate(f, seed) {
  return coStream(() => seed, () => iterate(f, f(seed)));
}

function take(stream, n) {
  const result = [];
  let current = stream;
  for (let i = 0; i < n; i++) {
    result.push(current.observe('head'));
    current = current.observe('tail');
  }
  return result;
}

function coMap(f, stream) {
  return coStream(
    () => f(stream.observe('head')),
    () => coMap(f, stream.observe('tail'))
  );
}

function zipWith(f, s1, s2) {
  return coStream(
    () => f(s1.observe('head'), s2.observe('head')),
    () => zipWith(f, s1.observe('tail'), s2.observe('tail'))
  );
}

// Record codata: {fst: A, snd: B}
function coRecord(fields) {
  return new CoObject('Record', Object.entries(fields).map(([k, v]) => new Copattern(k, v)));
}

export { Copattern, CoObject, coStream, repeat, iterate, take, coMap, zipWith, coRecord };
