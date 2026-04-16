/**
 * Effect Handlers (Frank-style): First-class handlers with do-notation
 */
class Effect { constructor(name, ops) { this.name = name; this.ops = ops; } }
class Handler {
  constructor(returnCase, opCases) { this.returnCase = returnCase; this.opCases = opCases; }
  handle(comp) {
    try { return this.returnCase(comp()); }
    catch (e) { if (e._op && this.opCases[e._op]) return this.opCases[e._op](e._arg, k => this.handle(() => k)); throw e; }
  }
}

function perform(op, arg) { const e = new Error(); e._op = op; e._arg = arg; throw e; }

// State handler
function stateHandler(init) {
  let s = init;
  return new Handler(v => [v, s], {
    get: (_, k) => { const result = k(s); return result; },
    put: (newS, k) => { s = newS; return k(null); },
  });
}

// Exception handler
function exnHandler() {
  return new Handler(v => ({ ok: true, value: v }), {
    raise: (msg, k) => ({ ok: false, error: msg }),
  });
}

// Choice handler (collect all)
function choiceHandler() {
  return new Handler(v => [v], {
    choose: (options, k) => options.flatMap(o => { try { return k(o); } catch { return []; } }),
  });
}

export { Effect, Handler, perform, stateHandler, exnHandler, choiceHandler };
