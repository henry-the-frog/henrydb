/**
 * Module #165: Scoped Effects — Effects with lexical scope
 */

class ScopedHandler {
  constructor(name, handlers) { this.name = name; this.handlers = handlers; }
  handle(comp) {
    try { return { ok: true, value: comp(this.handlers) }; }
    catch (e) { if (e._effect) return this.handlers[e._effect] ? { ok: true, value: this.handlers[e._effect](...e._args) } : { ok: false, error: e }; throw e; }
  }
}

function perform(name, ...args) {
  const e = new Error(`Unhandled: ${name}`);
  e._effect = name; e._args = args;
  throw e;
}

function withHandler(name, handlers, body) {
  const h = new ScopedHandler(name, handlers);
  return h.handle(body);
}

// Scoped: inner handler shadows outer
function nested(outerHandlers, innerHandlers, body) {
  return withHandler('outer', outerHandlers, ops => {
    return withHandler('inner', innerHandlers, body).value;
  });
}

// Reader effect
function ask(handlers) { return handlers.ask ? handlers.ask() : perform('ask'); }
function local(handlers, f, body) {
  const oldAsk = handlers.ask;
  const newHandlers = { ...handlers, ask: () => f(oldAsk()) };
  return body(newHandlers);
}

// State effect (via handlers)
function stateHandler(init) {
  let state = init;
  return {
    get: () => state,
    put: (v) => { state = v; },
    modify: (f) => { state = f(state); },
  };
}

export { ScopedHandler, perform, withHandler, nested, ask, local, stateHandler };
