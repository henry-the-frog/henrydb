/**
 * Typed State: Indexed state monad (state type can change)
 * 
 * Regular state: State s a = s → (a, s)     (state type stays same)
 * Indexed state: IxState i j a = i → (a, j)  (state type can change!)
 * 
 * Example: File handle state: Open → Read/Write → Close
 * The type tracks the state transition.
 */

class IxState {
  constructor(run) { this.run = run; } // i → {value: a, state: j}
  
  static pure(value) { return new IxState(s => ({ value, state: s })); }
  
  static get() { return new IxState(s => ({ value: s, state: s })); }
  static put(newState) { return new IxState(s => ({ value: null, state: newState })); }
  static modify(f) { return new IxState(s => ({ value: null, state: f(s) })); }
  
  map(f) { return new IxState(s => { const r = this.run(s); return { value: f(r.value), state: r.state }; }); }
  
  chain(f) {
    return new IxState(s => {
      const r = this.run(s);
      return f(r.value).run(r.state);
    });
  }
  
  exec(initState) { return this.run(initState); }
}

// Type-safe protocol: state transitions encoded in types
class Protocol {
  constructor(transitions) { this.transitions = transitions; }
  
  canTransition(from, action) {
    return this.transitions.some(t => t.from === from && t.action === action);
  }
  
  nextState(from, action) {
    const t = this.transitions.find(t => t.from === from && t.action === action);
    return t ? t.to : null;
  }
}

function protocolAction(protocol, action) {
  return new IxState(state => {
    if (!protocol.canTransition(state, action)) throw new Error(`Invalid: ${action} from ${state}`);
    return { value: action, state: protocol.nextState(state, action) };
  });
}

// Resource management
function bracket(acquire, use, release) {
  return acquire.chain(resource =>
    use(resource).chain(result =>
      release(resource).map(() => result)));
}

export { IxState, Protocol, protocolAction, bracket };
