/**
 * Type-Level State Machine
 * 
 * Encode protocol states in types, prevent invalid transitions at compile time.
 * 
 * Example: File handle states
 *   Closed → open() → Open → read()/write() → Open → close() → Closed
 *   Cannot read from Closed file (type error!)
 * 
 * Example: HTTP request builder
 *   Empty → setMethod() → HasMethod → setUrl() → HasUrl → send() → Response
 */

// ============================================================
// State Machine Definition
// ============================================================

class StateMachine {
  constructor(name, states, transitions, initial) {
    this.name = name;
    this.states = states;           // Set<string>
    this.transitions = transitions; // [{from, action, to}]
    this.initial = initial;
  }

  /**
   * Check if a transition is valid
   */
  canTransition(currentState, action) {
    return this.transitions.some(t => t.from === currentState && t.action === action);
  }

  /**
   * Get next state after action
   */
  nextState(currentState, action) {
    const t = this.transitions.find(t => t.from === currentState && t.action === action);
    if (!t) return null;
    return t.to;
  }

  /**
   * Validate a sequence of actions
   */
  validateSequence(actions) {
    let state = this.initial;
    const trace = [state];
    
    for (const action of actions) {
      const next = this.nextState(state, action);
      if (!next) {
        return {
          valid: false,
          error: `Invalid transition: ${action} from state ${state}`,
          trace,
          failedAt: actions.indexOf(action)
        };
      }
      state = next;
      trace.push(state);
    }
    
    return { valid: true, trace, finalState: state };
  }

  /**
   * Get all reachable states from a given state
   */
  reachableFrom(state) {
    const visited = new Set();
    const queue = [state];
    
    while (queue.length > 0) {
      const current = queue.pop();
      if (visited.has(current)) continue;
      visited.add(current);
      
      for (const t of this.transitions) {
        if (t.from === current && !visited.has(t.to)) {
          queue.push(t.to);
        }
      }
    }
    
    return visited;
  }

  /**
   * Check if all states are reachable from initial
   */
  isFullyReachable() {
    const reachable = this.reachableFrom(this.initial);
    return [...this.states].every(s => reachable.has(s));
  }
}

// ============================================================
// Type-safe handle (runtime enforcement of state machine)
// ============================================================

class TypedHandle {
  constructor(machine, state = null) {
    this.machine = machine;
    this.state = state || machine.initial;
  }

  /**
   * Perform an action (throws if invalid transition)
   */
  do(action) {
    const next = this.machine.nextState(this.state, action);
    if (!next) {
      throw new Error(`Invalid: cannot ${action} in state ${this.state}`);
    }
    return new TypedHandle(this.machine, next);
  }

  /**
   * Check if action is available
   */
  can(action) {
    return this.machine.canTransition(this.state, action);
  }

  /**
   * Get available actions
   */
  availableActions() {
    return this.machine.transitions
      .filter(t => t.from === this.state)
      .map(t => t.action);
  }
}

// ============================================================
// Predefined state machines
// ============================================================

const FileHandle = new StateMachine('FileHandle',
  new Set(['Closed', 'Open', 'EOF']),
  [
    { from: 'Closed', action: 'open', to: 'Open' },
    { from: 'Open', action: 'read', to: 'Open' },
    { from: 'Open', action: 'write', to: 'Open' },
    { from: 'Open', action: 'close', to: 'Closed' },
    { from: 'Open', action: 'readToEnd', to: 'EOF' },
    { from: 'EOF', action: 'close', to: 'Closed' },
  ],
  'Closed'
);

const HttpRequest = new StateMachine('HttpRequest',
  new Set(['Empty', 'HasMethod', 'HasUrl', 'HasHeaders', 'Sent']),
  [
    { from: 'Empty', action: 'setMethod', to: 'HasMethod' },
    { from: 'HasMethod', action: 'setUrl', to: 'HasUrl' },
    { from: 'HasUrl', action: 'addHeader', to: 'HasHeaders' },
    { from: 'HasHeaders', action: 'addHeader', to: 'HasHeaders' },
    { from: 'HasUrl', action: 'send', to: 'Sent' },
    { from: 'HasHeaders', action: 'send', to: 'Sent' },
  ],
  'Empty'
);

const TCPConnection = new StateMachine('TCP',
  new Set(['Closed', 'Listen', 'SynSent', 'SynReceived', 'Established', 'FinWait', 'TimeWait']),
  [
    { from: 'Closed', action: 'listen', to: 'Listen' },
    { from: 'Closed', action: 'connect', to: 'SynSent' },
    { from: 'Listen', action: 'accept', to: 'SynReceived' },
    { from: 'SynSent', action: 'synAck', to: 'Established' },
    { from: 'SynReceived', action: 'ack', to: 'Established' },
    { from: 'Established', action: 'send', to: 'Established' },
    { from: 'Established', action: 'receive', to: 'Established' },
    { from: 'Established', action: 'close', to: 'FinWait' },
    { from: 'FinWait', action: 'ack', to: 'TimeWait' },
    { from: 'TimeWait', action: 'timeout', to: 'Closed' },
  ],
  'Closed'
);

export { StateMachine, TypedHandle, FileHandle, HttpRequest, TCPConnection };
