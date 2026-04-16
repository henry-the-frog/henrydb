/**
 * Capabilities: Object capability security model
 */
class Capability { constructor(name, actions) { this.name = name; this.actions = new Set(actions); } can(action) { return this.actions.has(action); } }

function attenuate(cap, allowed) { return new Capability(cap.name + '/attenuated', [...cap.actions].filter(a => allowed.includes(a))); }

function combine(caps) {
  const actions = new Set();
  for (const c of caps) for (const a of c.actions) actions.add(a);
  return new Capability('combined', actions);
}

function revocable(cap) {
  let revoked = false;
  return {
    cap: { name: cap.name, actions: cap.actions, can(a) { if (revoked) return false; return cap.can(a); } },
    revoke() { revoked = true; },
    isRevoked() { return revoked; }
  };
}

function guard(cap, action, fn) {
  if (!cap.can(action)) throw new Error(`Denied: ${action} on ${cap.name}`);
  return fn();
}

function membrane(inner, allowed) {
  return new Proxy(inner, {
    get(target, prop) {
      if (!allowed.includes(prop)) throw new Error(`Blocked: ${prop}`);
      return target[prop];
    }
  });
}

export { Capability, attenuate, combine, revocable, guard, membrane };
