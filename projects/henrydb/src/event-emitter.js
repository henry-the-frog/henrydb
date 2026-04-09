// event-emitter.js — Simple pub/sub event system
export class EventEmitter {
  constructor() { this._handlers = new Map(); }

  on(event, handler) {
    if (!this._handlers.has(event)) this._handlers.set(event, []);
    this._handlers.get(event).push(handler);
    return () => this.off(event, handler);
  }

  off(event, handler) {
    const h = this._handlers.get(event);
    if (h) this._handlers.set(event, h.filter(fn => fn !== handler));
  }

  emit(event, ...args) {
    const handlers = this._handlers.get(event) || [];
    for (const h of handlers) h(...args);
    // Also emit to wildcard listeners
    const wild = this._handlers.get('*') || [];
    for (const h of wild) h(event, ...args);
  }

  once(event, handler) {
    const wrapped = (...args) => { handler(...args); this.off(event, wrapped); };
    this.on(event, wrapped);
  }
}
