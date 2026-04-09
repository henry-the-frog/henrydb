// object-pool.js — Reusable object pool to reduce GC pressure
export class ObjectPool {
  constructor(factory, reset, initialSize = 10) {
    this._factory = factory;
    this._reset = reset || (obj => obj);
    this._pool = [];
    this._inUse = 0;
    for (let i = 0; i < initialSize; i++) this._pool.push(this._factory());
  }

  acquire() {
    this._inUse++;
    return this._pool.length > 0 ? this._reset(this._pool.pop()) : this._factory();
  }

  release(obj) {
    this._inUse--;
    this._pool.push(obj);
  }

  get available() { return this._pool.length; }
  get inUse() { return this._inUse; }
}
