// connection-pool.js — Database connection pool
export class ConnectionPool {
  constructor(size, factory) {
    this._factory = factory || (() => ({ id: Math.random(), connected: true }));
    this._pool = [];
    this._inUse = new Set();
    this._maxSize = size;
    for (let i = 0; i < size; i++) this._pool.push(this._factory());
  }

  acquire() {
    if (this._pool.length > 0) {
      const conn = this._pool.pop();
      this._inUse.add(conn);
      return conn;
    }
    return null; // Pool exhausted
  }

  release(conn) {
    this._inUse.delete(conn);
    this._pool.push(conn);
  }

  getStats() {
    return { available: this._pool.length, inUse: this._inUse.size, max: this._maxSize };
  }
}
