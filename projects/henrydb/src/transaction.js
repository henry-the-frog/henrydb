// transaction.js — Simple transaction manager
export class Transaction {
  constructor(id) {
    this.id = id;
    this.status = 'active'; // active, committed, aborted
    this.startTime = Date.now();
    this.writes = [];
    this.readSet = new Set();
    this.writeSet = new Set();
  }

  addRead(key) { this.readSet.add(key); }
  addWrite(key, value) { this.writeSet.add(key); this.writes.push({ key, value }); }
  commit() { this.status = 'committed'; this.endTime = Date.now(); }
  abort() { this.status = 'aborted'; this.endTime = Date.now(); }
  get isActive() { return this.status === 'active'; }
}

export class TransactionManager {
  constructor() { this._txns = new Map(); this._nextId = 1; }
  
  begin() { const tx = new Transaction(this._nextId++); this._txns.set(tx.id, tx); return tx; }
  get(id) { return this._txns.get(id); }
  commit(id) { this._txns.get(id)?.commit(); }
  abort(id) { this._txns.get(id)?.abort(); }
  
  getActive() { return [...this._txns.values()].filter(t => t.isActive); }
  get activeCount() { return this.getActive().length; }
}
