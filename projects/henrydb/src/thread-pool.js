// thread-pool.js — Work-stealing task scheduler simulation
export class ThreadPool {
  constructor(workers = 4) {
    this._queues = Array.from({ length: workers }, () => []);
    this._workers = workers;
    this._completed = [];
  }

  submit(task) {
    // Add to shortest queue
    let minLen = Infinity, minIdx = 0;
    for (let i = 0; i < this._workers; i++) {
      if (this._queues[i].length < minLen) { minLen = this._queues[i].length; minIdx = i; }
    }
    this._queues[minIdx].push(task);
  }

  /** Simulate one round of execution with work stealing. */
  tick() {
    for (let i = 0; i < this._workers; i++) {
      if (this._queues[i].length > 0) {
        this._completed.push(this._queues[i].shift());
      } else {
        // Work stealing: steal from longest queue
        let maxLen = 0, maxIdx = -1;
        for (let j = 0; j < this._workers; j++) {
          if (j !== i && this._queues[j].length > maxLen) { maxLen = this._queues[j].length; maxIdx = j; }
        }
        if (maxIdx >= 0 && this._queues[maxIdx].length > 1) {
          this._completed.push(this._queues[maxIdx].pop());
        }
      }
    }
  }

  runAll() { while (this._queues.some(q => q.length > 0)) this.tick(); }
  get completed() { return this._completed; }
  getStats() { return { queues: this._queues.map(q => q.length), completed: this._completed.length }; }
}
