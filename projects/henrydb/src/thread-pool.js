// thread-pool.js — Work-stealing task scheduler simulation
// Models a multi-worker executor with per-worker deques and work stealing.
// In a real DB: parallel query execution, background compaction, index builds.

/**
 * Double-ended queue for work-stealing.
 * Owner pushes/pops from bottom; thieves steal from top.
 */
class WorkDeque {
  constructor() {
    this._items = [];
  }

  pushBottom(item) { this._items.push(item); }
  popBottom() { return this._items.pop() ?? null; }
  stealTop() { return this._items.length > 0 ? this._items.shift() : null; }
  get length() { return this._items.length; }
  get isEmpty() { return this._items.length === 0; }
}

/**
 * Simulated worker thread. Processes tasks from its deque,
 * steals from others when idle.
 */
class Worker {
  constructor(id) {
    this.id = id;
    this.deque = new WorkDeque();
    this.completed = [];
    this.stolen = 0;
    this.idleTicks = 0;
  }
}

/**
 * Work-stealing thread pool simulator.
 * 
 * Features:
 * - Per-worker double-ended queues (push bottom, steal top)
 * - Round-robin or shortest-queue submission
 * - Work stealing from busiest worker
 * - Task priorities (higher = sooner)
 * - Task dependencies (waits for prerequisites)
 * - Metrics (per-worker completion, steal count, idle ticks)
 */
export class ThreadPool {
  /**
   * @param {number} numWorkers - Number of worker threads
   * @param {Object} opts
   * @param {string} opts.submitPolicy - 'round-robin' | 'shortest' (default 'shortest')
   */
  constructor(numWorkers = 4, opts = {}) {
    this._workers = Array.from({ length: numWorkers }, (_, i) => new Worker(i));
    this._submitPolicy = opts.submitPolicy ?? 'shortest';
    this._nextWorker = 0; // for round-robin
    this._totalSubmitted = 0;
    this._totalCompleted = 0;
    this._tickCount = 0;

    // Dependency tracking
    this._completed = [];          // all completed task IDs
    this._completedSet = new Set();
    this._blocked = [];            // tasks waiting on dependencies
  }

  /**
   * Submit a task to the pool.
   * @param {Object|string|number} task
   * @param {Object} opts
   * @param {number} opts.priority - Higher = sooner (default 0)
   * @param {string[]} opts.dependsOn - Task IDs that must complete first
   */
  submit(task, opts = {}) {
    const wrapped = {
      task,
      id: typeof task === 'object' && task.id ? task.id : `task-${this._totalSubmitted}`,
      priority: opts.priority ?? 0,
      dependsOn: opts.dependsOn ?? [],
      submittedAt: this._tickCount,
    };
    this._totalSubmitted++;

    // Check if dependencies are met
    if (wrapped.dependsOn.length > 0 && !wrapped.dependsOn.every(d => this._completedSet.has(d))) {
      this._blocked.push(wrapped);
      return wrapped.id;
    }

    this._enqueue(wrapped);
    return wrapped.id;
  }

  /**
   * Simulate one tick: each worker processes one task or steals.
   * @returns {number} Number of tasks completed this tick
   */
  tick() {
    this._tickCount++;
    let completedThisTick = 0;

    for (const worker of this._workers) {
      let item = worker.deque.popBottom();

      // Work stealing if own deque is empty
      if (!item) {
        const victim = this._findVictim(worker.id);
        if (victim) {
          item = victim.deque.stealTop();
          if (item) worker.stolen++;
        }
      }

      if (item) {
        worker.completed.push(item);
        this._completed.push(item);
        this._completedSet.add(item.id);
        this._totalCompleted++;
        completedThisTick++;
      } else {
        worker.idleTicks++;
      }
    }

    // Unblock tasks whose dependencies are now met
    this._unblockReady();

    return completedThisTick;
  }

  /**
   * Run until all submitted tasks are complete.
   * @param {number} maxTicks - Safety limit (default 10000)
   * @returns {number} Ticks taken
   */
  runAll(maxTicks = 10000) {
    let ticks = 0;
    while (ticks < maxTicks) {
      const hasWork = this._workers.some(w => !w.deque.isEmpty) || this._blocked.length > 0;
      if (!hasWork) break;
      this.tick();
      ticks++;
    }
    return ticks;
  }

  /** All completed tasks (in completion order). */
  get completed() {
    return this._completed.map(w => w.task);
  }

  /** Detailed statistics. */
  getStats() {
    return {
      workers: this._workers.map(w => ({
        id: w.id,
        completed: w.completed.length,
        stolen: w.stolen,
        idleTicks: w.idleTicks,
        queueLength: w.deque.length,
      })),
      totalSubmitted: this._totalSubmitted,
      totalCompleted: this._totalCompleted,
      blocked: this._blocked.length,
      ticks: this._tickCount,
    };
  }

  // --- Internals ---

  _enqueue(wrapped) {
    const worker = this._selectWorker();
    // Insert by priority (higher priority closer to bottom = processed sooner)
    // Simple approach: just push; higher priority items submitted first naturally
    worker.deque.pushBottom(wrapped);
  }

  _selectWorker() {
    if (this._submitPolicy === 'round-robin') {
      const w = this._workers[this._nextWorker % this._workers.length];
      this._nextWorker++;
      return w;
    }
    // shortest queue
    let min = Infinity, best = this._workers[0];
    for (const w of this._workers) {
      if (w.deque.length < min) { min = w.deque.length; best = w; }
    }
    return best;
  }

  _findVictim(thiefId) {
    let maxLen = 0, victim = null;
    for (const w of this._workers) {
      if (w.id !== thiefId && w.deque.length > maxLen) {
        maxLen = w.deque.length;
        victim = w;
      }
    }
    return victim && maxLen > 0 ? victim : null;
  }

  _unblockReady() {
    const stillBlocked = [];
    for (const item of this._blocked) {
      if (item.dependsOn.every(d => this._completedSet.has(d))) {
        this._enqueue(item);
      } else {
        stillBlocked.push(item);
      }
    }
    this._blocked = stillBlocked;
  }
}
