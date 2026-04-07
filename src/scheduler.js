// scheduler.js — DAG scheduler + worker pool for HenryDB
// Topological sort for dependency-ordered execution.
// Worker pool for simulating parallel query execution.

/**
 * Directed Acyclic Graph with topological sort.
 * Used for query plan execution ordering.
 */
export class DAG {
  constructor() {
    this._nodes = new Map(); // id → { data, deps: Set<id> }
    this._dependents = new Map(); // id → Set<id> that depend on this node
  }

  /**
   * Add a task/node with its dependencies.
   */
  addNode(id, data = null, deps = []) {
    this._nodes.set(id, { data, deps: new Set(deps) });
    if (!this._dependents.has(id)) this._dependents.set(id, new Set());
    for (const dep of deps) {
      if (!this._dependents.has(dep)) this._dependents.set(dep, new Set());
      this._dependents.get(dep).add(id);
    }
  }

  /**
   * Topological sort using Kahn's algorithm.
   * Returns nodes in dependency-first order.
   * Throws if cycle detected.
   */
  topologicalSort() {
    const inDegree = new Map();
    for (const [id, node] of this._nodes) {
      inDegree.set(id, node.deps.size);
    }

    const queue = [];
    for (const [id, degree] of inDegree) {
      if (degree === 0) queue.push(id);
    }

    const sorted = [];
    while (queue.length > 0) {
      const id = queue.shift();
      sorted.push(id);

      for (const dependent of (this._dependents.get(id) || [])) {
        const newDegree = inDegree.get(dependent) - 1;
        inDegree.set(dependent, newDegree);
        if (newDegree === 0) queue.push(dependent);
      }
    }

    if (sorted.length !== this._nodes.size) {
      throw new Error('Cycle detected in DAG');
    }

    return sorted.map(id => ({ id, data: this._nodes.get(id).data }));
  }

  /**
   * Get nodes that can execute in parallel (no dependencies between them).
   * Returns array of "levels" where each level can execute concurrently.
   */
  parallelLevels() {
    const inDegree = new Map();
    for (const [id, node] of this._nodes) {
      inDegree.set(id, node.deps.size);
    }

    const levels = [];
    let remaining = new Set(this._nodes.keys());

    while (remaining.size > 0) {
      const level = [];
      for (const id of remaining) {
        if (inDegree.get(id) === 0) level.push(id);
      }

      if (level.length === 0) throw new Error('Cycle detected in DAG');

      for (const id of level) {
        remaining.delete(id);
        for (const dependent of (this._dependents.get(id) || [])) {
          inDegree.set(dependent, inDegree.get(dependent) - 1);
        }
      }

      levels.push(level.map(id => ({ id, data: this._nodes.get(id).data })));
    }

    return levels;
  }

  get size() { return this._nodes.size; }
}

/**
 * Worker Pool simulator.
 * Simulates parallel task execution with configurable concurrency.
 */
export class WorkerPool {
  constructor(concurrency = 4) {
    this._concurrency = concurrency;
    this._taskQueue = [];
    this._results = new Map();
    this._completed = 0;
  }

  /**
   * Submit a task (function that returns a result).
   */
  submit(taskId, taskFn) {
    this._taskQueue.push({ id: taskId, fn: taskFn });
  }

  /**
   * Execute all tasks respecting concurrency limit.
   * Tasks are executed synchronously (simulating parallel with batching).
   */
  executeAll() {
    while (this._taskQueue.length > 0) {
      // Take up to `concurrency` tasks
      const batch = this._taskQueue.splice(0, this._concurrency);
      
      for (const task of batch) {
        try {
          const result = task.fn();
          this._results.set(task.id, { status: 'ok', result });
        } catch (e) {
          this._results.set(task.id, { status: 'error', error: e.message });
        }
        this._completed++;
      }
    }
    return this._results;
  }

  getResult(taskId) {
    return this._results.get(taskId);
  }

  get completedCount() { return this._completed; }
}
