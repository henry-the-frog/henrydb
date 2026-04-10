// gossip.js — SWIM-style Gossip Protocol for Cluster Membership
//
// Detects failed nodes and disseminates membership changes through gossip.
// Each tick: ping a random member, if no response → indirect ping → suspect → dead.
//
// Properties:
// - O(1) message load per member per tick
// - False positive rate configurable via suspicion timeout
// - Eventually consistent membership view
//
// Based on: Das et al., "SWIM: Scalable Weakly-consistent Infection-style
// Process Group Membership Protocol" (2002)

const MEMBER_STATE = { ALIVE: 'alive', SUSPECT: 'suspect', DEAD: 'dead' };

/**
 * GossipNode — a member of the gossip cluster.
 */
export class GossipNode {
  constructor(id) {
    this.id = id;
    this.members = new Map();  // id → {state, incarnation, suspectTimer}
    this.incarnation = 0;      // Monotonically increasing, refutes suspicion
    this._alive = true;
    this._inbox = [];          // Pending messages
    this._updates = [];        // Piggyback dissemination buffer
    this.stats = { pings: 0, acks: 0, indirectPings: 0, suspicions: 0, deaths: 0 };
  }

  get alive() { return this._alive; }
  crash() { this._alive = false; }
  recover() { this._alive = true; this.incarnation++; }

  /** Join the cluster by learning about other members */
  join(memberIds) {
    for (const id of memberIds) {
      if (id === this.id) continue;
      this.members.set(id, { state: MEMBER_STATE.ALIVE, incarnation: 0, suspectTimer: 0 });
    }
  }

  /** Get all alive members */
  getAliveMembers() {
    const alive = [this.id];
    for (const [id, info] of this.members) {
      if (info.state === MEMBER_STATE.ALIVE) alive.push(id);
    }
    return alive;
  }

  /**
   * Run one gossip tick:
   * 1. Pick a random member and ping it
   * 2. Process any incoming messages
   * 3. Update suspicion timers
   */
  tick(cluster) {
    if (!this._alive) return;
    
    // Process inbox
    for (const msg of this._inbox) this._handleMessage(msg, cluster);
    this._inbox = [];
    
    // Pick a random member to ping
    const candidates = [...this.members.entries()]
      .filter(([_, info]) => info.state !== MEMBER_STATE.DEAD);
    
    if (candidates.length === 0) return;
    
    const [targetId] = candidates[Math.floor(Math.random() * candidates.length)];
    this._ping(targetId, cluster);
    
    // Update suspicion timers
    for (const [id, info] of this.members) {
      if (info.state === MEMBER_STATE.SUSPECT) {
        info.suspectTimer++;
        if (info.suspectTimer > 5) { // Suspicion timeout
          info.state = MEMBER_STATE.DEAD;
          this.stats.deaths++;
          this._updates.push({ type: 'dead', id, incarnation: info.incarnation });
        }
      }
    }
  }

  _ping(targetId, cluster) {
    this.stats.pings++;
    const target = cluster.getNode(targetId);
    
    if (!target || !target.alive) {
      // No response → try indirect ping through other members
      this._indirectPing(targetId, cluster);
      return;
    }
    
    // Send ping with piggybacked updates
    target._inbox.push({
      type: 'ping',
      from: this.id,
      updates: this._getUpdates(),
    });
    
    // Process immediate ack (synchronous simulation)
    if (target.alive) {
      this.stats.acks++;
      this._applyUpdates(target._getUpdates());
      const info = this.members.get(targetId);
      if (info && info.state === MEMBER_STATE.SUSPECT) {
        info.state = MEMBER_STATE.ALIVE;
        info.suspectTimer = 0;
      }
    }
  }

  _indirectPing(targetId, cluster) {
    this.stats.indirectPings++;
    // Ask K random members to ping the target
    const helpers = [...this.members.keys()]
      .filter(id => id !== targetId && this.members.get(id).state === MEMBER_STATE.ALIVE)
      .sort(() => Math.random() - 0.5)
      .slice(0, 3);
    
    let gotAck = false;
    for (const helperId of helpers) {
      const helper = cluster.getNode(helperId);
      const target = cluster.getNode(targetId);
      if (helper?.alive && target?.alive) {
        gotAck = true;
        break;
      }
    }
    
    if (!gotAck) {
      // Mark as suspect
      const info = this.members.get(targetId);
      if (info && info.state === MEMBER_STATE.ALIVE) {
        info.state = MEMBER_STATE.SUSPECT;
        info.suspectTimer = 0;
        this.stats.suspicions++;
        this._updates.push({ type: 'suspect', id: targetId, incarnation: info.incarnation });
      }
    }
  }

  _handleMessage(msg, cluster) {
    if (msg.type === 'ping') {
      // Apply piggybacked updates
      this._applyUpdates(msg.updates);
    }
  }

  _applyUpdates(updates) {
    for (const update of updates) {
      if (update.id === this.id) {
        // Someone suspects/kills us — refute with higher incarnation
        if (update.type === 'suspect' || update.type === 'dead') {
          this.incarnation++;
          this._updates.push({ type: 'alive', id: this.id, incarnation: this.incarnation });
        }
        continue;
      }
      
      const info = this.members.get(update.id);
      if (!info) continue;
      
      if (update.type === 'alive' && update.incarnation > info.incarnation) {
        info.state = MEMBER_STATE.ALIVE;
        info.incarnation = update.incarnation;
        info.suspectTimer = 0;
      } else if (update.type === 'suspect' && update.incarnation >= info.incarnation) {
        if (info.state === MEMBER_STATE.ALIVE) {
          info.state = MEMBER_STATE.SUSPECT;
          info.suspectTimer = 0;
        }
      } else if (update.type === 'dead') {
        info.state = MEMBER_STATE.DEAD;
      }
    }
  }

  _getUpdates() {
    const updates = [...this._updates];
    // Add self-alive announcement
    updates.push({ type: 'alive', id: this.id, incarnation: this.incarnation });
    // Limit piggybacked updates
    return updates.slice(-10);
  }
}

/**
 * GossipCluster — simulated gossip cluster.
 */
export class GossipCluster {
  constructor(size) {
    this.nodes = new Map();
    const ids = Array.from({ length: size }, (_, i) => `node-${i}`);
    
    for (const id of ids) {
      const node = new GossipNode(id);
      node.join(ids);
      this.nodes.set(id, node);
    }
  }

  getNode(id) { return this.nodes.get(id); }

  /** Run one gossip round for all nodes */
  tick() {
    for (const node of this.nodes.values()) {
      node.tick(this);
    }
  }

  /** Run multiple ticks */
  run(ticks) {
    for (let i = 0; i < ticks; i++) this.tick();
  }

  /** Get cluster membership view from a specific node */
  getView(nodeId) {
    return this.getNode(nodeId)?.getAliveMembers() || [];
  }
}

export { MEMBER_STATE };
