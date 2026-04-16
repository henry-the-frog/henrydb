/**
 * Module #185: Coherence Checking — No ambiguous type class instances
 */
class Instance { constructor(cls, type, prio = 0) { this.cls = cls; this.type = type; this.prio = prio; } }

class CoherenceChecker {
  constructor() { this.instances = []; }
  add(inst) { this.instances.push(inst); }
  
  check() {
    const errors = [];
    for (let i = 0; i < this.instances.length; i++)
      for (let j = i + 1; j < this.instances.length; j++)
        if (this.instances[i].cls === this.instances[j].cls && this.overlap(this.instances[i].type, this.instances[j].type))
          errors.push({ inst1: this.instances[i], inst2: this.instances[j], reason: `Overlapping ${this.instances[i].cls} instances for ${this.instances[i].type} and ${this.instances[j].type}` });
    return errors;
  }
  
  isCoherent() { return this.check().length === 0; }
  
  overlap(t1, t2) {
    if (t1 === t2) return true;
    if (t1[0] === t1[0].toLowerCase() || t2[0] === t2[0].toLowerCase()) return true; // Type var
    return false;
  }
  
  resolve(cls, type) {
    const matching = this.instances.filter(i => i.cls === cls && (i.type === type || i.type[0] === i.type[0].toLowerCase()));
    if (matching.length === 0) return null;
    if (matching.length > 1) return matching.sort((a, b) => b.prio - a.prio)[0]; // Priority
    return matching[0];
  }
}

export { Instance, CoherenceChecker };
