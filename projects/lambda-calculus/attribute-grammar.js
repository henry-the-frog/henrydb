/**
 * Attribute Grammars: Synthesized and inherited attributes
 * 
 * Compute properties on ASTs by propagating attributes:
 * - Synthesized: computed bottom-up (e.g., type, value)
 * - Inherited: computed top-down (e.g., environment, expected type)
 */

class AGNode {
  constructor(tag, children = [], attrs = {}) {
    this.tag = tag;
    this.children = children;
    this.synth = {}; // Synthesized attributes
    this.inh = {};   // Inherited attributes
    Object.assign(this, attrs);
  }
}

// Define an attribute grammar
class AttributeGrammar {
  constructor() { this.synthRules = new Map(); this.inhRules = new Map(); }
  
  addSynthRule(nodeTag, attrName, computeFn) {
    const key = `${nodeTag}.${attrName}`;
    this.synthRules.set(key, computeFn);
  }
  
  addInhRule(nodeTag, childIdx, attrName, computeFn) {
    const key = `${nodeTag}.${childIdx}.${attrName}`;
    this.inhRules.set(key, computeFn);
  }
  
  evaluate(node, parentInh = {}) {
    node.inh = { ...parentInh };
    
    // First: propagate inherited attributes to children
    for (let i = 0; i < node.children.length; i++) {
      const childInh = {};
      for (const [key, fn] of this.inhRules) {
        const [tag, idx, attr] = key.split('.');
        if (tag === node.tag && parseInt(idx) === i) {
          childInh[attr] = fn(node);
        }
      }
      this.evaluate(node.children[i], { ...node.inh, ...childInh });
    }
    
    // Then: compute synthesized attributes bottom-up
    for (const [key, fn] of this.synthRules) {
      const [tag, attr] = key.split('.');
      if (tag === node.tag) {
        node.synth[attr] = fn(node);
      }
    }
    
    return node;
  }
}

// Example: expression evaluator AG
function makeEvalAG() {
  const ag = new AttributeGrammar();
  ag.addSynthRule('Num', 'value', n => n.n);
  ag.addSynthRule('Add', 'value', n => n.children[0].synth.value + n.children[1].synth.value);
  ag.addSynthRule('Mul', 'value', n => n.children[0].synth.value * n.children[1].synth.value);
  ag.addSynthRule('Neg', 'value', n => -n.children[0].synth.value);
  return ag;
}

// Example: depth AG
function makeDepthAG() {
  const ag = new AttributeGrammar();
  ag.addSynthRule('Leaf', 'depth', () => 0);
  ag.addSynthRule('Node', 'depth', n => 1 + Math.max(...n.children.map(c => c.synth.depth)));
  return ag;
}

export { AGNode, AttributeGrammar, makeEvalAG, makeDepthAG };
