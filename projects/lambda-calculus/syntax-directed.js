/**
 * 🎉 Module #175: Syntax-Directed Translation — AST to IR in one pass
 */
class IR { constructor(ops) { this.ops = ops; } toString() { return this.ops.map(o => `  ${o}`).join('\n'); } }

function translate(expr, target = null) {
  const ops = [];
  let temp = 0;
  function fresh() { return `t${temp++}`; }
  
  function emit(expr) {
    switch(expr.tag) {
      case 'Num': { const t = fresh(); ops.push(`${t} = const ${expr.n}`); return t; }
      case 'Var': return expr.name;
      case 'Add': { const l = emit(expr.left), r = emit(expr.right), t = fresh(); ops.push(`${t} = add ${l}, ${r}`); return t; }
      case 'Mul': { const l = emit(expr.left), r = emit(expr.right), t = fresh(); ops.push(`${t} = mul ${l}, ${r}`); return t; }
      case 'Neg': { const v = emit(expr.expr), t = fresh(); ops.push(`${t} = neg ${v}`); return t; }
      case 'Let': { const init = emit(expr.init); ops.push(`${expr.var} = ${init}`); return emit(expr.body); }
      case 'If': {
        const c = emit(expr.cond), thenLabel = `L${temp++}`, elseLabel = `L${temp++}`, endLabel = `L${temp++}`, result = fresh();
        ops.push(`br ${c}, ${thenLabel}, ${elseLabel}`);
        ops.push(`${thenLabel}:`); const tv = emit(expr.then); ops.push(`${result} = ${tv}`); ops.push(`jmp ${endLabel}`);
        ops.push(`${elseLabel}:`); const fv = emit(expr.else); ops.push(`${result} = ${fv}`); ops.push(`jmp ${endLabel}`);
        ops.push(`${endLabel}:`);
        return result;
      }
      case 'Call': { const args = expr.args.map(emit); const t = fresh(); ops.push(`${t} = call ${expr.fn}(${args.join(', ')})`); return t; }
    }
  }
  
  const result = emit(expr);
  if (target) ops.push(`${target} = ${result}`);
  else ops.push(`ret ${result}`);
  return new IR(ops);
}

const Num = n => ({ tag:'Num', n }); const Var = n => ({ tag:'Var', name:n });
const Add = (l,r) => ({ tag:'Add', left:l, right:r }); const Mul = (l,r) => ({ tag:'Mul', left:l, right:r });
const Neg = e => ({ tag:'Neg', expr:e }); const Let = (v,i,b) => ({ tag:'Let', var:v, init:i, body:b });
const If = (c,t,f) => ({ tag:'If', cond:c, then:t, else:f }); const Call = (fn, args) => ({ tag:'Call', fn, args });

export { IR, translate, Num, Var, Add, Mul, Neg, Let, If, Call };
