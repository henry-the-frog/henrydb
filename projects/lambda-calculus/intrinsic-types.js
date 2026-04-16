/**
 * Intrinsically Typed Syntax: ASTs that are well-typed by construction
 * Only representable terms are well-typed. Ill-typed terms are unrepresentable.
 */

class TyNum { constructor(n) { this.tag='TyNum'; this.n=n; this.type='Int'; } }
class TyBool { constructor(b) { this.tag='TyBool'; this.b=b; this.type='Bool'; } }
class TyAdd { constructor(l,r) { if(l.type!=='Int'||r.type!=='Int') throw new Error('Add: need Int'); this.tag='TyAdd'; this.left=l; this.right=r; this.type='Int'; } }
class TyEq { constructor(l,r) { if(l.type!==r.type) throw new Error('Eq: types differ'); this.tag='TyEq'; this.left=l; this.right=r; this.type='Bool'; } }
class TyIf { constructor(c,t,f) { if(c.type!=='Bool') throw new Error('If: cond not Bool'); if(t.type!==f.type) throw new Error('If: branch types differ'); this.tag='TyIf'; this.cond=c; this.then=t; this.else=f; this.type=t.type; } }
class TyPair { constructor(a,b) { this.tag='TyPair'; this.fst=a; this.snd=b; this.type=`(${a.type},${b.type})`; } }
class TyFst { constructor(p) { if(!p.type.startsWith('(')) throw new Error('Fst: not a pair'); this.tag='TyFst'; this.pair=p; this.type=p.fst.type; } }
class TySnd { constructor(p) { if(!p.type.startsWith('(')) throw new Error('Snd: not a pair'); this.tag='TySnd'; this.pair=p; this.type=p.snd.type; } }

function eval_(e) {
  switch(e.tag) {
    case 'TyNum': return e.n;
    case 'TyBool': return e.b;
    case 'TyAdd': return eval_(e.left) + eval_(e.right);
    case 'TyEq': return eval_(e.left) === eval_(e.right);
    case 'TyIf': return eval_(e.cond) ? eval_(e.then) : eval_(e.else);
    case 'TyPair': return [eval_(e.fst), eval_(e.snd)];
    case 'TyFst': return eval_(e.pair)[0];
    case 'TySnd': return eval_(e.pair)[1];
  }
}

export { TyNum, TyBool, TyAdd, TyEq, TyIf, TyPair, TyFst, TySnd, eval_ };
