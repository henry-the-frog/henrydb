// showcase.test.js — Full feature showcase for Monkey → RISC-V compilation
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RiscVCodeGen } from './monkey-codegen.js';
import { inferTypes } from './type-infer.js';
import { analyzeFreeVars } from './closure-analysis.js';
import { peepholeOptimize } from './riscv-peephole.js';
import { Assembler } from './assembler.js';
import { CPU } from './cpu.js';
import { Lexer } from '/Users/henry/repos/monkey-lang/src/lexer.js';
import { Parser } from '/Users/henry/repos/monkey-lang/src/parser.js';

function runFull(input, { useRegisters = false, optimize = false } = {}) {
  const p = new Parser(new Lexer(input));
  const prog = p.parseProgram();
  if (p.errors.length > 0) throw new Error(p.errors.join('\n'));
  const typeInfo = inferTypes(prog);
  const closureInfo = analyzeFreeVars(prog);
  const cg = new RiscVCodeGen({ useRegisters });
  let asm = cg.compile(prog, typeInfo, closureInfo);
  if (optimize) asm = peepholeOptimize(asm).optimized;
  const assembler = new Assembler();
  const result = assembler.assemble(asm);
  if (result.errors.length > 0) throw new Error(`Asm: ${result.errors.map(e=>e.message).join(', ')}\n${asm}`);
  const cpu = new CPU();
  cpu.loadProgram(result.words);
  cpu.regs.set(2, 0x100000 - 4);
  cpu.run(2000000);
  return { output: cpu.output.join(''), cycles: cpu.cycles, words: result.words.length };
}

function run(input) {
  return runFull(input).output;
}

describe('Showcase: Feature-Complete Demo', () => {
  it('FizzBuzz 1..20', () => {
    const { output } = runFull(`
      let fizzbuzz = fn(n) {
        if (n % 15 == 0) { puts("FizzBuzz") }
        if (n % 15 != 0) {
          if (n % 3 == 0) { puts("Fizz") }
          if (n % 3 != 0) {
            if (n % 5 == 0) { puts("Buzz") }
            if (n % 5 != 0) { puts(n) }
          }
        }
      }
      let i = 1
      while (i <= 20) {
        fizzbuzz(i)
        set i = i + 1
      }
    `);
    assert.equal(output, '12Fizz4BuzzFizz78FizzBuzz11Fizz1314FizzBuzz1617Fizz19Buzz');
  });

  it('Sieve of Eratosthenes', () => {
    const { output } = runFull(`
      let is_prime = fn(n) {
        if (n < 2) { return 0 }
        let i = 2
        while (i * i <= n) {
          if (n % i == 0) { return 0 }
          set i = i + 1
        }
        return 1
      }
      let primes = []
      let n = 2
      while (n <= 50) {
        if (is_prime(n) == 1) {
          set primes = push(primes, n)
        }
        set n = n + 1
      }
      puts("Primes: ")
      for (p in primes) {
        puts(p)
        puts(" ")
      }
    `);
    assert.ok(output.includes('Primes: '));
    assert.ok(output.includes('2 '));
    assert.ok(output.includes('47 '));
  });

  it('String builder pattern', () => {
    const { output } = runFull(`
      let repeat = fn(s, n) {
        let result = ""
        let i = 0
        while (i < n) {
          set result = result + s
          set i = i + 1
        }
        return result
      }
      puts(repeat("ha", 3))
    `);
    assert.equal(output, 'hahaha');
  });

  it('Tower of Hanoi counter', () => {
    const { output } = runFull(`
      let hanoi_count = fn(n) {
        if (n == 0) { return 0 }
        return 1 + hanoi_count(n - 1) * 2
      }
      puts("Moves for 1: "); puts(hanoi_count(1))
      puts(" Moves for 5: "); puts(hanoi_count(5))
      puts(" Moves for 10: "); puts(hanoi_count(10))
    `);
    assert.ok(output.includes('Moves for 1: 1'));
    assert.ok(output.includes('Moves for 5: 31'));
    assert.ok(output.includes('Moves for 10: 1023'));
  });

  it('Binary search', () => {
    const { output } = runFull(`
      let binary_search = fn(arr, target, lo, hi) {
        if (lo > hi) { return -1 }
        let mid = (lo + hi) / 2
        if (arr[mid] == target) { return mid }
        if (arr[mid] < target) {
          return binary_search(arr, target, mid + 1, hi)
        }
        return binary_search(arr, target, lo, mid - 1)
      }
      let data = [2, 5, 8, 12, 16, 23, 38, 56, 72, 91]
      puts(binary_search(data, 23, 0, 9))
    `);
    assert.equal(output, '5');
  });

  it('Matrix-like computation', () => {
    const { output } = runFull(`
      let dot = fn(a, b, n) {
        let sum = 0
        let i = 0
        while (i < n) {
          set sum = sum + a[i] * b[i]
          set i = i + 1
        }
        return sum
      }
      let v1 = [1, 0, 0]
      let v2 = [0, 1, 0]
      let v3 = [1, 1, 1]
      puts("v1·v2="); puts(dot(v1, v2, 3))
      puts(" v1·v3="); puts(dot(v1, v3, 3))
      puts(" v3·v3="); puts(dot(v3, v3, 3))
    `);
    assert.ok(output.includes('v1·v2=0'));
    assert.ok(output.includes('v1·v3=1'));
    assert.ok(output.includes('v3·v3=3'));
  });

  it('Complete pipeline: all features with optimization', () => {
    const code = `
      let greet = fn(name) {
        puts("Hello, " + name + "!")
      }
      let fib = fn(n) {
        if (n <= 1) { return n }
        return fib(n - 1) + fib(n - 2)
      }
      let names = ["Alice", "Bob", "Charlie"]
      for (name in names) {
        greet(name)
      }
      puts(" fib(8)=")
      puts(fib(8))
      let squares = []
      let i = 0
      while (i < 5) {
        set squares = push(squares, i * i)
        set i = i + 1
      }
      puts(" squares=")
      for (s in squares) {
        puts(s)
        puts(" ")
      }
    `;

    // Run without optimization
    const base = runFull(code);
    assert.ok(base.output.includes('Hello, Alice!'));
    assert.ok(base.output.includes('Hello, Bob!'));
    assert.ok(base.output.includes('Hello, Charlie!'));
    assert.ok(base.output.includes('fib(8)=21'));
    assert.ok(base.output.includes('squares='));
    assert.ok(base.output.includes('0 1 4 9 16'));

    // Run with register allocation + peephole
    const opt = runFull(code, { useRegisters: true, optimize: true });
    assert.ok(opt.output.includes('Hello, Alice!'));
    assert.ok(opt.output.includes('fib(8)=21'));

    console.log(`  Showcase: ${base.words} instructions, ${base.cycles} cycles (base)`);
    console.log(`  Showcase: ${opt.words} instructions, ${opt.cycles} cycles (optimized)`);
    console.log(`  Savings: ${base.cycles - opt.cycles} cycles (${((base.cycles - opt.cycles)/base.cycles*100).toFixed(1)}%)`);
  });
});

describe('Showcase: Higher-Order Functions', () => {
  it('reduce/fold pattern', () => {
    const result = run(`
      let reduce = fn(arr, init, f) {
        let acc = init
        let i = 0
        while (i < len(arr)) {
          set acc = f(acc, arr[i])
          set i = i + 1
        }
        return acc
      }
      let sum = fn(a, b) { a + b }
      puts(reduce([1, 2, 3, 4, 5], 0, sum))
    `);
    assert.equal(result, '15');
  });

  it('apply_n (repeated application)', () => {
    const result = run(`
      let apply_n = fn(f, n, x) {
        let result = x
        let i = 0
        while (i < n) {
          set result = f(result)
          set i = i + 1
        }
        return result
      }
      let double = fn(x) { x * 2 }
      puts(apply_n(double, 5, 1))
    `);
    assert.equal(result, '32');
  });

  it('function composition chain', () => {
    const result = run(`
      let compose = fn(f, g, x) { f(g(x)) }
      let add1 = fn(x) { x + 1 }
      let mul3 = fn(x) { x * 3 }
      puts(compose(mul3, add1, 4))
    `);
    assert.equal(result, '15');  // mul3(add1(4)) = mul3(5) = 15
  });

  it('closure factory with higher-order apply', () => {
    const result = run(`
      let make_adder = fn(n) { fn(x) { x + n } }
      let apply = fn(f, x) { f(x) }
      puts(apply(make_adder(100), 42))
    `);
    assert.equal(result, '142');
  });

  it('reduce with multiplication (factorial via fold)', () => {
    const result = run(`
      let reduce = fn(arr, init, f) {
        let acc = init
        let i = 0
        while (i < len(arr)) {
          set acc = f(acc, arr[i])
          set i = i + 1
        }
        return acc
      }
      let mul = fn(a, b) { a * b }
      puts(reduce([1, 2, 3, 4, 5], 1, mul))
    `);
    assert.equal(result, '120');  // 5! = 120
  });

  it('recursive sum with accumulator (tail-recursive style)', () => {
    // Note: nested recursive closures (helper calls itself) not yet supported
    // Using top-level recursive function instead
    const result = run(`
      let sum_helper = fn(n, i, acc) {
        if (i > n) { return acc }
        return sum_helper(n, i + 1, acc + i)
      }
      let sum_to = fn(n) { sum_helper(n, 1, 0) }
      puts(sum_to(100))
    `);
    assert.equal(result, '5050');
  });
});

describe('Showcase: Functional Programming Patterns', () => {
  it('map with double', () => {
    const result = run(`
      let map = fn(arr, f) {
        let result = []
        let i = 0
        while (i < len(arr)) {
          set result = push(result, f(arr[i]))
          set i = i + 1
        }
        return result
      }
      let double = fn(x) { x * 2 }
      let arr = map([1, 2, 3], double)
      puts(arr[0])
      puts(arr[1])
      puts(arr[2])
    `);
    assert.equal(result, '246');
  });

  it('filter with predicate', () => {
    const { output } = runFull(`
      let filter = fn(arr, pred) {
        let result = []
        let i = 0
        while (i < len(arr)) {
          if (pred(arr[i])) {
            set result = push(result, arr[i])
          }
          set i = i + 1
        }
        return result
      }
      let is_even = fn(x) { x % 2 == 0 }
      let arr = filter([1, 2, 3, 4, 5, 6], is_even)
      puts(len(arr))
    `);
    assert.equal(output, '3');
  });

  it('map + filter pipeline', () => {
    const { output } = runFull(`
      let map = fn(arr, f) {
        let result = []
        let i = 0
        while (i < len(arr)) {
          set result = push(result, f(arr[i]))
          set i = i + 1
        }
        return result
      }
      let filter = fn(arr, pred) {
        let result = []
        let i = 0
        while (i < len(arr)) {
          if (pred(arr[i])) { set result = push(result, arr[i]) }
          set i = i + 1
        }
        return result
      }
      let square = fn(x) { x * x }
      let gt10 = fn(x) { x > 10 }
      let result = filter(map([1, 2, 3, 4, 5], square), gt10)
      puts(len(result))
      puts(result[0])
      puts(result[1])
    `);
    assert.equal(output, '21625');  // len=2, 16, 25
  });

  it('reduce with max', () => {
    const result = run(`
      let reduce = fn(arr, init, f) {
        let acc = init
        let i = 0
        while (i < len(arr)) {
          set acc = f(acc, arr[i])
          set i = i + 1
        }
        return acc
      }
      let max = fn(a, b) { if (a > b) { return a }; return b }
      puts(reduce([3, 7, 2, 9, 1], 0, max))
    `);
    assert.equal(result, '9');
  });

  it('closure-based counter', () => {
    const result = run(`
      let make_counter = fn(start) {
        fn(step) { start + step }
      }
      let from10 = make_counter(10)
      puts(from10(0))
      puts(from10(5))
      puts(from10(90))
    `);
    assert.equal(result, '1015100');  // 10, 15, 100
  });

  it('compose two closures', () => {
    const result = run(`
      let make_adder = fn(n) { fn(x) { x + n } }
      let make_mul = fn(n) { fn(x) { x * n } }
      let compose = fn(f, g, x) { f(g(x)) }
      let add5 = make_adder(5)
      let mul3 = make_mul(3)
      puts(compose(add5, mul3, 4))
    `);
    assert.equal(result, '17');  // mul3(4) = 12, add5(12) = 17
  });
});

describe('Showcase: Advanced Algorithms', () => {
  it('Euclidean GCD', () => {
    const result = run(`
      let gcd = fn(a, b) {
        if (b == 0) { return a }
        return gcd(b, a % b)
      }
      puts(gcd(48, 18))
    `);
    assert.equal(result, '6');
  });

  it('GCD multiple values', () => {
    const { output } = runFull(`
      let gcd = fn(a, b) { if (b == 0) { return a }; return gcd(b, a % b) }
      puts(gcd(100, 75))
      puts(gcd(17, 13))
      puts(gcd(1071, 462))
    `);
    assert.equal(output, '25121');  // 25, 1, 21
  });

  it('cons/car/cdr linked list', () => {
    const { output } = runFull(`
      let cons = fn(a, b) { [a, b] }
      let car = fn(pair) { pair[0] }
      let cdr = fn(pair) { pair[1] }
      let list = cons(1, cons(2, cons(3, cons(4, []))))
      puts(car(list))
      puts(car(cdr(list)))
      puts(car(cdr(cdr(list))))
      puts(car(cdr(cdr(cdr(list)))))
    `);
    assert.equal(output, '1234');
  });

  it('power function (recursive)', () => {
    const result = run(`
      let power = fn(base, exp) {
        if (exp == 0) { return 1 }
        return base * power(base, exp - 1)
      }
      puts(power(2, 10))
    `);
    assert.equal(result, '1024');
  });

  it('abs function', () => {
    const result = run(`
      let abs = fn(x) { if (x < 0) { return 0 - x }; return x }
      puts(abs(42))
      puts(abs(0 - 17))
    `);
    assert.equal(result, '4217');
  });

  it('fibonacci iterative', () => {
    const result = run(`
      let fib_iter = fn(n) {
        let a = 0
        let b = 1
        let i = 0
        while (i < n) {
          let tmp = a + b
          set a = b
          set b = tmp
          set i = i + 1
        }
        return a
      }
      puts(fib_iter(10))
      puts(fib_iter(20))
    `);
    assert.equal(result, '556765');  // 55, 6765
  });

  it('is_prime function', () => {
    const result = run(`
      let is_prime = fn(n) {
        if (n < 2) { return 0 }
        let i = 2
        while (i * i <= n) {
          if (n % i == 0) { return 0 }
          set i = i + 1
        }
        return 1
      }
      puts(is_prime(2))
      puts(is_prime(17))
      puts(is_prime(18))
      puts(is_prime(97))
    `);
    assert.equal(result, '1101');  // 1, 1, 0, 1
  });

  it('count primes up to 50', () => {
    const result = run(`
      let is_prime = fn(n) {
        if (n < 2) { return 0 }
        let i = 2
        while (i * i <= n) {
          if (n % i == 0) { return 0 }
          set i = i + 1
        }
        return 1
      }
      let count = 0
      let n = 2
      while (n <= 50) {
        if (is_prime(n)) { set count = count + 1 }
        set n = n + 1
      }
      puts(count)
    `);
    assert.equal(result, '15');  // 15 primes ≤ 50
  });
});

describe('Showcase: Meta — Interpreter on RISC-V', () => {
  it('expression tree evaluator (add)', () => {
    const result = run(`
      let eval_expr = fn(expr) {
        if (len(expr) == 1) { return expr[0] }
        let op = expr[0]
        let left = eval_expr(expr[1])
        let right = eval_expr(expr[2])
        if (op == 1) { return left + right }
        if (op == 2) { return left - right }
        if (op == 3) { return left * right }
        return 0
      }
      puts(eval_expr([1, [10], [20]]))
    `);
    assert.equal(result, '30');
  });

  it('expression tree evaluator (complex)', () => {
    const result = run(`
      let eval_expr = fn(expr) {
        if (len(expr) == 1) { return expr[0] }
        let op = expr[0]
        let left = eval_expr(expr[1])
        let right = eval_expr(expr[2])
        if (op == 1) { return left + right }
        if (op == 2) { return left - right }
        if (op == 3) { return left * right }
        return 0
      }
      puts(eval_expr([3, [1, [2], [3]], [1, [4], [5]]]))
    `);
    assert.equal(result, '45');  // (2+3) * (4+5) = 45
  });
});

describe('Showcase: Algorithms', () => {
  it('binary search finds element', () => {
    const result = run(`
      let binary_search = fn(arr, target) {
        let low = 0
        let high = len(arr) - 1
        while (low <= high) {
          let mid = (low + high) / 2
          if (arr[mid] == target) { return mid }
          if (arr[mid] < target) { set low = mid + 1 } else { set high = mid - 1 }
        }
        return -1
      }
      puts(binary_search([1, 3, 5, 7, 9, 11, 13, 15], 7))
    `);
    assert.equal(result, '3');
  });

  it('binary search not found', () => {
    const result = run(`
      let binary_search = fn(arr, target) {
        let low = 0
        let high = len(arr) - 1
        while (low <= high) {
          let mid = (low + high) / 2
          if (arr[mid] == target) { return mid }
          if (arr[mid] < target) { set low = mid + 1 } else { set high = mid - 1 }
        }
        return -1
      }
      puts(binary_search([1, 3, 5, 7, 9], 4))
    `);
    assert.equal(result, '-1');
  });
});
