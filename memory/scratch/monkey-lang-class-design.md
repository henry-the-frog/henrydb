# Monkey-Lang Class Syntax Design

## Design Principles
1. Keep it simple — monkey-lang is not Java
2. Classes are syntactic sugar over closures+hashes (existing mechanism)
3. Minimal keywords — reuse existing syntax where possible
4. No `this` — use explicit `self` parameter

## Proposed Syntax

```monkey
class Animal {
  // Constructor
  init(self, name, sound) {
    self.name = name;
    self.sound = sound;
  }
  
  // Method
  speak(self) {
    self.name + " says " + self.sound
  }
}

class Dog extends Animal {
  init(self, name) {
    super.init(self, name, "woof");
    self.tricks = [];
  }
  
  fetch(self) {
    self.name + " fetches the ball"
  }
  
  learn(self, trick) {
    set self.tricks = push(self.tricks, trick);
  }
}

let rex = Dog("Rex");
rex.speak();        // "Rex says woof"
rex.learn("sit");
rex.fetch();        // "Rex fetches the ball"
```

## Compilation Strategy

### Option A: Compile to Closure Pattern (simplest)
```monkey
// class Animal compiles to:
let Animal = fn(name, sound) {
  let self = {"name": name, "sound": sound};
  self["speak"] = fn() { self["name"] + " says " + self["sound"] };
  self
};
```
- **Pro**: No new runtime support needed
- **Con**: No proper prototype chain, no instanceof

### Option B: Compile to Hash + Method Table
```monkey
// class Animal compiles to:
let Animal__methods = {
  "speak": fn(self) { self.name + " says " + self.sound }
};
let Animal = fn(name, sound) {
  let self = {"__class": "Animal", "__methods": Animal__methods, "name": name, "sound": sound};
  // Proxy method calls through __methods
  self
};
```
- **Pro**: Proper method sharing, supports instanceof
- **Con**: Needs method dispatch mechanism in evaluator/VM

### Option C: New Runtime Object (most powerful)
- Add `CompiledClass` object type
- Methods stored on class, not instance
- Prototype chain for inheritance
- `instanceof` operator
- **Pro**: Full OOP semantics
- **Con**: Significant new runtime complexity

## Recommendation: Option A with Sugar
1. Start with Option A (closure compilation)
2. Add `class` keyword to parser → generates closure AST
3. Add `.` method call syntax (syntactic sugar for hash indexing)
4. No prototype chain needed initially
5. Later: upgrade to Option B or C if needed

## Parser Changes (~100 LOC)
- New token: CLASS
- New AST: ClassStatement { name, superClass?, methods[], init? }
- Parse: `class Name { methods... }` → ClassStatement
- Parse: `extends Name` → optional super class reference

## Compiler Changes (~50 LOC)  
- Compile ClassStatement → closure factory function
- Handle `super.method()` calls
- Method binding to self

## Total Effort: ~200 LOC, 1 day
