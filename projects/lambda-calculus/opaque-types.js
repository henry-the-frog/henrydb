/**
 * Opaque Types: Abstract types with controlled operations
 * Like Rust's newtype or Haskell's newtype with smart constructors.
 */

function createOpaqueType(name, validate = null) {
  const tag = Symbol(name);
  
  return {
    create(value) {
      if (validate && !validate(value)) throw new Error(`Invalid ${name}: ${value}`);
      return { [tag]: true, _value: value, _type: name };
    },
    unwrap(opaque) {
      if (!opaque || !opaque[tag]) throw new Error(`Not a ${name}`);
      return opaque._value;
    },
    is(value) { return value && value[tag] === true; },
    map(f, opaque) { return this.create(f(this.unwrap(opaque))); },
    name,
  };
}

// Examples
const PositiveInt = createOpaqueType('PositiveInt', n => Number.isInteger(n) && n > 0);
const Email = createOpaqueType('Email', s => typeof s === 'string' && s.includes('@'));
const NonEmptyString = createOpaqueType('NonEmptyString', s => typeof s === 'string' && s.length > 0);
const Percentage = createOpaqueType('Percentage', n => typeof n === 'number' && n >= 0 && n <= 100);

export { createOpaqueType, PositiveInt, Email, NonEmptyString, Percentage };
