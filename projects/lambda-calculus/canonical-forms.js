/**
 * 🎉 Module #190: Canonical Forms Lemma — Values have canonical shapes
 */
function canonicalForm(value, type) {
  switch(type) {
    case 'Int': return typeof value === 'number' && Number.isInteger(value) ? { canonical: true, form: 'integer' } : { canonical: false };
    case 'Bool': return typeof value === 'boolean' ? { canonical: true, form: 'boolean' } : { canonical: false };
    case 'String': return typeof value === 'string' ? { canonical: true, form: 'string' } : { canonical: false };
    case 'Unit': return value === null ? { canonical: true, form: 'unit' } : { canonical: false };
    default:
      if (type.startsWith('Fun')) return typeof value === 'function' ? { canonical: true, form: 'lambda' } : { canonical: false };
      if (type.startsWith('List')) return Array.isArray(value) ? { canonical: true, form: value.length === 0 ? 'nil' : 'cons' } : { canonical: false };
      if (type.startsWith('Pair')) return Array.isArray(value) && value.length === 2 ? { canonical: true, form: 'pair' } : { canonical: false };
      return { canonical: false, reason: 'unknown type' };
  }
}

function verifyCanonicalForms(examples) {
  return examples.map(({ value, type }) => ({ ...canonicalForm(value, type), value, type }));
}

export { canonicalForm, verifyCanonicalForms };
