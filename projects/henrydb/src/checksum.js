// checksum.js — Multiple checksum algorithms for data integrity
export function adler32(buf) {
  let a = 1, b = 0;
  for (const byte of buf) { a = (a + byte) % 65521; b = (b + a) % 65521; }
  return (b << 16) | a;
}

export function fletcher16(buf) {
  let sum1 = 0, sum2 = 0;
  for (const byte of buf) { sum1 = (sum1 + byte) % 255; sum2 = (sum2 + sum1) % 255; }
  return (sum2 << 8) | sum1;
}

export function xorChecksum(buf) {
  let result = 0;
  for (const byte of buf) result ^= byte;
  return result;
}
