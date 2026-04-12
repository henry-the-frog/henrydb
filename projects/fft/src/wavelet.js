// wavelet.js — Discrete Wavelet Transform (DWT)
// Haar and Daubechies wavelets, multiresolution analysis, denoising

// ===== Wavelet Filters =====

// Haar wavelet (db1) — simplest possible
export const HAAR = {
  name: 'haar',
  dec_lo: [1 / Math.SQRT2, 1 / Math.SQRT2],           // decomposition lowpass
  dec_hi: [-1 / Math.SQRT2, 1 / Math.SQRT2],           // decomposition highpass
  rec_lo: [1 / Math.SQRT2, 1 / Math.SQRT2],            // reconstruction lowpass
  rec_hi: [1 / Math.SQRT2, -1 / Math.SQRT2],           // reconstruction highpass
};

// Daubechies-4 (db2) — 4 coefficients
const h0 = (1 + Math.sqrt(3)) / (4 * Math.SQRT2);
const h1 = (3 + Math.sqrt(3)) / (4 * Math.SQRT2);
const h2 = (3 - Math.sqrt(3)) / (4 * Math.SQRT2);
const h3 = (1 - Math.sqrt(3)) / (4 * Math.SQRT2);

export const DB2 = {
  name: 'db2',
  dec_lo: [h0, h1, h2, h3],
  dec_hi: [h3, -h2, h1, -h0],
  rec_lo: [h3, h2, h1, h0],
  rec_hi: [-h0, h1, -h2, h3],
};

// Daubechies-6 (db3) — 6 coefficients
const sq10 = Math.sqrt(10);
const sq52 = Math.sqrt(5 + 2 * sq10);
const d3_norm = 1 / (16 * Math.SQRT2);
const d3 = [
  d3_norm * (1 + Math.sqrt(10) + sq52),
  d3_norm * (5 + Math.sqrt(10) + 3 * sq52),
  d3_norm * (10 - 2 * Math.sqrt(10) + 2 * sq52),
  d3_norm * (10 - 2 * Math.sqrt(10) - 2 * sq52),
  d3_norm * (5 + Math.sqrt(10) - 3 * sq52),
  d3_norm * (1 + Math.sqrt(10) - sq52),
];

export const DB3 = {
  name: 'db3',
  dec_lo: d3,
  dec_hi: d3.map((v, i) => (i % 2 === 0 ? 1 : -1) * d3[d3.length - 1 - i]),
  rec_lo: [...d3].reverse(),
  rec_hi: d3.map((v, i) => (i % 2 === 0 ? -1 : 1) * d3[d3.length - 1 - i]).reverse(),
};

// ===== Single-level DWT =====
export function dwtStep(signal, wavelet = HAAR) {
  const N = signal.length;
  const halfN = N / 2;
  const lo = new Array(halfN);
  const hi = new Array(halfN);
  const L = wavelet.dec_lo.length;

  for (let i = 0; i < halfN; i++) {
    let sumLo = 0, sumHi = 0;
    for (let j = 0; j < L; j++) {
      const idx = ((2 * i + j) % N + N) % N;
      sumLo += signal[idx] * wavelet.dec_lo[j];
      sumHi += signal[idx] * wavelet.dec_hi[j];
    }
    lo[i] = sumLo;
    hi[i] = sumHi;
  }

  return { approx: lo, detail: hi };
}

// ===== Single-level IDWT =====
// For orthogonal wavelets: synthesis = adjoint of analysis
export function idwtStep(approx, detail, wavelet = HAAR) {
  const halfN = approx.length;
  const N = halfN * 2;
  const L = wavelet.dec_lo.length;
  const output = new Array(N).fill(0);

  // Transpose of the analysis operation:
  // Forward: lo[i] = sum_j signal[(2i+j)%N] * dec_lo[j]
  // Adjoint: signal[(2i+j)%N] += lo[i] * dec_lo[j] + hi[i] * dec_hi[j]
  for (let i = 0; i < halfN; i++) {
    for (let j = 0; j < L; j++) {
      const idx = ((2 * i + j) % N + N) % N;
      output[idx] += approx[i] * wavelet.dec_lo[j] + detail[i] * wavelet.dec_hi[j];
    }
  }

  return output;
}

// ===== Multi-level DWT =====
export function dwt(signal, wavelet = HAAR, maxLevel = null) {
  const levels = maxLevel || Math.floor(Math.log2(signal.length));
  const details = [];
  let current = [...signal];

  for (let level = 0; level < levels && current.length >= wavelet.dec_lo.length; level++) {
    const { approx, detail } = dwtStep(current, wavelet);
    details.push(detail);
    current = approx;
  }

  return { approx: current, details };
}

// ===== Multi-level IDWT =====
export function idwt(coefficients, wavelet = HAAR) {
  let current = coefficients.approx;

  for (let level = coefficients.details.length - 1; level >= 0; level--) {
    current = idwtStep(current, coefficients.details[level], wavelet);
  }

  return current;
}

// ===== Denoising =====

// Hard thresholding: set coefficients below threshold to 0
export function hardThreshold(value, threshold) {
  return Math.abs(value) >= threshold ? value : 0;
}

// Soft thresholding: shrink toward zero
export function softThreshold(value, threshold) {
  if (Math.abs(value) < threshold) return 0;
  return Math.sign(value) * (Math.abs(value) - threshold);
}

// Universal threshold (VisuShrink): sigma * sqrt(2 * ln(N))
export function universalThreshold(detail, N) {
  // Estimate noise sigma from finest detail coefficients (MAD estimator)
  const sorted = [...detail].map(Math.abs).sort((a, b) => a - b);
  const median = sorted[Math.floor(sorted.length / 2)];
  const sigma = median / 0.6745;
  return sigma * Math.sqrt(2 * Math.log(N));
}

// Denoise signal using wavelet thresholding
export function denoise(signal, wavelet = HAAR, mode = 'soft', levels = null) {
  const coeffs = dwt(signal, wavelet, levels);
  const N = signal.length;

  // Threshold each detail level
  const threshold = universalThreshold(coeffs.details[0], N);
  const threshFn = mode === 'soft' ? softThreshold : hardThreshold;

  const thresholdedDetails = coeffs.details.map(detail =>
    detail.map(v => threshFn(v, threshold))
  );

  return idwt({ approx: coeffs.approx, details: thresholdedDetails }, wavelet);
}

// ===== Multiresolution Analysis =====
export function multiresolution(signal, wavelet = HAAR, levels = null) {
  const coeffs = dwt(signal, wavelet, levels);
  const components = [];

  // Reconstruct each detail level's contribution
  for (let level = 0; level < coeffs.details.length; level++) {
    const zeroDetails = coeffs.details.map((d, i) =>
      i === level ? d : new Array(d.length).fill(0)
    );
    const zeroApprox = new Array(coeffs.approx.length).fill(0);
    const component = idwt({ approx: zeroApprox, details: zeroDetails }, wavelet);
    components.push({ level: level + 1, type: 'detail', data: component });
  }

  // Final approximation
  const zeroAllDetails = coeffs.details.map(d => new Array(d.length).fill(0));
  const approxComponent = idwt({ approx: coeffs.approx, details: zeroAllDetails }, wavelet);
  components.push({ level: coeffs.details.length, type: 'approx', data: approxComponent });

  return components;
}

// ===== Wavelet Energy =====
export function waveletEnergy(coefficients) {
  const approxEnergy = coefficients.approx.reduce((s, v) => s + v * v, 0);
  const detailEnergies = coefficients.details.map(d => d.reduce((s, v) => s + v * v, 0));
  const total = approxEnergy + detailEnergies.reduce((a, b) => a + b, 0);
  return {
    total,
    approx: approxEnergy / total,
    details: detailEnergies.map(e => e / total),
  };
}

// ===== Stationary Wavelet Transform (SWT) — no downsampling =====
export function swtStep(signal, wavelet, level) {
  const N = signal.length;
  const lo = new Array(N);
  const hi = new Array(N);
  const fLen = wavelet.dec_lo.length;
  const stride = Math.pow(2, level);

  for (let i = 0; i < N; i++) {
    let sumLo = 0, sumHi = 0;
    for (let j = 0; j < fLen; j++) {
      const idx = (i + j * stride) % N;
      sumLo += signal[idx] * wavelet.dec_lo[j];
      sumHi += signal[idx] * wavelet.dec_hi[j];
    }
    lo[i] = sumLo;
    hi[i] = sumHi;
  }

  return { approx: lo, detail: hi };
}

// ===== 2D Wavelet Transform (for images) =====
export function dwt2d(matrix, wavelet = HAAR) {
  const rows = matrix.length;
  const cols = matrix[0].length;

  // Transform rows
  const rowTransformed = matrix.map(row => {
    const { approx, detail } = dwtStep(row, wavelet);
    return [...approx, ...detail];
  });

  // Transform columns
  const colTransformed = [];
  for (let i = 0; i < rows; i++) colTransformed.push(new Array(cols));

  for (let j = 0; j < cols; j++) {
    const col = rowTransformed.map(row => row[j]);
    const { approx, detail } = dwtStep(col, wavelet);
    for (let i = 0; i < rows / 2; i++) {
      colTransformed[i][j] = approx[i];
      colTransformed[i + rows / 2][j] = detail[i];
    }
  }

  // Split into quadrants: LL, LH, HL, HH
  const halfR = rows / 2, halfC = cols / 2;
  return {
    LL: colTransformed.slice(0, halfR).map(row => row.slice(0, halfC)),
    LH: colTransformed.slice(halfR).map(row => row.slice(0, halfC)),
    HL: colTransformed.slice(0, halfR).map(row => row.slice(halfC)),
    HH: colTransformed.slice(halfR).map(row => row.slice(halfC)),
  };
}
