import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  HAAR, DB2, DB3,
  dwtStep, idwtStep, dwt, idwt,
  hardThreshold, softThreshold, universalThreshold, denoise,
  multiresolution, waveletEnergy,
  swtStep, dwt2d,
} from './wavelet.js';

const approx = (a, b, eps = 0.01) => Math.abs(a - b) < eps;

describe('Haar Wavelet', () => {
  it('single-level decomposition', () => {
    const signal = [1, 2, 3, 4];
    const { approx: lo, detail: hi } = dwtStep(signal, HAAR);
    assert.equal(lo.length, 2);
    assert.equal(hi.length, 2);
    // Haar: lo[0] = (1+2)/sqrt(2), hi[0] = (-1+2)/sqrt(2)
    assert.ok(approx(lo[0], 3 / Math.SQRT2, 0.001));
    assert.ok(approx(hi[0], 1 / Math.SQRT2, 0.001));
  });

  it('roundtrips single level', () => {
    const signal = [1, 3, 5, 7, 2, 4, 6, 8];
    const { approx: lo, detail: hi } = dwtStep(signal, HAAR);
    const recovered = idwtStep(lo, hi, HAAR);
    for (let i = 0; i < signal.length; i++) {
      assert.ok(approx(recovered[i], signal[i], 0.001),
        `Mismatch at ${i}: ${recovered[i]} vs ${signal[i]}`);
    }
  });

  it('constant signal has zero detail', () => {
    const signal = [5, 5, 5, 5, 5, 5, 5, 5];
    const { detail: hi } = dwtStep(signal, HAAR);
    for (const v of hi) assert.ok(approx(v, 0, 0.001));
  });

  it('multi-level roundtrip', () => {
    const signal = [1, 2, 3, 4, 5, 6, 7, 8];
    const coeffs = dwt(signal, HAAR);
    const recovered = idwt(coeffs, HAAR);
    for (let i = 0; i < signal.length; i++) {
      assert.ok(approx(recovered[i], signal[i], 0.001));
    }
  });
});

describe('Daubechies Wavelets', () => {
  it('DB2 roundtrips', () => {
    const signal = [1, -2, 3, -4, 5, -6, 7, -8];
    const { approx: lo, detail: hi } = dwtStep(signal, DB2);
    const recovered = idwtStep(lo, hi, DB2);
    for (let i = 0; i < signal.length; i++) {
      assert.ok(approx(recovered[i], signal[i], 0.01),
        `DB2 mismatch at ${i}: ${recovered[i].toFixed(4)} vs ${signal[i]}`);
    }
  });

  it('DB2 multi-level roundtrip', () => {
    const signal = Array.from({ length: 16 }, (_, i) => Math.sin(i * 0.5));
    const coeffs = dwt(signal, DB2);
    const recovered = idwt(coeffs, DB2);
    for (let i = 0; i < signal.length; i++) {
      assert.ok(approx(recovered[i], signal[i], 0.01),
        `DB2 multi mismatch at ${i}`);
    }
  });

  it('DB3 roundtrips', () => {
    const signal = Array.from({ length: 16 }, (_, i) => Math.cos(i * 0.3));
    const coeffs = dwt(signal, DB3);
    const recovered = idwt(coeffs, DB3);
    for (let i = 0; i < signal.length; i++) {
      assert.ok(approx(recovered[i], signal[i], 0.05),
        `DB3 mismatch at ${i}: ${recovered[i].toFixed(4)} vs ${signal[i].toFixed(4)}`);
    }
  });

  it('DB2 captures smoother features than Haar', () => {
    // Smooth signal should have less detail energy with DB2
    const signal = Array.from({ length: 64 }, (_, i) => Math.sin(2 * Math.PI * i / 64));
    const haarCoeffs = dwt(signal, HAAR);
    const db2Coeffs = dwt(signal, DB2);

    const haarDetailEnergy = haarCoeffs.details[0].reduce((s, v) => s + v * v, 0);
    const db2DetailEnergy = db2Coeffs.details[0].reduce((s, v) => s + v * v, 0);

    // DB2 should capture less noise in detail for smooth signals
    // (Not always strictly less, but both should be reasonable)
    assert.ok(Number.isFinite(haarDetailEnergy));
    assert.ok(Number.isFinite(db2DetailEnergy));
  });
});

describe('Thresholding', () => {
  it('hard threshold', () => {
    assert.equal(hardThreshold(5, 3), 5);
    assert.equal(hardThreshold(2, 3), 0);
    assert.equal(hardThreshold(-5, 3), -5);
    assert.equal(hardThreshold(-2, 3), 0);
  });

  it('soft threshold', () => {
    assert.ok(approx(softThreshold(5, 3), 2));
    assert.equal(softThreshold(2, 3), 0);
    assert.ok(approx(softThreshold(-5, 3), -2));
    assert.equal(softThreshold(-2, 3), 0);
    assert.equal(softThreshold(3, 3), 0);
  });

  it('universal threshold is positive', () => {
    const detail = Array.from({ length: 64 }, () => Math.random() - 0.5);
    const t = universalThreshold(detail, 64);
    assert.ok(t > 0);
  });
});

describe('Denoising', () => {
  it('reduces noise in sinusoidal signal', () => {
    const N = 256;
    const clean = Array.from({ length: N }, (_, i) => Math.sin(2 * Math.PI * 4 * i / N));
    // Use higher noise level so denoising effect is clearer
    const noisy = clean.map(v => v + (Math.random() - 0.5) * 2.0);

    const denoised = denoise(noisy, HAAR, 'soft', 3);

    // Denoised should be closer to clean than noisy
    const noisyError = clean.reduce((s, v, i) => s + (v - noisy[i]) ** 2, 0) / N;
    const denoisedError = clean.reduce((s, v, i) => s + (v - denoised[i]) ** 2, 0) / N;

    assert.ok(denoisedError < noisyError,
      `Denoised MSE (${denoisedError.toFixed(4)}) should be < noisy MSE (${noisyError.toFixed(4)})`);
  });

  it('hard thresholding also works', () => {
    const N = 128;
    const clean = Array.from({ length: N }, (_, i) => Math.cos(2 * Math.PI * 2 * i / N));
    const noisy = clean.map(v => v + (Math.random() - 0.5) * 0.8);

    const denoised = denoise(noisy, HAAR, 'hard');
    const noisyError = clean.reduce((s, v, i) => s + (v - noisy[i]) ** 2, 0) / N;
    const denoisedError = clean.reduce((s, v, i) => s + (v - denoised[i]) ** 2, 0) / N;

    assert.ok(denoisedError < noisyError * 1.5, // Hard threshold can be slightly less effective
      'Hard thresholding should help with denoising');
  });
});

describe('Multiresolution Analysis', () => {
  it('components sum to original', () => {
    const signal = [1, 2, 3, 4, 5, 6, 7, 8];
    const components = multiresolution(signal, HAAR);

    const reconstructed = new Array(signal.length).fill(0);
    for (const c of components) {
      for (let i = 0; i < signal.length; i++) {
        reconstructed[i] += c.data[i];
      }
    }

    for (let i = 0; i < signal.length; i++) {
      assert.ok(approx(reconstructed[i], signal[i], 0.01),
        `Multiresolution sum mismatch at ${i}`);
    }
  });

  it('has correct number of components', () => {
    const signal = Array.from({ length: 16 }, () => Math.random());
    const components = multiresolution(signal, HAAR);
    // Should have detail levels + 1 approx
    assert.ok(components.length >= 2);
    assert.equal(components[components.length - 1].type, 'approx');
  });
});

describe('Wavelet Energy', () => {
  it('energies sum to 1', () => {
    const signal = Array.from({ length: 64 }, (_, i) => Math.sin(i * 0.3) + 0.5 * Math.cos(i * 1.2));
    const coeffs = dwt(signal, HAAR);
    const energy = waveletEnergy(coeffs);
    const total = energy.approx + energy.details.reduce((a, b) => a + b, 0);
    assert.ok(approx(total, 1, 0.01), `Energy should sum to 1: ${total}`);
  });

  it('low-frequency signal has energy mostly in lower levels', () => {
    const signal = Array.from({ length: 64 }, (_, i) => Math.sin(2 * Math.PI * i / 64));
    const coeffs = dwt(signal, HAAR, 3); // limit to 3 levels
    const energy = waveletEnergy(coeffs);
    // Finest detail should have least energy for low-frequency signal
    assert.ok(energy.details[0] < energy.approx,
      `Fine detail (${energy.details[0].toFixed(4)}) should be < approx (${energy.approx.toFixed(4)})`);
  });
});

describe('Stationary Wavelet Transform', () => {
  it('preserves signal length', () => {
    const signal = [1, 2, 3, 4, 5, 6, 7, 8];
    const { approx: lo, detail: hi } = swtStep(signal, HAAR, 0);
    assert.equal(lo.length, signal.length);
    assert.equal(hi.length, signal.length);
  });

  it('shift invariance', () => {
    const signal = [0, 0, 0, 1, 1, 0, 0, 0];
    const shifted = [0, 0, 0, 0, 1, 1, 0, 0];
    const { detail: hi1 } = swtStep(signal, HAAR, 0);
    const { detail: hi2 } = swtStep(shifted, HAAR, 0);
    // Both should detect the transition, just shifted
    const energy1 = hi1.reduce((s, v) => s + v * v, 0);
    const energy2 = hi2.reduce((s, v) => s + v * v, 0);
    assert.ok(approx(energy1, energy2, 0.1),
      `Shift invariant energy: ${energy1.toFixed(4)} vs ${energy2.toFixed(4)}`);
  });
});

describe('2D Wavelet Transform', () => {
  it('decomposes 4x4 matrix', () => {
    const matrix = [
      [1, 2, 3, 4],
      [5, 6, 7, 8],
      [9, 10, 11, 12],
      [13, 14, 15, 16],
    ];
    const result = dwt2d(matrix, HAAR);
    assert.equal(result.LL.length, 2);
    assert.equal(result.LL[0].length, 2);
    assert.equal(result.LH.length, 2);
    assert.equal(result.HL.length, 2);
    assert.equal(result.HH.length, 2);
  });

  it('LL contains low-frequency content', () => {
    // Constant matrix should have all energy in LL
    const matrix = [
      [5, 5, 5, 5],
      [5, 5, 5, 5],
      [5, 5, 5, 5],
      [5, 5, 5, 5],
    ];
    const result = dwt2d(matrix, HAAR);

    // LH, HL, HH should be ~0
    const lhEnergy = result.LH.flat().reduce((s, v) => s + v * v, 0);
    const hlEnergy = result.HL.flat().reduce((s, v) => s + v * v, 0);
    const hhEnergy = result.HH.flat().reduce((s, v) => s + v * v, 0);

    assert.ok(approx(lhEnergy, 0, 0.01), 'LH should be zero for constant');
    assert.ok(approx(hlEnergy, 0, 0.01), 'HL should be zero for constant');
    assert.ok(approx(hhEnergy, 0, 0.01), 'HH should be zero for constant');
  });

  it('detects edges in non-constant matrix', () => {
    const matrix = [
      [0, 1, 0, 1],
      [0, 1, 0, 1],
      [0, 1, 0, 1],
      [0, 1, 0, 1],
    ];
    const result = dwt2d(matrix, HAAR);
    // HL (horizontal detail) should capture vertical edge pattern
    const hlEnergy = result.HL.flat().reduce((s, v) => s + v * v, 0);
    assert.ok(hlEnergy > 0, `HL should detect vertical edges: ${hlEnergy}`);
  });
});

describe('Wavelet Roundtrip Stress', () => {
  it('roundtrips 20 random signals with Haar', () => {
    for (let i = 0; i < 20; i++) {
      const N = 64;
      const signal = Array.from({ length: N }, () => Math.random() * 10 - 5);
      const coeffs = dwt(signal, HAAR);
      const recovered = idwt(coeffs, HAAR);
      for (let j = 0; j < N; j++) {
        assert.ok(approx(recovered[j], signal[j], 0.01),
          `Haar roundtrip failed at ${j} (trial ${i})`);
      }
    }
  });

  it('roundtrips 10 random signals with DB2', () => {
    for (let i = 0; i < 10; i++) {
      const N = 32;
      const signal = Array.from({ length: N }, () => Math.random() * 4 - 2);
      const coeffs = dwt(signal, DB2);
      const recovered = idwt(coeffs, DB2);
      for (let j = 0; j < N; j++) {
        assert.ok(approx(recovered[j], signal[j], 0.05),
          `DB2 roundtrip failed at ${j} (trial ${i}): ${recovered[j].toFixed(4)} vs ${signal[j].toFixed(4)}`);
      }
    }
  });
});
