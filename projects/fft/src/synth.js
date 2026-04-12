// synth.js — Audio Synthesizer
// Additive, FM, wavetable, and subtractive synthesis

import { sine, fft, ifft, hanning, applyWindow } from './fft.js';
import { applyBiquad, biquadLowpass, biquadHighpass, biquadBandpass } from './fft.js';

// ===== Oscillators =====

export function sineOsc(freq, sampleRate, duration, phase = 0) {
  const N = Math.floor(sampleRate * duration);
  return Array.from({ length: N }, (_, n) =>
    Math.sin(2 * Math.PI * freq * n / sampleRate + phase)
  );
}

export function sawtoothOsc(freq, sampleRate, duration) {
  const N = Math.floor(sampleRate * duration);
  return Array.from({ length: N }, (_, n) => {
    const t = (freq * n / sampleRate) % 1;
    return 2 * t - 1;
  });
}

export function squareOsc(freq, sampleRate, duration, duty = 0.5) {
  const N = Math.floor(sampleRate * duration);
  return Array.from({ length: N }, (_, n) => {
    const t = (freq * n / sampleRate) % 1;
    return t < duty ? 1 : -1;
  });
}

export function triangleOsc(freq, sampleRate, duration) {
  const N = Math.floor(sampleRate * duration);
  return Array.from({ length: N }, (_, n) => {
    const t = (freq * n / sampleRate) % 1;
    return t < 0.5 ? 4 * t - 1 : 3 - 4 * t;
  });
}

export function noiseOsc(sampleRate, duration) {
  const N = Math.floor(sampleRate * duration);
  return Array.from({ length: N }, () => Math.random() * 2 - 1);
}

// ===== Envelopes =====

// ADSR envelope (attack, decay, sustain level, release — in seconds)
export function adsr(attack, decay, sustainLevel, release, duration, sampleRate) {
  const N = Math.floor(sampleRate * duration);
  const attackSamples = Math.floor(attack * sampleRate);
  const decaySamples = Math.floor(decay * sampleRate);
  const releaseSamples = Math.floor(release * sampleRate);
  const sustainSamples = N - attackSamples - decaySamples - releaseSamples;

  const env = new Array(N);
  for (let i = 0; i < N; i++) {
    if (i < attackSamples) {
      env[i] = i / attackSamples;
    } else if (i < attackSamples + decaySamples) {
      const t = (i - attackSamples) / decaySamples;
      env[i] = 1 - t * (1 - sustainLevel);
    } else if (i < attackSamples + decaySamples + Math.max(0, sustainSamples)) {
      env[i] = sustainLevel;
    } else {
      const t = (i - N + releaseSamples) / releaseSamples;
      env[i] = sustainLevel * (1 - t);
    }
    env[i] = Math.max(0, Math.min(1, env[i]));
  }
  return env;
}

// Exponential decay envelope
export function expDecay(decayTime, duration, sampleRate) {
  const N = Math.floor(sampleRate * duration);
  return Array.from({ length: N }, (_, n) =>
    Math.exp(-n / (decayTime * sampleRate))
  );
}

// Apply envelope to signal
export function applyEnvelope(signal, envelope) {
  return signal.map((v, i) => v * (envelope[i] || 0));
}

// ===== Additive Synthesis =====
// Build signal from harmonic partials
export function additiveSynth(fundamentalFreq, harmonics, sampleRate, duration) {
  // harmonics: array of { harmonic, amplitude, phase }
  const N = Math.floor(sampleRate * duration);
  const signal = new Array(N).fill(0);

  for (const { harmonic, amplitude, phase = 0 } of harmonics) {
    const freq = fundamentalFreq * harmonic;
    if (freq >= sampleRate / 2) continue; // skip above Nyquist
    for (let n = 0; n < N; n++) {
      signal[n] += amplitude * Math.sin(2 * Math.PI * freq * n / sampleRate + phase);
    }
  }

  return signal;
}

// Common harmonic presets
export function organHarmonics(numHarmonics = 8) {
  return Array.from({ length: numHarmonics }, (_, i) => ({
    harmonic: i + 1,
    amplitude: 1 / (i + 1),
    phase: 0,
  }));
}

export function bellHarmonics() {
  // Bells have inharmonic partials
  return [
    { harmonic: 1, amplitude: 1.0, phase: 0 },
    { harmonic: 2.0, amplitude: 0.6, phase: 0 },
    { harmonic: 2.76, amplitude: 0.4, phase: 0 },
    { harmonic: 3.68, amplitude: 0.25, phase: 0 },
    { harmonic: 5.36, amplitude: 0.15, phase: 0 },
    { harmonic: 6.84, amplitude: 0.1, phase: 0 },
  ];
}

// ===== FM Synthesis =====
// carrier + modulator → rich timbres
export function fmSynth(carrierFreq, modFreq, modIndex, sampleRate, duration) {
  const N = Math.floor(sampleRate * duration);
  return Array.from({ length: N }, (_, n) => {
    const t = n / sampleRate;
    const modulator = modIndex * Math.sin(2 * Math.PI * modFreq * t);
    return Math.sin(2 * Math.PI * carrierFreq * t + modulator);
  });
}

// FM with time-varying modulation index
export function fmSynthEnvelope(carrierFreq, modFreq, modIndexEnvelope, sampleRate, duration) {
  const N = Math.floor(sampleRate * duration);
  return Array.from({ length: N }, (_, n) => {
    const t = n / sampleRate;
    const idx = modIndexEnvelope[n] || 0;
    const modulator = idx * Math.sin(2 * Math.PI * modFreq * t);
    return Math.sin(2 * Math.PI * carrierFreq * t + modulator);
  });
}

// ===== Wavetable Synthesis =====
export class Wavetable {
  constructor(table) {
    this.table = table;
    this.size = table.length;
  }

  static fromHarmonics(harmonics, size = 2048) {
    const table = new Array(size).fill(0);
    for (const { harmonic, amplitude, phase = 0 } of harmonics) {
      for (let i = 0; i < size; i++) {
        table[i] += amplitude * Math.sin(2 * Math.PI * harmonic * i / size + phase);
      }
    }
    // Normalize
    const max = Math.max(...table.map(Math.abs));
    if (max > 0) for (let i = 0; i < size; i++) table[i] /= max;
    return new Wavetable(table);
  }

  static sine(size = 2048) {
    return new Wavetable(Array.from({ length: size }, (_, i) =>
      Math.sin(2 * Math.PI * i / size)));
  }

  static saw(size = 2048) {
    return Wavetable.fromHarmonics(
      Array.from({ length: 32 }, (_, i) => ({
        harmonic: i + 1,
        amplitude: 1 / (i + 1) * (i % 2 === 0 ? 1 : -1),
      })),
      size
    );
  }

  // Read with linear interpolation
  read(phase) {
    const idx = ((phase % 1) + 1) % 1 * this.size;
    const i0 = Math.floor(idx) % this.size;
    const i1 = (i0 + 1) % this.size;
    const frac = idx - Math.floor(idx);
    return this.table[i0] * (1 - frac) + this.table[i1] * frac;
  }

  // Generate signal at given frequency
  play(freq, sampleRate, duration) {
    const N = Math.floor(sampleRate * duration);
    const signal = new Array(N);
    const phaseInc = freq / sampleRate;
    let phase = 0;
    for (let n = 0; n < N; n++) {
      signal[n] = this.read(phase);
      phase += phaseInc;
    }
    return signal;
  }
}

// ===== Subtractive Synthesis =====
// Rich waveform → filter to shape timbre
export function subtractiveSynth(waveform, cutoffFreq, sampleRate, resonance = 1) {
  const filter = biquadLowpass(cutoffFreq, sampleRate, resonance);
  return applyBiquad(waveform, filter);
}

// ===== Effects =====

// Delay line (echo)
export function delay(signal, delayTime, feedback, sampleRate) {
  const delaySamples = Math.floor(delayTime * sampleRate);
  const output = [...signal];
  for (let i = delaySamples; i < output.length; i++) {
    output[i] += output[i - delaySamples] * feedback;
  }
  return output;
}

// Simple reverb (multiple delay lines with different times)
export function reverb(signal, roomSize = 0.5, damping = 0.5, sampleRate = 44100) {
  const delays = [0.0297, 0.0371, 0.0411, 0.0437].map(d => d * roomSize);
  const gains = [0.7, 0.6, 0.5, 0.4].map(g => g * (1 - damping));

  let wet = new Array(signal.length).fill(0);
  for (let d = 0; d < delays.length; d++) {
    const delaySamples = Math.floor(delays[d] * sampleRate);
    for (let i = 0; i < signal.length; i++) {
      const src = i - delaySamples;
      if (src >= 0) wet[i] += signal[src] * gains[d];
    }
  }

  return signal.map((v, i) => v * 0.7 + wet[i] * 0.3);
}

// Distortion (soft clipping)
export function distort(signal, drive = 2) {
  return signal.map(v => Math.tanh(v * drive));
}

// Tremolo (amplitude modulation)
export function tremolo(signal, rate, depth, sampleRate) {
  return signal.map((v, i) => {
    const mod = 1 - depth * (0.5 + 0.5 * Math.sin(2 * Math.PI * rate * i / sampleRate));
    return v * mod;
  });
}

// Vibrato (frequency modulation)
export function vibrato(signal, rate, depth, sampleRate) {
  const output = new Array(signal.length);
  for (let i = 0; i < signal.length; i++) {
    const offset = depth * sampleRate * Math.sin(2 * Math.PI * rate * i / sampleRate);
    const readIdx = i + offset;
    const i0 = Math.floor(readIdx);
    const frac = readIdx - i0;
    const s0 = signal[Math.max(0, Math.min(signal.length - 1, i0))];
    const s1 = signal[Math.max(0, Math.min(signal.length - 1, i0 + 1))];
    output[i] = s0 * (1 - frac) + s1 * frac;
  }
  return output;
}

// ===== Mix utility =====
export function mix(signals, gains = null) {
  const maxLen = Math.max(...signals.map(s => s.length));
  const g = gains || signals.map(() => 1 / signals.length);
  const output = new Array(maxLen).fill(0);
  for (let ch = 0; ch < signals.length; ch++) {
    for (let i = 0; i < signals[ch].length; i++) {
      output[i] += signals[ch][i] * g[ch];
    }
  }
  return output;
}

// Normalize to [-1, 1]
export function normalize(signal) {
  const max = Math.max(...signal.map(Math.abs));
  if (max === 0) return signal;
  return signal.map(v => v / max);
}
