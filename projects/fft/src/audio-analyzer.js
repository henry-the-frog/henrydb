// audio-analyzer.js — Note/Chord detection + ASCII spectrogram
import { fft, sine, hanning, applyWindow, goertzel, Complex, toDB } from './fft.js';

// ===== Musical Note Frequencies (A4 = 440 Hz) =====
const NOTE_NAMES = ['C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#', 'A', 'A#', 'B'];

export function noteFrequency(note, octave) {
  // A4 = 440 Hz, MIDI note 69
  const noteIdx = NOTE_NAMES.indexOf(note);
  if (noteIdx === -1) throw new Error(`Unknown note: ${note}`);
  const midi = noteIdx + (octave + 1) * 12;
  return 440 * Math.pow(2, (midi - 69) / 12);
}

export function frequencyToNote(freq) {
  if (freq <= 0) return null;
  const midi = 69 + 12 * Math.log2(freq / 440);
  const midiRound = Math.round(midi);
  const noteIdx = ((midiRound % 12) + 12) % 12;
  const octave = Math.floor(midiRound / 12) - 1;
  const cents = (midi - midiRound) * 100;
  return { note: NOTE_NAMES[noteIdx], octave, cents: Math.round(cents), freq };
}

// ===== Chord Detection =====
const CHORD_PATTERNS = {
  'major':     [0, 4, 7],
  'minor':     [0, 3, 7],
  'dim':       [0, 3, 6],
  'aug':       [0, 4, 8],
  'sus2':      [0, 2, 7],
  'sus4':      [0, 5, 7],
  'maj7':      [0, 4, 7, 11],
  'min7':      [0, 3, 7, 10],
  'dom7':      [0, 4, 7, 10],
  'dim7':      [0, 3, 6, 9],
};

export function detectNotes(signal, sampleRate, threshold = -30) {
  // Find spectral peaks above threshold (in dB)
  const N = signal.length;
  const win = hanning(N);
  const windowed = applyWindow(signal, win);
  const spectrum = fft(windowed);
  const halfN = N / 2;

  const magnitudes = spectrum.slice(0, halfN).map(c => c.magnitude() * 2 / N);
  const peakThreshold = Math.pow(10, threshold / 20);

  // Find local maxima
  const peaks = [];
  for (let i = 2; i < halfN - 2; i++) {
    if (magnitudes[i] > magnitudes[i - 1] && magnitudes[i] > magnitudes[i + 1] &&
        magnitudes[i] > magnitudes[i - 2] && magnitudes[i] > magnitudes[i + 2] &&
        magnitudes[i] > peakThreshold) {
      // Parabolic interpolation for better frequency resolution
      const alpha = Math.log(magnitudes[i - 1] + 1e-10);
      const beta = Math.log(magnitudes[i] + 1e-10);
      const gamma = Math.log(magnitudes[i + 1] + 1e-10);
      const delta = 0.5 * (alpha - gamma) / (alpha - 2 * beta + gamma);
      const exactBin = i + delta;
      const exactFreq = exactBin * sampleRate / N;
      peaks.push({ freq: exactFreq, magnitude: magnitudes[i], bin: i });
    }
  }

  // Sort by magnitude, take strongest
  peaks.sort((a, b) => b.magnitude - a.magnitude);
  return peaks.slice(0, 12).map(p => ({
    ...frequencyToNote(p.freq),
    magnitude: p.magnitude,
    db: toDB(p.magnitude),
  }));
}

export function detectChord(signal, sampleRate, threshold = -30) {
  const notes = detectNotes(signal, sampleRate, threshold);
  if (notes.length < 3) return { chord: null, notes };

  // Get unique pitch classes
  const pitchClasses = [...new Set(notes.map(n => NOTE_NAMES.indexOf(n.note)))].sort((a, b) => a - b);
  if (pitchClasses.length < 3) return { chord: null, notes };

  // Try each pitch class as root
  let bestMatch = null;
  let bestScore = -1;

  for (const root of pitchClasses) {
    const intervals = pitchClasses.map(p => ((p - root) + 12) % 12).sort((a, b) => a - b);

    for (const [name, pattern] of Object.entries(CHORD_PATTERNS)) {
      // Check if pattern is a subset of detected intervals
      const matches = pattern.filter(p => intervals.includes(p)).length;
      const score = matches / pattern.length;
      if (score > bestScore && matches >= 3) {
        bestScore = score;
        bestMatch = { root: NOTE_NAMES[root], type: name, score };
      }
    }
  }

  return {
    chord: bestMatch ? `${bestMatch.root}${bestMatch.type === 'major' ? '' : bestMatch.type}` : null,
    confidence: bestMatch?.score || 0,
    notes,
  };
}

// ===== ASCII Spectrogram =====
export function asciiSpectrogram(signal, sampleRate, {
  windowSize = 256,
  hopSize = 128,
  maxFreq = null,
  width = 80,
  height = 24,
  chars = ' ░▒▓█',
} = {}) {
  const win = hanning(windowSize);
  const frames = [];
  for (let start = 0; start + windowSize <= signal.length; start += hopSize) {
    const frame = signal.slice(start, start + windowSize);
    const windowed = applyWindow(frame, win);
    const spectrum = fft(windowed);
    const halfN = windowSize / 2;
    const maxBin = maxFreq ? Math.min(Math.ceil(maxFreq * windowSize / sampleRate), halfN) : halfN;
    frames.push(spectrum.slice(0, maxBin).map(c => c.magnitude()));
  }

  if (frames.length === 0) return '';

  const numFreqBins = frames[0].length;

  // Resample to fit width x height
  const timeStep = Math.max(1, Math.floor(frames.length / width));
  const freqStep = Math.max(1, Math.floor(numFreqBins / height));

  // Find global max for normalization
  let maxMag = 0;
  for (const f of frames) for (const v of f) if (v > maxMag) maxMag = v;
  if (maxMag === 0) maxMag = 1;

  const lines = [];
  for (let row = height - 1; row >= 0; row--) {
    let line = '';
    for (let col = 0; col < width && col * timeStep < frames.length; col++) {
      const frameIdx = col * timeStep;
      const freqIdx = row * freqStep;
      const value = frames[frameIdx][freqIdx] || 0;
      const normalized = value / maxMag;
      const charIdx = Math.min(Math.floor(normalized * chars.length), chars.length - 1);
      line += chars[charIdx];
    }
    const freq = (row * freqStep * sampleRate / windowSize).toFixed(0);
    lines.push(`${freq.padStart(6)} Hz |${line}`);
  }

  // Time axis
  const duration = signal.length / sampleRate;
  lines.push('         +' + '-'.repeat(width));
  lines.push('         ' + '0s'.padEnd(width / 2) + `${duration.toFixed(1)}s`);

  return lines.join('\n');
}

// ===== Generate Musical Signals =====
export function generateChord(notes, sampleRate, duration) {
  // notes: array of { note, octave } or just frequency numbers
  const N = Math.floor(sampleRate * duration);
  const signal = new Array(N).fill(0);

  for (const n of notes) {
    const freq = typeof n === 'number' ? n : noteFrequency(n.note, n.octave);
    for (let i = 0; i < N; i++) {
      signal[i] += Math.sin(2 * Math.PI * freq * i / sampleRate);
    }
  }

  // Normalize
  const max = Math.max(...signal.map(Math.abs));
  if (max > 0) for (let i = 0; i < N; i++) signal[i] /= max;

  return signal;
}

// ===== DTMF (Dual-Tone Multi-Frequency) Detection =====
const DTMF_FREQS = {
  low: [697, 770, 852, 941],
  high: [1209, 1336, 1477, 1633],
};
const DTMF_MAP = [
  ['1', '2', '3', 'A'],
  ['4', '5', '6', 'B'],
  ['7', '8', '9', 'C'],
  ['*', '0', '#', 'D'],
];

export function dtmfGenerate(key, sampleRate, duration = 0.1) {
  let lowIdx = -1, highIdx = -1;
  for (let r = 0; r < 4; r++) {
    for (let c = 0; c < 4; c++) {
      if (DTMF_MAP[r][c] === key) { lowIdx = r; highIdx = c; break; }
    }
    if (lowIdx >= 0) break;
  }
  if (lowIdx < 0) throw new Error(`Unknown DTMF key: ${key}`);

  const N = Math.floor(sampleRate * duration);
  return Array.from({ length: N }, (_, i) =>
    Math.sin(2 * Math.PI * DTMF_FREQS.low[lowIdx] * i / sampleRate) +
    Math.sin(2 * Math.PI * DTMF_FREQS.high[highIdx] * i / sampleRate)
  );
}

export function dtmfDetect(signal, sampleRate) {
  // Use Goertzel to check each DTMF frequency
  const allFreqs = [...DTMF_FREQS.low, ...DTMF_FREQS.high];
  const magnitudes = allFreqs.map(f => goertzel(signal, f, sampleRate).magnitude());

  // Find strongest low and high
  let maxLow = 0, maxLowIdx = 0;
  for (let i = 0; i < 4; i++) {
    if (magnitudes[i] > maxLow) { maxLow = magnitudes[i]; maxLowIdx = i; }
  }
  let maxHigh = 0, maxHighIdx = 0;
  for (let i = 4; i < 8; i++) {
    if (magnitudes[i] > maxHigh) { maxHigh = magnitudes[i]; maxHighIdx = i - 4; }
  }

  // Threshold: both tones must be significantly above noise
  const noise = magnitudes.reduce((a, b) => a + b, 0) / 8;
  if (maxLow < noise * 2 || maxHigh < noise * 2) return null;

  return DTMF_MAP[maxLowIdx][maxHighIdx];
}
