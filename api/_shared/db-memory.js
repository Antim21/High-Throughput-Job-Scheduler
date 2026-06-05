/**
 * In-Memory DB Replacement for Vercel (no native deps)
 * 
 * Implements the same interface as db.js but without better-sqlite3.
 * Tracks metrics in memory; discards actual job data to avoid heap bloat.
 */

let totalJobsInDb = 0;
let dbInsertCount = 0;
let lastCheckTime = Date.now();
let currentInsertRate = 0;

// Lightweight write buffer counter (we don't store actual records)
let pendingWrites = 0;
const BATCH_SIZE = 1000;
const FLUSH_INTERVAL_MS = 100;
let flushTimeout = null;

function flushBuffer() {
  if (pendingWrites === 0) return;
  const count = pendingWrites;
  pendingWrites = 0;
  if (flushTimeout) {
    clearTimeout(flushTimeout);
    flushTimeout = null;
  }
  dbInsertCount += count;
  totalJobsInDb += count;
}

/**
 * Queue a job for "persistent" storage (in-memory only on Vercel)
 */
export function queueJobWrite(_job) {
  pendingWrites++;

  if (pendingWrites >= BATCH_SIZE) {
    flushBuffer();
  } else if (!flushTimeout) {
    flushTimeout = setTimeout(flushBuffer, FLUSH_INTERVAL_MS);
  }
}

/**
 * Get telemetry metrics (same interface as db.js)
 */
export function getDbMetrics() {
  const now = Date.now();
  const timeDiffSec = (now - lastCheckTime) / 1000;

  if (timeDiffSec >= 1.0) {
    currentInsertRate = Math.round(dbInsertCount / timeDiffSec);
    dbInsertCount = 0;
    lastCheckTime = now;
  }

  return {
    insertRate: currentInsertRate,
    pendingWrites,
    totalJobsInDb,
    dbSizeMB: '0.00', // No file on Vercel
  };
}

/**
 * Purge all in-memory counters
 */
export function purgeDatabase() {
  pendingWrites = 0;
  dbInsertCount = 0;
  totalJobsInDb = 0;
  currentInsertRate = 0;
}
