import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';

const DB_FILE = process.env.DB_PATH || './scheduler.db';
const dbPath = path.resolve(DB_FILE);

// Ensure target database directory exists recursively
const dbDir = path.dirname(dbPath);
if (!fs.existsSync(dbDir)) {
  fs.mkdirSync(dbDir, { recursive: true });
}

// Initialize SQLite database
const db = new Database(dbPath);

// Configure SQLite for high performance (WAL Mode, normal sync, memory temp store)
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.pragma('temp_store = MEMORY');
db.pragma('cache_size = -64000'); // ~64MB cache

// Create jobs table
db.exec(`
  CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    type TEXT,
    status TEXT,
    payload TEXT,
    created_at INTEGER,
    queued_at INTEGER,
    started_at INTEGER,
    completed_at INTEGER,
    ingest_latency INTEGER,
    execution_latency INTEGER,
    total_latency INTEGER
  );
  
  CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
  CREATE INDEX IF NOT EXISTS idx_jobs_completed ON jobs(completed_at) WHERE status = 'completed';
`);

// Prepared statement for bulk inserts
const insertStmt = db.prepare(`
  INSERT OR REPLACE INTO jobs (
    id, type, status, payload, created_at, queued_at, started_at, completed_at, 
    ingest_latency, execution_latency, total_latency
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

// In-memory buffer for bulk writes
let writeBuffer = [];
const BATCH_SIZE = 1000;
const FLUSH_INTERVAL_MS = 100; // Flush every 100ms at most
let flushTimeout = null;

// Telemetry counters
let dbInsertCount = 0;
let lastCheckTime = Date.now();
let currentInsertRate = 0;

// Initialize total jobs count in-memory to prevent blocking the event loop on every telemetry tick
let totalJobsInDb = 0;
try {
  const row = db.prepare("SELECT count(*) as count FROM jobs").get();
  totalJobsInDb = row ? row.count : 0;
  console.log(`[Database] Initialized total jobs counter from database: ${totalJobsInDb}`);
} catch (err) {
  console.error('[Database] Failed to read initial jobs count:', err);
}

/**
 * Executes a high-performance batch transaction on the SQLite database
 */
function flushBuffer() {
  if (writeBuffer.length === 0) return;

  const currentBatch = writeBuffer;
  writeBuffer = [];
  if (flushTimeout) {
    clearTimeout(flushTimeout);
    flushTimeout = null;
  }

  const startTime = Date.now();

  // Execute batch in a single optimized transaction
  const transaction = db.transaction((jobs) => {
    for (const job of jobs) {
      insertStmt.run(
        job.id,
        job.type,
        job.status,
        JSON.stringify(job.payload || {}),
        job.created_at,
        job.queued_at,
        job.started_at,
        job.completed_at,
        job.ingest_latency,
        job.execution_latency,
        job.total_latency
      );
    }
  });

  try {
    transaction(currentBatch);
    dbInsertCount += currentBatch.length;
    totalJobsInDb += currentBatch.length;
  } catch (err) {
    console.error('Failed to commit SQLite transaction batch:', err);
  }
}

/**
 * Queue a job for persistent storage (bulk transaction)
 */
export function queueJobWrite(job) {
  writeBuffer.push(job);

  // Trigger flush immediately if buffer reaches capacity
  if (writeBuffer.length >= BATCH_SIZE) {
    flushBuffer();
  } else if (!flushTimeout) {
    // Or schedule flush on interval
    flushTimeout = setTimeout(flushBuffer, FLUSH_INTERVAL_MS);
  }
}

/**
 * Get telemetry metrics for the database
 */
export function getDbMetrics() {
  const now = Date.now();
  const timeDiffSec = (now - lastCheckTime) / 1000;
  
  if (timeDiffSec >= 1.0) {
    currentInsertRate = Math.round(dbInsertCount / timeDiffSec);
    dbInsertCount = 0;
    lastCheckTime = now;
  }

  let dbSize = 0;
  try {
    if (fs.existsSync(dbPath)) {
      dbSize = fs.statSync(dbPath).size;
    }
  } catch (err) {
    // Ignore stat failures
  }

  // Count total items currently in db
  // (Served from cached top-level in-memory counter to prevent synchronous database calls blocking the event loop)

  return {
    insertRate: currentInsertRate,
    pendingWrites: writeBuffer.length,
    totalJobsInDb,
    dbSizeMB: (dbSize / (1024 * 1024)).toFixed(2)
  };
}

/**
 * Purges database records to keep files lightweight during benchmarks
 */
export function purgeDatabase() {
  db.exec("DELETE FROM jobs; VACUUM;");
  writeBuffer = [];
  dbInsertCount = 0;
  totalJobsInDb = 0;
  console.log("SQLite database purged successfully and in-memory counter reset.");
}

// Flush any remaining writes on process exit
process.on('exit', () => {
  flushBuffer();
  db.close();
});
