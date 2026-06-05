/**
 * Shared Broker State for Vercel Serverless Functions
 * 
 * All state lives at module-level so it persists across requests
 * within the same serverless function instance. State resets on cold starts.
 */

// ─── Configurations ───
const NUM_PARTITIONS = 16;
let maxWorkerCount = 8;
let simulateNetworkLag = 0;
let isBrokerPaused = false;

// ─── Simulator State ───
let isSimulationRunning = false;
let targetGeneratorRps = 10000;
let isInternalWorkersRunning = false;
let inFlightBatches = [];
const workerBusyUntil = new Map();

// ─── SLA & Telemetry ───
let slaThresholdMs = 3000;
let jobTypeCounters = { transaction: 0, auth: 0, notification: 0, cleanup: 0 };
let isAutoScaleEnabled = false;
let autoScaleStatus = 'DISABLED';
let latencyHistory = [];
const LATENCY_WINDOW_SIZE = 5000;

// ─── RPS Tracking ───
let lastTelemetryTime = Date.now();
let lastIngestedCount = 0;
let lastProcessedCount = 0;
let currentIngestRps = 0;
let currentProcessedRps = 0;

// ─── In-memory DB metrics ───
import { queueJobWrite, getDbMetrics, purgeDatabase } from './db-memory.js';

// ─── MessageBroker ───
class MessageBroker {
  constructor(numPartitions) {
    this.numPartitions = numPartitions;
    this.partitions = Array.from({ length: numPartitions }, () => []);
    this.consumers = new Map();
    this.partitionLocks = Array.from({ length: numPartitions }, () => null);
    this.totalIngested = 0;
    this.totalProcessed = 0;
  }

  getPartitionIndex(jobId) {
    let hash = 0;
    for (let i = 0; i < jobId.length; i++) {
      hash = jobId.charCodeAt(i) + ((hash << 5) - hash);
    }
    return Math.abs(hash) % this.numPartitions;
  }

  publish(job) {
    if (isBrokerPaused) return false;
    const partitionIdx = this.getPartitionIndex(job.id);
    if (this.partitions[partitionIdx].length >= 10000) return false;
    job.queued_at = Date.now();
    job.partition = partitionIdx;
    this.partitions[partitionIdx].push(job);
    this.totalIngested++;
    return true;
  }

  poll(workerId, batchSize = 100) {
    const assignedPartitions = this.consumers.get(workerId) || [];
    const jobs = [];
    for (const partitionIdx of assignedPartitions) {
      const partitionQueue = this.partitions[partitionIdx];
      if (partitionQueue.length > 0) {
        const batch = partitionQueue.splice(0, Math.min(batchSize - jobs.length, partitionQueue.length));
        jobs.push(...batch);
        if (jobs.length >= batchSize) break;
      }
    }
    return jobs;
  }

  registerWorker(workerId) {
    if (this.consumers.has(workerId)) return;
    this.consumers.set(workerId, []);
    this.rebalance();
  }

  unregisterWorker(workerId) {
    if (!this.consumers.has(workerId)) return;
    this.consumers.delete(workerId);
    this.rebalance();
  }

  rebalance() {
    const workerIds = Array.from(this.consumers.keys());
    for (const wid of workerIds) this.consumers.set(wid, []);
    this.partitionLocks.fill(null);
    if (workerIds.length === 0) return;
    for (let i = 0; i < this.numPartitions; i++) {
      const workerIdx = i % workerIds.length;
      const workerId = workerIds[workerIdx];
      this.consumers.get(workerId).push(i);
      this.partitionLocks[i] = workerId;
    }
  }

  getTelemetry() {
    return {
      partitionSizes: this.partitions.map(p => p.length),
      activeWorkers: this.consumers.size,
      workerAssignments: Object.fromEntries(this.consumers),
    };
  }
}

const broker = new MessageBroker(NUM_PARTITIONS);

// ─── Fast ID Generator ───
let jobCounter = 0;
function generateFastId() {
  jobCounter = (jobCounter + 1) % 10000000;
  return `job-${Date.now()}-${jobCounter}-${Math.random().toString(36).substring(2, 6)}`;
}

// ─── Percentile Calculator ───
function calculatePercentiles(arr) {
  if (arr.length === 0) return { p50: 0, p90: 0, p99: 0 };
  const sorted = [...arr].sort((a, b) => a - b);
  return {
    p50: sorted[Math.floor(sorted.length * 0.50)],
    p90: sorted[Math.floor(sorted.length * 0.90)],
    p99: sorted[Math.floor(sorted.length * 0.99)],
  };
}

// ─── Internal Traffic Generator ───
function startInternalTrafficGenerator() {
  isSimulationRunning = true;
}

function stopInternalTrafficGenerator() {
  isSimulationRunning = false;
}

// ─── Internal Workers ───
function startInternalWorkers() {
  isInternalWorkersRunning = true;
}

function stopInternalWorkers() {
  isInternalWorkersRunning = false;
}

// ─── Run Simulation Step (driven by active connection tick) ───
function runSimulationStep(deltaMs) {
  const now = Date.now();

  // 1. Process completed in-flight batches
  const completedBatches = [];
  const remainingBatches = [];
  
  for (const batch of inFlightBatches) {
    if (batch.completionTime <= now) {
      completedBatches.push(batch);
    } else {
      remainingBatches.push(batch);
    }
  }
  inFlightBatches = remainingBatches;

  let latencyAddedCount = 0;
  for (const batch of completedBatches) {
    const commitTime = batch.completionTime;
    const started_at = batch.started_at;

    for (const job of batch.jobs) {
      job.status = 'completed';
      job.started_at = started_at;
      job.completed_at = commitTime;
      job.ingest_latency = Math.max(0, job.started_at - job.created_at);
      job.execution_latency = Math.max(0, job.completed_at - job.started_at);
      job.total_latency = Math.max(0, job.completed_at - job.created_at);

      latencyHistory.push(job.total_latency);
      latencyAddedCount++;

      const typeKey = job.type || 'transaction';
      if (jobTypeCounters[typeKey] !== undefined) jobTypeCounters[typeKey]++;
      else jobTypeCounters.transaction++;

      broker.totalProcessed++;
      queueJobWrite(job);
    }
  }

  // Cap latencyHistory window efficiently using slice
  if (latencyAddedCount > 0 && latencyHistory.length > LATENCY_WINDOW_SIZE) {
    latencyHistory = latencyHistory.slice(-LATENCY_WINDOW_SIZE);
  }

  if (!isSimulationRunning) return;

  // 2. Ingest simulated traffic
  if (!isBrokerPaused) {
    const jobsPerInterval = (targetGeneratorRps * deltaMs) / 1000;
    const integerJobs = Math.floor(jobsPerInterval);
    const fractionalRemainder = jobsPerInterval - integerJobs;
    const extraJob = Math.random() < fractionalRemainder ? 1 : 0;
    const finalJobsCount = integerJobs + extraJob;

    for (let i = 0; i < finalJobsCount; i++) {
      const rand = Math.random();
      let jobType = 'transaction';
      if (rand > 0.9) jobType = 'cleanup';
      else if (rand > 0.7) jobType = 'notification';
      else if (rand > 0.5) jobType = 'auth';

      broker.publish({
        id: generateFastId(),
        type: jobType,
        status: 'pending',
        payload: { amount: Math.round(Math.random() * 1000), currency: 'USD' },
        created_at: now,
        queued_at: null,
        started_at: null,
        completed_at: null,
        ingest_latency: 0,
        execution_latency: 0,
        total_latency: 0,
      });
    }
  }

  // 3. Consume jobs using simulated worker threads
  if (isInternalWorkersRunning) {
    const count = maxWorkerCount;
    for (let i = 0; i < count; i++) {
      const workerId = `internal-worker-${i}`;
      broker.registerWorker(workerId);

      // Check if worker is busy
      const busyUntil = workerBusyUntil.get(workerId) || 0;
      if (now < busyUntil) {
        continue; // Worker is still processing previous batch
      }

      // Worker is free! Poll jobs
      const batchSize = 250;
      const jobs = broker.poll(workerId, batchSize);
      if (jobs.length === 0) continue;

      // Calculate latency & delay
      const workDelay = 5 + Math.floor(Math.random() * 15); // 5-20ms
      const totalDelay = workDelay + simulateNetworkLag;
      const completionTime = now + totalDelay;

      // Mark worker as busy
      workerBusyUntil.set(workerId, completionTime);

      // Group into a single batch object
      inFlightBatches.push({
        completionTime,
        started_at: now,
        jobs,
      });
    }
  }
}

// ─── Worker Heartbeat Tracking ───
const workerHeartbeats = new Map();

// Periodic dead worker cleanup
setInterval(() => {
  const now = Date.now();
  for (const [workerId, lastSeen] of workerHeartbeats.entries()) {
    if (now - lastSeen > 5000) {
      broker.unregisterWorker(workerId);
      workerHeartbeats.delete(workerId);
    }
  }
}, 2000);

// ─── Telemetry Snapshot Builder ───
function buildTelemetrySnapshot() {
  const now = Date.now();
  const timeDiffSec = (now - lastTelemetryTime) / 1000;

  if (timeDiffSec >= 0.5) {
    const deltaIngest = broker.totalIngested - lastIngestedCount;
    const deltaProcess = broker.totalProcessed - lastProcessedCount;
    currentIngestRps = Math.round(deltaIngest / timeDiffSec);
    currentProcessedRps = Math.round(deltaProcess / timeDiffSec);
    lastIngestedCount = broker.totalIngested;
    lastProcessedCount = broker.totalProcessed;
    lastTelemetryTime = now;
  }

  const latencies = calculatePercentiles(latencyHistory);

  // Auto-scaling logic
  if (isAutoScaleEnabled) {
    const avgLatency = latencyHistory.length > 0
      ? Math.round(latencyHistory.reduce((a, b) => a + b, 0) / latencyHistory.length)
      : 0;
    const hasBacklog = broker.partitions.some(p => p.length > 400);
    const p99 = latencies.p99;
    const scaleUpAvgLimit = Math.round(slaThresholdMs * 0.2);
    const scaleUpP99Limit = Math.round(slaThresholdMs * 0.4);
    const scaleDownAvgLimit = Math.round(slaThresholdMs * 0.067);
    const scaleDownP99Limit = Math.round(slaThresholdMs * 0.167);

    if (avgLatency > scaleUpAvgLimit || p99 > scaleUpP99Limit || hasBacklog) {
      if (maxWorkerCount < 24) maxWorkerCount = 24;
      autoScaleStatus = 'SCALING_UP';
    } else if (avgLatency < scaleDownAvgLimit && p99 < scaleDownP99Limit && !broker.partitions.some(p => p.length > 50)) {
      if (maxWorkerCount > 4) maxWorkerCount = 4;
      autoScaleStatus = maxWorkerCount <= 4 ? 'IDLE' : 'SCALING_DOWN';
    } else {
      autoScaleStatus = 'IDLE';
    }
  } else {
    autoScaleStatus = 'DISABLED';
  }

  const dbMetrics = getDbMetrics();
  const brokerTelemetry = broker.getTelemetry();

  return {
    timestamp: now,
    ingestRps: currentIngestRps,
    processRps: currentProcessedRps,
    totalIngested: broker.totalIngested,
    totalProcessed: broker.totalProcessed,
    avgLatency: latencyHistory.length > 0
      ? Math.round(latencyHistory.reduce((a, b) => a + b, 0) / latencyHistory.length)
      : 0,
    p50Latency: latencies.p50,
    p90Latency: latencies.p90,
    p99Latency: latencies.p99,
    partitions: brokerTelemetry.partitionSizes,
    activeWorkers: brokerTelemetry.activeWorkers,
    maxWorkerCount,
    simulateNetworkLag,
    isBrokerPaused,
    isAutoScaleEnabled,
    autoScaleStatus,
    db: dbMetrics,
    isSimulationRunning,
    generatorRps: targetGeneratorRps,
    slaThresholdMs,
    jobTypeCounters,
  };
}

// ─── Control Handler ───
function handleControl(action, value) {
  switch (action) {
    case 'pause':
      isBrokerPaused = value;
      break;
    case 'set_workers':
      maxWorkerCount = Math.max(1, Math.min(32, parseInt(value, 10)));
      break;
    case 'set_lag':
      simulateNetworkLag = Math.max(0, parseInt(value, 10));
      break;
    case 'set_autoscale':
      isAutoScaleEnabled = !!value;
      autoScaleStatus = isAutoScaleEnabled ? 'IDLE' : 'DISABLED';
      break;
    case 'start_simulation':
      isSimulationRunning = true;
      startInternalTrafficGenerator();
      startInternalWorkers();
      break;
    case 'stop_simulation':
      isSimulationRunning = false;
      stopInternalTrafficGenerator();
      stopInternalWorkers();
      break;
    case 'set_generator_rps':
      targetGeneratorRps = Math.max(100, Math.min(15000, parseInt(value, 10)));
      if (isSimulationRunning) startInternalTrafficGenerator();
      break;
    case 'set_sla_threshold':
      slaThresholdMs = Math.max(100, Math.min(10000, parseInt(value, 10)));
      break;
    case 'purge':
      purgeDatabase();
      broker.partitions.forEach(p => (p.length = 0));
      broker.totalIngested = 0;
      broker.totalProcessed = 0;
      latencyHistory = [];
      jobTypeCounters = { transaction: 0, auth: 0, notification: 0, cleanup: 0 };
      inFlightBatches = [];
      workerBusyUntil.clear();
      break;
    default:
      return null;
  }

  return {
    success: true,
    isBrokerPaused,
    maxWorkerCount,
    simulateNetworkLag,
    isAutoScaleEnabled,
    autoScaleStatus,
    isSimulationRunning,
    targetGeneratorRps,
  };
}

export {
  broker,
  generateFastId,
  buildTelemetrySnapshot,
  handleControl,
  workerHeartbeats,
  queueJobWrite,
  getDbMetrics,
  purgeDatabase,
  runSimulationStep,
};

// Re-export getter accessors for mutable state
export function getState() {
  return {
    isBrokerPaused,
    maxWorkerCount,
    simulateNetworkLag,
    isAutoScaleEnabled,
    autoScaleStatus,
    isSimulationRunning,
    targetGeneratorRps,
    slaThresholdMs,
    jobTypeCounters,
    latencyHistory,
  };
}
