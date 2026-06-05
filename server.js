import { fastify } from 'fastify';
import fastifyStatic from '@fastify/static';
import { WebSocketServer } from 'ws';
import path from 'path';
import { fileURLToPath } from 'url';
import { queueJobWrite, getDbMetrics, purgeDatabase } from './db.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configurations
const PORT = parseInt(process.env.PORT || '3000', 10);
const NUM_PARTITIONS = 16;
let maxWorkerCount = 8;
let simulateNetworkLag = 0; // ms
let isBrokerPaused = false;

// Built-in Simulator
let isSimulationRunning = false;
let targetGeneratorRps = 10000;
let trafficGeneratorInterval = null;
let internalWorkersInterval = null;
let isInternalWorkersRunning = false;

// Dynamic SLA & Job Telemetry
let slaThresholdMs = 3000;
let jobTypeCounters = { transaction: 0, auth: 0, notification: 0, cleanup: 0 };

// SLA Auto-Scaling & Telemetry
let isAutoScaleEnabled = false;
let autoScaleStatus = 'DISABLED';
let latencyHistory = []; // Sliding window of recent latencies
const LATENCY_WINDOW_SIZE = 5000;

// Custom Partition-based Message Broker
class MessageBroker {
  constructor(numPartitions) {
    this.numPartitions = numPartitions;
    this.partitions = Array.from({ length: numPartitions }, () => []);
    this.consumers = new Map(); // workerId -> Assigned Partition Indices
    this.partitionLocks = Array.from({ length: numPartitions }, () => null); // partitionIndex -> workerId
    
    // Telemetry
    this.totalIngested = 0;
    this.totalProcessed = 0;
  }

  // Assign partitions using a simple hash modulo
  getPartitionIndex(jobId) {
    let hash = 0;
    for (let i = 0; i < jobId.length; i++) {
      hash = jobId.charCodeAt(i) + ((hash << 5) - hash);
    }
    return Math.abs(hash) % this.numPartitions;
  }

  // Publish a job to the broker
  publish(job) {
    if (isBrokerPaused) return false;
    
    const partitionIdx = this.getPartitionIndex(job.id);
    
    // BACKPRESSURE: Cap partition backlog at 10,000 items to avoid heap exhaustion
    if (this.partitions[partitionIdx].length >= 10000) {
      return false;
    }
    
    job.queued_at = Date.now();
    job.partition = partitionIdx;
    
    this.partitions[partitionIdx].push(job);
    this.totalIngested++;
    return true;
  }

  // Worker polling: fetch batch of jobs from assigned partitions
  poll(workerId, batchSize = 100) {
    const assignedPartitions = this.consumers.get(workerId) || [];
    const jobs = [];

    for (const partitionIdx of assignedPartitions) {
      const partitionQueue = this.partitions[partitionIdx];
      if (partitionQueue.length > 0) {
        // Drain up to batchSize from this partition
        const batch = partitionQueue.splice(0, Math.min(batchSize - jobs.length, partitionQueue.length));
        jobs.push(...batch);
        if (jobs.length >= batchSize) break;
      }
    }

    return jobs;
  }

  // Register worker and rebalance partition assignments
  registerWorker(workerId) {
    if (this.consumers.has(workerId)) return;
    this.consumers.set(workerId, []);
    this.rebalance();
    console.log(`Worker registered: ${workerId}. Rebalanced partitions.`);
  }

  // Unregister worker and rebalance
  unregisterWorker(workerId) {
    if (!this.consumers.has(workerId)) return;
    this.consumers.delete(workerId);
    this.rebalance();
    console.log(`Worker unregistered: ${workerId}. Rebalanced partitions.`);
  }

  // Evenly distribute partitions among registered consumers (Kafka-like rebalance)
  rebalance() {
    const workerIds = Array.from(this.consumers.keys());
    
    // Clear assignments
    for (const workerId of workerIds) {
      this.consumers.set(workerId, []);
    }
    this.partitionLocks.fill(null);

    if (workerIds.length === 0) return;

    // Distribute partitions
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

// Initialize Fastify
const app = fastify({ logger: false });

// Serve static dashboard files
app.register(fastifyStatic, {
  root: path.join(__dirname, 'public'),
  prefix: '/',
});

// Fast custom ID generator for 10k RPS
let jobCounter = 0;
function generateFastId() {
  jobCounter = (jobCounter + 1) % 10000000;
  return `job-${Date.now()}-${jobCounter}-${Math.random().toString(36).substring(2, 6)}`;
}

// In-Memory Simulated Traffic Generator (Producer)
function startInternalTrafficGenerator(targetRps = 10000) {
  stopInternalTrafficGenerator();
  isSimulationRunning = true;
  
  const intervalMs = 50;
  
  trafficGeneratorInterval = setInterval(() => {
    if (isBrokerPaused || !isSimulationRunning) return;
    const jobsPerInterval = Math.round(targetGeneratorRps / (1000 / intervalMs));
    const now = Date.now();
    for (let i = 0; i < jobsPerInterval; i++) {
      const rand = Math.random();
      let jobType = 'transaction';
      if (rand > 0.9) {
        jobType = 'cleanup';
      } else if (rand > 0.7) {
        jobType = 'notification';
      } else if (rand > 0.5) {
        jobType = 'auth';
      }

      const job = {
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
        total_latency: 0
      };
      broker.publish(job);
    }
  }, intervalMs);
}

function stopInternalTrafficGenerator() {
  if (trafficGeneratorInterval) {
    clearInterval(trafficGeneratorInterval);
    trafficGeneratorInterval = null;
  }
}

// In-Memory Simulated Worker Pool (Consumer)
function startInternalWorkers() {
  stopInternalWorkers();
  isInternalWorkersRunning = true;
  
  internalWorkersInterval = setInterval(() => {
    if (!isInternalWorkersRunning) return;
    
    // Create logical worker ids based on maxWorkerCount
    const count = maxWorkerCount;
    for (let i = 0; i < count; i++) {
      const workerId = `internal-worker-${i}`;
      broker.registerWorker(workerId);
      
      // Poll jobs
      const jobs = broker.poll(workerId, 250); // poll batch size
      if (jobs.length === 0) continue;
      
      // Simulate processing
      const now = Date.now();
      const workDelay = 5 + Math.floor(Math.random() * 15);
      
      setTimeout(() => {
        if (!isInternalWorkersRunning) return;
        
        const commitTime = Date.now();
        for (const job of jobs) {
          job.status = 'completed';
          job.started_at = now;
          job.completed_at = commitTime;
          
          job.ingest_latency = Math.max(0, job.started_at - job.created_at);
          job.execution_latency = Math.max(0, job.completed_at - job.started_at);
          job.total_latency = Math.max(0, job.completed_at - job.created_at);
          
          latencyHistory.push(job.total_latency);
          if (latencyHistory.length > LATENCY_WINDOW_SIZE) {
            latencyHistory.shift();
          }
          
          // Increment specific job type telemetry counter
          const typeKey = job.type || 'transaction';
          if (jobTypeCounters[typeKey] !== undefined) {
            jobTypeCounters[typeKey]++;
          } else {
            jobTypeCounters.transaction++;
          }

          broker.totalProcessed++;
          
          queueJobWrite(job);
        }
      }, workDelay);
    }
  }, 100);
}

function stopInternalWorkers() {
  isInternalWorkersRunning = false;
  if (internalWorkersInterval) {
    clearInterval(internalWorkersInterval);
    internalWorkersInterval = null;
  }
}

// Ingestion API: Submit Job
app.post('/api/jobs', async (request, reply) => {
  const payload = request.body || {};
  const job = {
    id: generateFastId(),
    type: payload.type || 'transaction',
    status: 'pending',
    payload: payload.data || {},
    created_at: Date.now(),
    queued_at: null,
    started_at: null,
    completed_at: null,
    ingest_latency: 0,
    execution_latency: 0,
    total_latency: 0
  };

  const success = broker.publish(job);
  if (success) {
    return reply.status(202).send({ id: job.id, status: 'queued', partition: job.partition });
  } else {
    return reply.status(503).send({ error: 'Message Broker is paused or congested' });
  }
});

// Ingestion API: Batch Job Submission (Highly Optimized for benchmarks)
app.post('/api/jobs/batch', async (request, reply) => {
  const { jobs: batchJobs } = request.body || { jobs: [] };
  const ingestedJobs = [];
  const now = Date.now();

  for (let i = 0; i < batchJobs.length; i++) {
    const job = {
      id: generateFastId(),
      type: batchJobs[i].type || 'transaction',
      status: 'pending',
      payload: batchJobs[i].data || {},
      created_at: now,
      queued_at: null,
      started_at: null,
      completed_at: null,
      ingest_latency: 0,
      execution_latency: 0,
      total_latency: 0
    };

    if (broker.publish(job)) {
      ingestedJobs.push(job.id);
    }
  }

  return reply.status(202).send({ count: ingestedJobs.length });
});

// Administrative control endpoints
app.post('/api/control', async (request, reply) => {
  const { action, value } = request.body || {};

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
      startInternalTrafficGenerator(targetGeneratorRps);
      startInternalWorkers();
      break;
    case 'stop_simulation':
      isSimulationRunning = false;
      stopInternalTrafficGenerator();
      stopInternalWorkers();
      break;
    case 'set_generator_rps':
      targetGeneratorRps = Math.max(100, Math.min(15000, parseInt(value, 10)));
      if (isSimulationRunning) {
        startInternalTrafficGenerator(targetGeneratorRps);
      }
      break;
    case 'set_sla_threshold':
      slaThresholdMs = Math.max(100, Math.min(10000, parseInt(value, 10)));
      break;
    case 'purge':
      purgeDatabase();
      broker.partitions.forEach(p => p.length = 0);
      broker.totalIngested = 0;
      broker.totalProcessed = 0;
      latencyHistory = [];
      jobTypeCounters = { transaction: 0, auth: 0, notification: 0, cleanup: 0 };
      break;
    default:
      return reply.status(400).send({ error: 'Unknown action' });
  }

  return reply.send({ 
    success: true, 
    isBrokerPaused, 
    maxWorkerCount, 
    simulateNetworkLag, 
    isAutoScaleEnabled, 
    autoScaleStatus,
    isSimulationRunning,
    targetGeneratorRps
  });
});

// Initialize WebSocket server (noServer mode — we'll wire up upgrade after listen)
const wss = new WebSocketServer({ noServer: true });

// Dashboard Telemetry Calculations

let lastTelemetryTime = Date.now();
let lastIngestedCount = 0;
let lastProcessedCount = 0;

let currentIngestRps = 0;
let currentProcessedRps = 0;

// Function to calculate exact percentiles
function calculatePercentiles(arr) {
  if (arr.length === 0) return { p50: 0, p90: 0, p99: 0 };
  const sorted = [...arr].sort((a, b) => a - b);
  return {
    p50: sorted[Math.floor(sorted.length * 0.50)],
    p90: sorted[Math.floor(sorted.length * 0.90)],
    p99: sorted[Math.floor(sorted.length * 0.99)],
  };
}

// System Health Metrics

/**
 * Endpoint for Workers to Poll Jobs
 */
app.post('/api/worker/poll', async (request, reply) => {
  const { workerId, batchSize } = request.body || {};
  broker.registerWorker(workerId);

  // Simulate network latency if set
  if (simulateNetworkLag > 0) {
    await new Promise(resolve => setTimeout(resolve, simulateNetworkLag));
  }

  const jobs = broker.poll(workerId, batchSize || 100);
  return reply.send({ jobs });
});

/**
 * Endpoint for Workers to Commit Processed Jobs
 */
app.post('/api/worker/commit', async (request, reply) => {
  const { workerId, jobs } = request.body || { jobs: [] };
  
  const now = Date.now();
  for (const job of jobs) {
    job.status = 'completed';
    job.completed_at = now;
    
    // Latency metrics in ms
    job.ingest_latency = Math.max(0, job.started_at - job.created_at);
    job.execution_latency = Math.max(0, job.completed_at - job.started_at);
    job.total_latency = Math.max(0, job.completed_at - job.created_at);

    // Record latency in history window
    latencyHistory.push(job.total_latency);
    if (latencyHistory.length > LATENCY_WINDOW_SIZE) {
      latencyHistory.shift();
    }

    // Increment specific job type telemetry counter
    const typeKey = job.type || 'transaction';
    if (jobTypeCounters[typeKey] !== undefined) {
      jobTypeCounters[typeKey]++;
    } else {
      jobTypeCounters.transaction++;
    }

    broker.totalProcessed++;

    // Write asynchronous to SQLite DB via optimized WAL batcher
    queueJobWrite(job);
  }

  return reply.send({ success: true });
});

// Keep track of active workers polling heartbeats
const workerHeartbeats = new Map();
app.post('/api/worker/heartbeat', async (request, reply) => {
  const { workerId } = request.body || {};
  workerHeartbeats.set(workerId, Date.now());
  broker.registerWorker(workerId);
  return reply.send({ success: true, maxWorkerCount });
});

// Periodic worker cleanups
setInterval(() => {
  const now = Date.now();
  for (const [workerId, lastSeen] of workerHeartbeats.entries()) {
    if (now - lastSeen > 5000) {
      // Dead worker
      broker.unregisterWorker(workerId);
      workerHeartbeats.delete(workerId);
    }
  }
}, 2000);

// Broadcast telemetry to connected Dashboard clients
setInterval(() => {
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

  // SLA Auto-Scaling Policy
  if (isAutoScaleEnabled) {
    const avgLatency = latencyHistory.length > 0 
      ? Math.round(latencyHistory.reduce((a, b) => a + b, 0) / latencyHistory.length) 
      : 0;
    
    const hasBacklog = broker.partitions.some(p => p.length > 400);
    const p99 = latencies.p99;
    
    // Auto-scale up if average latency exceeds 20% of SLA, or p99 exceeds 40% of SLA
    const scaleUpAvgLimit = Math.round(slaThresholdMs * 0.2);
    const scaleUpP99Limit = Math.round(slaThresholdMs * 0.4);
    
    // Auto-scale down if metrics recover below baseline limits
    const scaleDownAvgLimit = Math.round(slaThresholdMs * 0.067);
    const scaleDownP99Limit = Math.round(slaThresholdMs * 0.167);

    if (avgLatency > scaleUpAvgLimit || p99 > scaleUpP99Limit || hasBacklog) {
      if (maxWorkerCount < 24) {
        maxWorkerCount = 24;
        autoScaleStatus = 'SCALING_UP';
        console.log(`[SLA Auto-Scale] Queue backlog or latency spike (Avg: ${avgLatency}ms, P99: ${p99}ms vs SLA: ${slaThresholdMs}ms). Scaling worker pool up to ${maxWorkerCount}.`);
      } else {
        autoScaleStatus = 'SCALING_UP';
      }
    } else if (avgLatency < scaleDownAvgLimit && p99 < scaleDownP99Limit && !broker.partitions.some(p => p.length > 50)) {
      if (maxWorkerCount > 4) {
        maxWorkerCount = 4;
        autoScaleStatus = 'SCALING_DOWN';
        console.log(`[SLA Auto-Scale] Metrics normalized (Avg: ${avgLatency}ms, P99: ${p99}ms). Scaling worker pool down to baseline ${maxWorkerCount}.`);
      } else {
        autoScaleStatus = 'IDLE';
      }
    } else {
      autoScaleStatus = 'IDLE';
    }
  } else {
    autoScaleStatus = 'DISABLED';
  }

  const dbMetrics = getDbMetrics();
  const brokerTelemetry = broker.getTelemetry();

  const packet = JSON.stringify({
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
    jobTypeCounters
  });

  for (const client of wss.clients) {
    if (client.readyState === 1) {
      client.send(packet);
    }
  }
}, 500); // Send updates every 500ms

// Start Server
app.listen({ port: PORT, host: '0.0.0.0' }, (err, address) => {
  if (err) {
    console.error(err);
    process.exit(1);
  }

  // Attach WebSocket upgrade handler AFTER the server is listening
  const server = app.server;
  server.on('upgrade', (request, socket, head) => {
    if (request.url === '/ws') {
      wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
      });
    } else {
      socket.destroy();
    }
  });

  console.log(`Ingestion API & Broker Server listening on ${address}`);
});
