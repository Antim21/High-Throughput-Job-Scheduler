import { parentPort } from 'worker_threads';

const SERVER_URL = 'http://localhost:3000';
const HEARTBEAT_INTERVAL = 2000;
const BATCH_SIZE = 500; // Batch poll size

class LogicalWorker {
  constructor(id) {
    this.id = id;
    this.running = false;
    this.jobsProcessed = 0;
    this.consecutiveEmptyPolls = 0;
  }

  async start() {
    this.running = true;
    this.heartbeat();
    this.heartbeatTimer = setInterval(() => this.heartbeat(), HEARTBEAT_INTERVAL);
    this.loop();
  }

  async stop() {
    this.running = false;
    clearInterval(this.heartbeatTimer);
    console.log(`Worker ${this.id} stopped. Total processed: ${this.jobsProcessed}`);
  }

  async heartbeat() {
    try {
      await fetch(`${SERVER_URL}/api/worker/heartbeat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ workerId: this.id })
      });
    } catch (err) {
      // Server down, ignore and retry next heartbeat
    }
  }

  async loop() {
    while (this.running) {
      try {
        const pollStart = Date.now();
        
        // Fetch jobs from assigned partitions
        const res = await fetch(`${SERVER_URL}/api/worker/poll`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ workerId: this.id, batchSize: BATCH_SIZE })
        });

        if (!res.ok) {
          await new Promise(resolve => setTimeout(resolve, 500));
          continue;
        }

        const { jobs } = await res.json();

        if (!jobs || jobs.length === 0) {
          this.consecutiveEmptyPolls++;
          // Exponential backoff to avoid hammering server when idle
          const backoff = Math.min(100, this.consecutiveEmptyPolls * 10);
          await new Promise(resolve => setTimeout(resolve, backoff));
          continue;
        }

        this.consecutiveEmptyPolls = 0;
        const now = Date.now();

        // Set start time for all jobs in the batch
        for (const job of jobs) {
          job.started_at = now;
        }

        // Simulate parallel execution latency for the batch using a single timer
        const workDelay = 5 + Math.floor(Math.random() * 15);
        await new Promise(resolve => setTimeout(resolve, workDelay));

        const processedJobs = jobs;

        this.jobsProcessed += processedJobs.length;

        // Commit completed jobs back to the server in a single bulk API write
        await fetch(`${SERVER_URL}/api/worker/commit`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ workerId: this.id, jobs: processedJobs })
        });

        // Loop immediately if we processed a full batch, otherwise small yield to event loop
        if (processedJobs.length < BATCH_SIZE) {
          await new Promise(resolve => setTimeout(resolve, 10));
        }

      } catch (err) {
        console.error(`Worker ${this.id} encountered error in loop:`, err.message);
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
  }
}

// Master coordinator to handle scaling worker loops
async function runWorkerManager() {
  const workers = new Map();
  let currentTargetCount = 4;

  console.log("Worker Manager process started.");

  // Periodic poll to check target worker count and scale accordingly
  setInterval(async () => {
    try {
      const res = await fetch(`${SERVER_URL}/api/worker/heartbeat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ workerId: 'manager' })
      });
      
      if (res.ok) {
        const { maxWorkerCount } = await res.json();
        currentTargetCount = maxWorkerCount;
      }
    } catch (e) {
      // Ignore API errors
    }

    // Scale UP workers
    if (workers.size < currentTargetCount) {
      const needed = currentTargetCount - workers.size;
      for (let i = 0; i < needed; i++) {
        const workerId = `worker-${Math.random().toString(36).substring(2, 6)}`;
        const worker = new LogicalWorker(workerId);
        worker.start();
        workers.set(workerId, worker);
        console.log(`Auto-Scaling: Spawned ${workerId}. Total workers: ${workers.size}/${currentTargetCount}`);
      }
    }

    // Scale DOWN workers
    if (workers.size > currentTargetCount) {
      const extra = workers.size - currentTargetCount;
      const keys = Array.from(workers.keys()).slice(0, extra);
      for (const key of keys) {
        const worker = workers.get(key);
        await worker.stop();
        workers.delete(key);
        console.log(`Auto-Scaling: Terminated ${key}. Total workers: ${workers.size}/${currentTargetCount}`);
      }
    }
  }, 1000);
}

runWorkerManager().catch(err => {
  console.error("Failed to start Worker Manager:", err);
});
