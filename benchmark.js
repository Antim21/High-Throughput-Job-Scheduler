import http from 'http';

const SERVER_URL = 'http://localhost:3000/api/jobs/batch';
const TARGET_RPS = 10000;
const BATCH_INTERVAL_MS = 50; // Emit batch every 50ms
const BATCH_SIZE = Math.round(TARGET_RPS / (1000 / BATCH_INTERVAL_MS)); // 500 jobs per batch

console.log(`=======================================================`);
console.log(` KRONOS HIGH-THROUGHPUT LOAD GENERATOR ACTIVE`);
console.log(` Target Rate: ${TARGET_RPS.toLocaleString()} jobs/second`);
console.log(` Batch Configuration: ${BATCH_SIZE} jobs per request every ${BATCH_INTERVAL_MS}ms`);
console.log(` Connecting to: ${SERVER_URL}`);
console.log(`=======================================================`);

let totalGenerated = 0;
let totalRequestsSent = 0;
let totalRequestsFailed = 0;

// High-performance HTTP Keep-Alive agent to reuse TCP sockets
const agent = new http.Agent({
  keepAlive: true,
  maxSockets: 100,
  maxFreeSockets: 50,
  timeout: 5000
});

/**
 * Sends a micro-batch of jobs to the ingestion server
 */
function sendBatch() {
  const jobsBatch = Array.from({ length: BATCH_SIZE }, () => ({
    type: 'transaction',
    data: {
      amount: Math.round(Math.random() * 1000),
      currency: 'USD',
      merchant: 'Stripe_Gateway_Sim',
      timestamp: Date.now()
    }
  }));

  const payload = JSON.stringify({ jobs: jobsBatch });
  
  const options = {
    hostname: 'localhost',
    port: 3000,
    path: '/api/jobs/batch',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(payload)
    },
    agent: agent
  };

  const req = http.request(options, (res) => {
    totalRequestsSent++;
    
    // Drain response stream to allow socket reuse
    res.resume();
    
    if (res.statusCode === 202) {
      totalGenerated += BATCH_SIZE;
    } else {
      totalRequestsFailed++;
    }
  });

  req.on('error', (err) => {
    totalRequestsFailed++;
    // Silently handle socket closes or timeouts during extreme load
  });

  // Write payload and close request stream
  req.write(payload);
  req.end();
}

// Log status report every 1 second
let lastReportTime = Date.now();
let lastTotalGenerated = 0;

const reportTimer = setInterval(() => {
  const now = Date.now();
  const timeDiffSec = (now - lastReportTime) / 1000;
  
  if (timeDiffSec >= 0.95) {
    const deltaGenerated = totalGenerated - lastTotalGenerated;
    const currentRps = Math.round(deltaGenerated / timeDiffSec);
    
    console.log(`[Telemetry] Sent ${currentRps.toLocaleString()} jobs/sec | Total: ${totalGenerated.toLocaleString()} | Failed Req: ${totalRequestsFailed}`);
    
    lastTotalGenerated = totalGenerated;
    lastReportTime = now;
  }
}, 1000);

// Sustained execution loop
const loadTimer = setInterval(() => {
  sendBatch();
}, BATCH_INTERVAL_MS);

// Handle graceful termination
process.on('SIGINT', () => {
  clearInterval(loadTimer);
  clearInterval(reportTimer);
  agent.destroy();
  console.log(`\n=======================================================`);
  console.log(` BENCHMARK RUN COMPLETED`);
  console.log(` Total Jobs Successfully Submitted: ${totalGenerated.toLocaleString()}`);
  console.log(` Total Failed Request Deliveries: ${totalRequestsFailed}`);
  console.log(`=======================================================`);
  process.exit(0);
});
