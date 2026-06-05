/**
 * Vercel Serverless API Handler
 * 
 * Single function that handles all API routes via path matching.
 * Uses shared module-level state from broker.js.
 */

import {
  broker,
  generateFastId,
  handleControl,
  workerHeartbeats,
  queueJobWrite,
  getState,
} from './_shared/broker.js';

function parseBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => (body += chunk));
    req.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch {
        resolve({});
      }
    });
    req.on('error', reject);
  });
}

function sendJSON(res, statusCode, data) {
  res.writeHead(statusCode, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  });
  res.end(JSON.stringify(data));
}

export default async function handler(req, res) {
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    });
    return res.end();
  }

  const url = new URL(req.url, `http://${req.headers.host}`);
  const path = url.pathname;

  try {
    // ─── POST /api/jobs ───
    if (path === '/api/jobs' && req.method === 'POST') {
      const payload = await parseBody(req);
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
        total_latency: 0,
      };

      const success = broker.publish(job);
      if (success) {
        return sendJSON(res, 202, { id: job.id, status: 'queued', partition: job.partition });
      } else {
        return sendJSON(res, 503, { error: 'Message Broker is paused or congested' });
      }
    }

    // ─── POST /api/jobs/batch ───
    if (path === '/api/jobs/batch' && req.method === 'POST') {
      const { jobs: batchJobs } = await parseBody(req) || { jobs: [] };
      const ingestedJobs = [];
      const now = Date.now();

      for (let i = 0; i < (batchJobs || []).length; i++) {
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
          total_latency: 0,
        };
        if (broker.publish(job)) ingestedJobs.push(job.id);
      }

      return sendJSON(res, 202, { count: ingestedJobs.length });
    }

    // ─── POST /api/control ───
    if (path === '/api/control' && req.method === 'POST') {
      const { action, value } = await parseBody(req);
      const result = handleControl(action, value);
      if (result === null) {
        return sendJSON(res, 400, { error: 'Unknown action' });
      }
      return sendJSON(res, 200, result);
    }

    // ─── POST /api/worker/poll ───
    if (path === '/api/worker/poll' && req.method === 'POST') {
      const { workerId, batchSize } = await parseBody(req);
      broker.registerWorker(workerId);

      const state = getState();
      if (state.simulateNetworkLag > 0) {
        await new Promise(resolve => setTimeout(resolve, state.simulateNetworkLag));
      }

      const jobs = broker.poll(workerId, batchSize || 100);
      return sendJSON(res, 200, { jobs });
    }

    // ─── POST /api/worker/commit ───
    if (path === '/api/worker/commit' && req.method === 'POST') {
      const { workerId, jobs } = await parseBody(req) || { jobs: [] };
      const now = Date.now();
      const state = getState();

      for (const job of (jobs || [])) {
        job.status = 'completed';
        job.completed_at = now;
        job.ingest_latency = Math.max(0, job.started_at - job.created_at);
        job.execution_latency = Math.max(0, job.completed_at - job.started_at);
        job.total_latency = Math.max(0, job.completed_at - job.created_at);

        state.latencyHistory.push(job.total_latency);
        if (state.latencyHistory.length > 5000) state.latencyHistory.shift();

        const typeKey = job.type || 'transaction';
        if (state.jobTypeCounters[typeKey] !== undefined) state.jobTypeCounters[typeKey]++;
        else state.jobTypeCounters.transaction++;

        broker.totalProcessed++;
        queueJobWrite(job);
      }

      return sendJSON(res, 200, { success: true });
    }

    // ─── POST /api/worker/heartbeat ───
    if (path === '/api/worker/heartbeat' && req.method === 'POST') {
      const { workerId } = await parseBody(req);
      workerHeartbeats.set(workerId, Date.now());
      broker.registerWorker(workerId);
      const state = getState();
      return sendJSON(res, 200, { success: true, maxWorkerCount: state.maxWorkerCount });
    }

    // ─── 404 ───
    return sendJSON(res, 404, { error: 'Not found' });

  } catch (err) {
    console.error('API error:', err);
    return sendJSON(res, 500, { error: 'Internal server error' });
  }
}
