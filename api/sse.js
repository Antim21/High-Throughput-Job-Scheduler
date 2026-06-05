/**
 * Server-Sent Events (SSE) Endpoint for Vercel
 * 
 * Replaces WebSocket for real-time telemetry streaming.
 * Vercel supports SSE via streaming responses with `maxDuration`.
 */

import { buildTelemetrySnapshot } from './_shared/broker.js';

export const config = {
  maxDuration: 60, // Keep SSE alive for up to 60 seconds (Vercel Pro: 300s)
};

export default async function handler(req, res) {
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Cache-Control',
    });
    return res.end();
  }

  // Set SSE headers
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'Access-Control-Allow-Origin': '*',
    'X-Accel-Buffering': 'no', // Disable nginx buffering
  });

  // Send initial comment to establish connection
  res.write(':ok\n\n');

  // Stream telemetry every 500ms
  const intervalId = setInterval(() => {
    try {
      const snapshot = buildTelemetrySnapshot();
      res.write(`data: ${JSON.stringify(snapshot)}\n\n`);
    } catch {
      // Client disconnected or write failed
      clearInterval(intervalId);
    }
  }, 500);

  // Clean up when client disconnects
  req.on('close', () => {
    clearInterval(intervalId);
  });

  req.on('error', () => {
    clearInterval(intervalId);
  });

  // Keep the function alive — don't call res.end()
  // Vercel will terminate after maxDuration
}
