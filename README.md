# KRONOS High-Throughput Job Scheduler

KRONOS is a premium, high-performance job scheduling simulation and telemetry dashboard designed for high-throughput backend services. Capable of simulating **10,000+ Requests Per Second (RPS)**, it leverages a highly optimized Node.js stack.

KRONOS supports **dual-mode deployment**:
1. **Local Mode**: Stateful execution with **Fastify**, a custom in-process partition broker, **WebSockets** for real-time telemetry, and **SQLite WAL (Write-Ahead Logging)** mode for buffered bulk database commits.
2. **Serverless (Vercel) Mode**: Stateless execution using **Vercel Serverless Functions**, an **In-Memory database fallback** (no native binary dependencies), and **Server-Sent Events (SSE)** with inline simulation driving to bypass Vercel function freezing.

The front-end dashboard features a professional editorial design inspired by **Claude.ai's warm paper aesthetics**, supporting both a warm light paper mode and a warm charcoal sepia dark mode.

---

## 🚀 Key Features

* **Dual-Mode Transport**: Auto-detects environment to negotiate connection: tries WebSocket first (local) and falls back to SSE (Serverless Vercel).
* **High-Throughput Ingestion**: Handles massive ingestion loads using custom-hashed memory partitions (16 concurrent execution lanes).
* **Simulated Worker Engine**: Fully models asynchronous worker threads with busy states, processing delays, and network jitter/latency.
* **SQLite WAL Architecture (Local)**: Optimizes SQLite write throughput via asynchronous WAL batching, allowing thousands of records to be flushed in bulk without blocking the event loop.
* **Vercel Serverless Optimization**: Drives simulation ticks inline inside active SSE streams to prevent lambda container suspension, and batches in-flight queues to reduce processing overhead.
* **Premium Editorial UI**: Elegant, gradient-free aesthetic designed with typography scale adjustments for professional readability, and persistent dark/light mode preference (`localStorage`).
* **Real-time Charts**: Displays live throughput telemetry, latencies, database size, and partition queue backlogs (updated every 200ms for ultra-responsive rendering).

---

## 🛠️ Tech Stack

* **Backend Framework**: Node.js, Fastify (Local), Vercel Serverless (Production)
* **Real-time Synchronization**: WebSockets (`ws` at 500ms intervals) or Server-Sent Events (`SSE` at 200ms intervals)
* **Database**: `better-sqlite3` in Write-Ahead Logging (`WAL`) mode (Local) or In-Memory mock DB (Vercel)
* **Frontend**: HTML5, Vanilla CSS3 (Custom Properties), Chart.js, Canvas Confetti

---

## ⚡ Getting Started

### Prerequisites
* Node.js (version 18 or higher recommended)
* NPM

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/Antim21/High-Throughput-Job-Scheduler.git
   cd High-Throughput-Job-Scheduler
   ```
2. Install dependencies:
   ```bash
   npm install
   ```

### Running Locally
To launch the stateful KRONOS server locally on port `3000`:
```bash
npm start
```
Open [http://localhost:3000](http://localhost:3000) in your browser.

### Running Benchmarks
To execute the high-throughput CLI load test generator (pushes 10,000+ jobs/sec):
```bash
npm run benchmark
```

---

## ☁️ Deployment

### Option A: Vercel (Recommended for Serverless Demos)
KRONOS is pre-configured to deploy on Vercel with a single command:
```bash
npx vercel --prod
```
* **Performance Enhancements**: To run efficiently on Vercel, the telemetry rate is set to **200ms**, in-flight jobs are grouped into batches for $O(B)$ lookup performance, and all `.shift()` list re-index operations inside worker loops are optimized to `.slice()` to avoid CPU bottlenecks.

### Option B: Render.com Blueprint (Stateful Disk)
A `render.yaml` Blueprint file is included for deploying to Render.com with a persistent SSD:
1. Go to your Render dashboard.
2. Click **New** -> **Blueprint**.
3. Render will scan `render.yaml` and configure a persistent 1GB SSD disk at `/var/data` to host the SQLite database.
