# KRONOS High-Throughput Job Scheduler

KRONOS is a premium, high-performance job scheduling simulation and telemetry dashboard designed for high-throughput backend services. Capable of simulating **10,000+ Requests Per Second (RPS)**, it leverages a highly optimized Node.js stack using **Fastify**, custom in-memory partitioned queues, multi-threaded background workers, and **SQLite WAL (Write-Ahead Logging)** mode for lightning-fast database writes.

The front-end dashboard features a professional editorial design inspired by **Claude.ai's warm paper aesthetics**, supporting both a warm light paper mode and a warm charcoal sepia dark mode.

---

## 🚀 Key Features

* **High-Throughput Ingestion**: Powered by Fastify and an in-memory batch-buffering queue capable of handling massive ingest rates.
* **SQLite WAL Architecture**: Optimized batch-transaction writes (~100ms commit flushes) scaling SQLite write rates to extreme levels without event-loop lag.
* **Interactive Sandbox Controls**: Real-time SLA threshold configurators, worker pool scaling, partition visual flowcharts, and load generators.
* **Premium Claude-Style UI**: Elegant, gradient-free aesthetic designed with typography scale adjustments for professional readability, and persistent dark/light mode preference (`localStorage`).
* **Telemetry Visuals**: Real-time charts powered by Chart.js showcasing ingestion rates, execution latencies, database size, and partition queue backlog.

---

## 🛠️ Tech Stack

* **Backend**: Node.js, Fastify (High-speed web framework), `@fastify/static`
* **Real-time Synchronization**: `ws` (WebSockets) for continuous 500ms server-to-client telemetry streams
* **Database**: `better-sqlite3` operating in Write-Ahead Logging (`WAL`) mode with optimized transaction cache sizing
* **Frontend**: Vanilla HTML5, CSS3 Custom Properties, Chart.js

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
To launch the KRONOS Server locally on port `3000`:
```bash
npm start
```
Open [http://localhost:3000](http://localhost:3000) in your browser.

### Running Benchmarks
To execute the high-throughput benchmark CLI simulator script:
```bash
npm run benchmark
```

---

## ☁️ Production Deployment

KRONOS is configured for stateful hosting environments. A `render.yaml` Blueprint file is included for instant, zero-configuration deployments on **Render.com**.

### Option A: Render 1-Click Blueprint (Stateful Disk)
1. Go to your Render dashboard.
2. Click **New** -> **Blueprint**.
3. Render will scan `render.yaml`, set up your Node service, and mount a persistent 1GB SSD disk at `/var/data` to house the SQLite WAL database.

### Option B: Render Web Service (Free Tier)
1. Click **New** -> **Web Service** on Render.
2. Select this repository.
3. Configure the following parameters:
   * **Language**: `Node`
   * **Build Command**: `npm install`
   * **Start Command**: `npm start`
   * **Instance Type**: `Free`
4. Deploy! (Note: The database runs on the container's standard filesystem and will reset when the instance redeploys or sleeps).
