# 🛡️ VolGuard 19.0 (Endgame Edition)

**Institutional-Grade Algorithmic Trading System for Upstox**
*Built with Python 3.11, FastAPI, Docker, and React-Ready Architecture.*

---

## 🚀 System Overview

VolGuard 19.0 is a high-frequency capable options trading bot designed for the Indian markets (NIFTY/BANKNIFTY). Unlike retail bots, it uses an "Endgame" architecture prioritizing safety, capital preservation, and mathematical precision.

### 🌟 Key Capabilities
* **Atomic Batch Execution:** Trades are executed via Upstox v2 Multi-Order API. All legs (e.g., Iron Condor) are filled or rejected together. Zero "orphaned leg" risk.
* **Zero-Downtime Operations:** Hot-swap API tokens at runtime without restarting the engine.
* **Smart Capital Buckets:** Segregates capital into `Weekly`, `Monthly`, and `Intraday` pools to prevent over-allocation.
* **Zombie Recovery:** Automatically detects and "adopts" open positions from the broker that exist outside the database (e.g., after a crash).
* **Advanced Risk Matrix:** Monitors Delta, Vega, Gamma, and Theta limits in real-time. Includes "Panic Flatten" circuit breakers.
* **Hybrid Pricing Engine:** Uses a localized SABR volatility model calibrated to live option chains, cross-referenced with broker Greeks.

---

## 🛠️ Installation & Setup

### 1. Prerequisites
* **Docker Desktop** (or Docker Engine on Linux)
* **Python 3.11+** (for local tooling)
* **Upstox API Credentials** (API Key & Secret)

### 2. Clone & Prepare
```bash
git clone [https://github.com/your-repo/volguard-19.git](https://github.com/your-repo/volguard-19.git)
cd volguard-19

# Create necessary data volumes
mkdir -p data dashboard_data logs
