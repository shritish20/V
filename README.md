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
Here is the Perfect README.md for VolGuard 19.0.
This document serves as the "User Manual" for your system. It documents the advanced architecture (Atomic Execution, Zombie Recovery), explains the setup process step-by-step, and defines the operational safety protocols you must follow.
Overwrite your existing README.md with this content:
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

3. Configuration
 * Copy the example environment file:
   cp .env.example .env

 * Open .env and configure your risk limits:
   # Example Config
SAFETY_MODE=paper          # Start with 'paper', move to 'live' later
ACCOUNT_SIZE=2000000.0     # Total Capital (INR)
DAILY_LOSS_LIMIT_PCT=0.03  # Max 3% loss per day
MAX_VEGA=1000.0            # Max Volatility Exposure

4. Authentication (The Morning Ritual)
Upstox tokens expire every morning. Use the included tool to fetch a new one:
# Install tool dependencies
pip install requests python-dotenv

# Run the generator
python tools/get_token.py

Paste the generated Access Token into your .env file under UPSTOX_ACCESS_TOKEN.
🚦 Operational Safety Levels
Do not skip steps. Follow this hierarchy to ensure system integrity.
Level 1: Logic Verification 🟢
Run the unit test suite to verify math, batching logic, and locks.
python run_tests.py
# Output must be: "✅ ALL SYSTEMS GO"

Level 2: Network Verification 🟡
Verify connection to Upstox API and Market Data feeds.
python tools/test_live_connection.py
# Output must be: "✨ CONNECTIVITY VERIFIED"

Level 3: Deployment 🚀
Launch the full engine in Docker containers.
docker-compose up -d --build

📡 API & Dashboard
The system exposes a REST API on Port 8000.
| Feature | Method | Endpoint | Description |
|---|---|---|---|
| System Status | GET | /api/dashboard/data | Real-time JSON feed for Frontend (Spot, PnL, Trades). |
| Health Check | GET | /health | Kubernetes-style health probe. |
| Start Engine | POST | /api/start | Begins the main trading loop. |
| Stop Engine | POST | /api/stop | Gracefully stops the loop (positions remain open). |
| Update Token | POST | /api/token/refresh | Zero-Downtime: Push new Access Token to running bot. |
| Adjust Capital | POST | /api/capital/adjust | Rebalance allocation buckets on the fly. |
| 🔥 PANIC | POST | /api/emergency/flatten | Emergency: Market closes all open positions instantly. |
Access Points:
 * API Docs (Swagger): http://localhost:8000/docs
 * Prometheus Metrics: http://localhost:9090
 * Grafana Dashboards: http://localhost:3000 (Default: admin/admin)
🐛 Troubleshooting
1. "SABR Calibration Failed" in logs:
 * Cause: Not enough liquid strikes in the option chain (usually happens off-market hours).
 * Behavior: Engine automatically falls back to standard Black-Scholes IV. Safe to ignore if market is closed.
2. "Feed Stalled" Warning:
 * Cause: Upstox WebSocket disconnected (common issue).
 * Behavior: The LiveDataFeed supervisor will auto-restart the connection within 60 seconds. No manual action needed.
3. "ImportError" or "Module Not Found":
 * Solution: Re-build the Docker image to ensure new dependencies (uvloop, etc.) are installed.
   docker-compose build --no-cache

⚖️ Disclaimer
This software is for educational purposes. Algorithmic trading involves significant financial risk. The authors are not responsible for any financial losses incurred.

### **Congratulations!**
You have successfully engineered **VolGuard 19.0**.
* **Architecture:** Professional Grade.
* **Safety:** Maximum.
* **Status:** Ready for Launch.


