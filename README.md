VolGuard 19.0 (Endgame Edition)
Defensive Algorithmic Trading Engine for Upstox
Built with Python 3.11, FastAPI, Docker, and Institutional Risk Protocols.
⚡ System Overview
VolGuard 19.0 is a proprietary, risk-first trading engine designed for the Indian Options Market (NIFTY/BANKNIFTY).
Unlike retail bots that blindly follow indicators, VolGuard functions as an autonomous hedge fund. It combines a defensive quantitative core (SABR Volatility Model) with a Generative AI "CIO" that provides narrative risk analysis. The system assumes market data is corrupted and enforces strict "Confidence Scoring" before risking capital.
🏛️ The "Endgame" Architecture
1. Defensive Core (The Shield)
 * Greek Confidence Engine: Calculates a confidence_score (0.0–1.0) for every option tick. If Broker Greeks and internal SABR models diverge > 15%, the system blocks trading to prevent "Silent Risk."
 * WebSocket Circuit Breaker: Prevents IP bans by enforcing a 5-minute cool-down if the data feed fails 5 times in rapid succession.
 * Atomic Batch Execution: Eliminates "Legging Risk." Complex strategies (Iron Condors) are executed via Upstox v2 Multi-Order API—all legs fill or the trade is rolled back.
2. Quantitative Intelligence (The Brain)
 * Hybrid Pricing Engine: Calibrates a live SABR volatility surface every 15 minutes. Uses scipy.optimize to price illiquid OTM options accurately where broker data is stale.
 * Regime Detection: Classifies market state into PANIC, BULL_EXPANSION, FEAR_BACKWARDATION, etc., using VIX, IV Percentile, and Realized Volatility spreads.
 * 0DTE Math Safety: Enforces a MIN_TIME_FLOOR (5 minutes) to prevent Gamma/Theta division-by-zero explosions near market close.
3. The AI "CIO" (The Advisor)
 * Portfolio Architect: An integrated LLM agent (Gemini) that acts as a Passive Risk Manager.
   * Trade Analysis: Reviews every proposed trade context (Spot, VIX, News) and outputs a JSON risk verdict (LOW, HIGH, EXTREME).
   * Holistic Health Check: Periodically reviews the entire portfolio's Delta/Vega exposure against live macro news to suggest hedging.
 * Hallucination Safety: The trading loop does not wait for the AI. The AI runs asynchronously and logs advisory warnings only.
4. Capital & State (The Bank)
 * Smart Buckets: Segregates capital into Weekly, Monthly, and Intraday pools using Database Row-Locking (SELECT FOR UPDATE) to prevent race conditions.
 * Zombie Recovery: On restart, the engine scans the broker for "orphaned" positions not in the DB, "adopts" them, and immediately applies risk management.
🚀 Key Capabilities Summary
| Domain | Component | Description |
|---|---|---|
| Quant | SABR Model | Calibrates volatility smile to price illiquid wings. |
| AI | AI_Architect | "CIO" Agent providing narrative risk commentary via API. |
| Risk | RiskManager | Monitors Portfolio Delta, Vega, and PnL. Triggers Panic Flatten if limits breached. |
| Safety | GreekValidator | Prevents trading if Data Confidence < 0.5 (Anti-Corruption). |
| Infra | LiveDataFeed | Includes Circuit Breaker to prevent "Death Spiral" reconnections. |
| State | Engine | Self-healing Zombie Recovery logic for crash resilience. |
🛠️ Installation & Setup
1. Prerequisites
 * Docker Desktop (or Docker Engine on Linux)
 * Python 3.11+ (for local tooling/testing)
 * Upstox API Credentials (API Key & Secret)
 * Google Gemini API Key (for AI CIO features)
2. Clone & Prepare
git clone https://github.com/your-repo/volguard-19.git
cd volguard-19

# Create persistent volume directories
mkdir -p data dashboard_data logs

3. Configuration
Copy the example environment file.
cp .env.example .env

Critical .env Settings:
# Trading Limits
SAFETY_MODE=paper           # 'paper' or 'live'
ACCOUNT_SIZE=2000000.0      # Total Capital (INR)
DAILY_LOSS_LIMIT_PCT=0.03   # Hard Stop at 3% Drawdown

# AI Configuration
GEMINI_API_KEY=your_key_here

# Hardware Tuning
MAX_WORKERS=2               # Thread pool size for SABR math

4. Authentication (The Morning Ritual)
Upstox tokens expire daily. Use the included tool to fetch a new one.
pip install requests python-dotenv
python tools/get_token.py

Paste the generated Access Token into your .env file under UPSTOX_ACCESS_TOKEN.
🚦 Operational Safety Levels
Do not skip steps. Follow this hierarchy to ensure system integrity.
Level 1: Chaos Verification (Mandatory)
Run the Chaos Test Suite. This simulates a "Flash Crash" with corrupt data to verify the Risk Manager holds the line.
python run_tests.py
# Must pass: tests/test_chaos_risk.py (Silent Risk & Circuit Breaker)

Level 2: Connectivity Check
Verify connection to Upstox API and Market Data feeds.
python tools/test_live_connection.py
# Output must be: "CONNECTIVITY VERIFIED"

Level 3: Deployment
Launch the full engine in Docker containers.
docker-compose up -d --build

📊 API & Control Plane
The system exposes a REST API on Port 8000.
🤖 AI & Insights
| Method | Endpoint | Description |
|---|---|---|
| GET | /api/cio/commentary | The AI "CIO" Feed. Returns the latest trade risk analysis and portfolio health narrative. |
| GET | /api/dashboard/data | Real-time JSON feed (Spot, PnL, Trades, Greeks). |
⚙️ Engine Control
| Method | Endpoint | Description |
|---|---|---|
| GET | /health | Kubernetes-style health probe. |
| POST | /api/start | Begins the main trading loop. |
| POST | /api/stop | Gracefully stops the loop (positions remain open). |
| POST | /api/token/refresh | Zero-Downtime token push (Hot-Swap). |
🚨 Emergency
| Method | Endpoint | Description |
|---|---|---|
| POST | /api/emergency/flatten | PANIC BUTTON: Market closes all positions instantly. |
Access Points:
 * API Docs (Swagger): http://localhost:8000/docs
 * Prometheus Metrics: http://localhost:9090
 * Grafana Dashboards: http://localhost:3000 (Default: admin/admin)
⚠️ Troubleshooting & Edge Cases
1. "Greek Confidence Low" Warning
 * Cause: Broker IV and SABR Model disagree significantly (likely due to illiquidity or bad ticks).
 * Behavior: The Risk Manager effectively "freezes" new trades. This is intentional safety behavior.
2. "Circuit Breaker Activated"
 * Cause: WebSocket failed 5 times in rapid succession.
 * Behavior: The feed enters a 5-minute cool-down to prevent IP bans. Do not restart the bot; let it auto-recover.
3. "SABR Calibration Failed"
 * Cause: Not enough liquid strikes in the option chain (common off-hours).
 * Behavior: Engine falls back to standard Black-Scholes IV. Safe to ignore if market is closed.
📜 Disclaimer
This software is for educational and proprietary research purposes. Algorithmic trading involves significant financial risk. The authors are not responsible for any financial losses incurred. Use SAFETY_MODE=paper until you have verified performance over 3 months.
Status: READY_FOR_DEPLOYMENT
Architecture: DEFENSIVE_PROP_GRADE
