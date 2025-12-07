🛡️ VolGuard 19.0 (Endgame Edition)

**Institutional-Grade Options Trading Bot for Retail** *Built with Python, FastAPI, Upstox API v3, and React-ready Architecture.*

---

## 🚀 Key Features (v19.0)
* **Zero Downtime:** Hot-swap API tokens without restarting the bot.
* **Capital Buckets:** Segregated capital for Weekly, Monthly, and Intraday strategies.
* **Smart Caching:** Instant startup time (<1s) using local instrument cache.
* **Non-Blocking Core:** Heavy math (SABR calibration) runs in background processes.
* **Zombie Recovery:** Auto-adopts orphaned broker positions after a crash.
* **Gamma & Theta Guard:** New risk limits for advanced Greeks.

---

## 🛠️ Installation

### 1. Prerequisites
* Docker & Docker Compose
* Python 3.11+ (for local tools)
* Upstox Account (API Key & Secret)

### 2. Setup
1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/your-repo/volguard-19.git](https://github.com/your-repo/volguard-19.git)
    cd volguard-19
    ```

2.  **Create Directories:**
    ```bash
    mkdir -p data dashboard_data logs
    ```

3.  **Configure Environment:**
    * Copy `.env.example` to `.env`.
    * Run the token generator to get your daily access token:
        ```bash
        python tools/get_token.py
        ```
    * Paste the token into `.env`.

### 3. Launch
```bash
docker-compose up -d --build
