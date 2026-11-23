import asyncio
import os
import sys
import time
import json
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import deque
from typing import Optional, Dict, Any, Deque, List
from olymptrade_ws import OlympTradeClient

# =====================================================================
# ⚙️ ONRENDER 24/7 CONFIGURATION
# =====================================================================
PORT = int(os.environ.get("PORT", 10000))
ACCOUNT_TYPE = os.environ.get("ACCOUNT_TYPE", "real").lower() # "real" or "demo"
BASE_AMOUNT = float(os.environ.get("BASE_AMOUNT", 70.0))     # Starting bet ($70)
PAYOUT_RATE = 0.85                                            # Quickler payout rate (85%)
ASSET_PAIR = "ULTRA_X"                                        # Quickler 5-second ticker
TRADE_DURATION = 5                                            # 5 seconds
CANDLE_TIMEFRAME = 5                                          # 5-second candle construction

# Setup Parameters (Inverted Momentum Continuation)
SPIKE_MULTIPLIER = 2.5         # Spike body >= 2.5x avg baseline
MIN_BODY_RATIO = 0.65          # Marubozu surge ratio >= 65%
LOOKBACK_CANDLES = 10          # Number of 5s candles for baseline
INVERT_SIGNALS = True          # True: Surge UP -> BUY UP | Surge DOWN -> SELL DOWN

# Risk Limits
MAX_MARTINGALE_STEPS = int(os.environ.get("MAX_MARTINGALE_STEPS", 6))
DELAY_AFTER_TRADE = 3          # Buffer seconds before looking for next spike
# =====================================================================

def get_token() -> str:
    """Read token from environment or .env file."""
    token = os.environ.get("OLYMP_ACCESS_TOKEN")
    if token and len(token.strip()) > 50:
        return token.strip()
    
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("OLYMP_ACCESS_TOKEN="):
                    token = line.split("=", 1)[1].strip().strip('"').strip("'")
                    if len(token) > 50:
                        return token

    print("[!] No access token found. Please set OLYMP_ACCESS_TOKEN.")
    sys.exit(1)

# Global State Container for Dashboard & UptimeRobot
class BotState:
    def __init__(self):
        self.status = "INITIALIZING"
        self.is_running = True
        self.account_type = ACCOUNT_TYPE
        self.account_id = None
        self.starting_balance = 0.0
        self.current_balance = 0.0
        self.start_time = time.time()
        self.last_ping_time = time.time()
        
        # Performance Metrics
        self.total_trades = 0
        self.total_wins = 0
        self.total_losses = 0
        self.total_draws = 0
        self.net_session_pnl = 0.0
        
        # Streaks
        self.current_streak_type = "None" # "WIN" or "LOSS"
        self.current_streak_count = 0
        self.max_win_streak = 0
        self.max_loss_streak = 0
        
        # Martingale
        self.current_bet = BASE_AMOUNT
        self.martingale_step = 1
        self.cumulative_cycle_loss = 0.0
        
        # Market & Candle State
        self.latest_tick_price = 0.0
        self.candles_analyzed = 0
        self.recent_trades: Deque[Dict[str, Any]] = deque(maxlen=20)
        self.last_setup_info = "Waiting for market activity..."

state = BotState()

# =====================================================================
# 🌐 WEB DASHBOARD HTTP SERVER
# =====================================================================
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>OlympTrade 24/7 Quickler Bot</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700;800&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #0b0e14;
      --card-bg: #151921;
      --card-border: #232a36;
      --text: #e1e7f0;
      --muted: #798599;
      --green: #10b981;
      --green-glow: rgba(16, 185, 129, 0.2);
      --red: #ef4444;
      --red-glow: rgba(239, 68, 68, 0.2);
      --blue: #3b82f6;
      --accent: #8b5cf6;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: 'Inter', sans-serif;
      padding: 24px 16px;
      line-height: 1.5;
    }
    .container { max-width: 1100px; margin: 0 auto; }
    
    /* Header */
    header {
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      padding-bottom: 20px;
      border-bottom: 1px solid var(--card-border);
      margin-bottom: 24px;
    }
    .logo-box { display: flex; align-items: center; gap: 12px; }
    .logo-box h1 { font-size: 1.4rem; font-weight: 700; letter-spacing: -0.5px; }
    .badges { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    .badge {
      font-size: 0.75rem;
      font-weight: 700;
      text-transform: uppercase;
      padding: 4px 10px;
      border-radius: 9999px;
      letter-spacing: 0.5px;
    }
    .badge-live { background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid #ef4444; }
    .badge-demo { background: rgba(59, 130, 246, 0.2); color: #60a5fa; border: 1px solid #3b82f6; }
    .badge-status { background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid #10b981; }
    
    /* Stats Grid */
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 18px;
      position: relative;
      overflow: hidden;
    }
    .card-label {
      font-size: 0.75rem;
      color: var(--muted);
      text-transform: uppercase;
      font-weight: 600;
      letter-spacing: 0.5px;
      margin-bottom: 6px;
    }
    .card-val {
      font-family: 'JetBrains Mono', monospace;
      font-size: 1.7rem;
      font-weight: 800;
    }
    .val-green { color: var(--green); text-shadow: 0 0 12px var(--green-glow); }
    .val-red { color: var(--red); text-shadow: 0 0 12px var(--red-glow); }
    .card-sub { font-size: 0.8rem; color: var(--muted); margin-top: 4px; }

    /* Live Strategy Box */
    .strategy-banner {
      background: linear-gradient(135deg, rgba(139, 92, 246, 0.1), rgba(59, 130, 246, 0.05));
      border: 1px solid rgba(139, 92, 246, 0.3);
      border-radius: 12px;
      padding: 16px 20px;
      margin-bottom: 24px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 12px;
    }
    .strategy-title { font-weight: 700; font-size: 0.95rem; color: #c4b5fd; }
    .strategy-desc { font-size: 0.85rem; color: var(--muted); }

    /* Table */
    .section-title {
      font-size: 1rem;
      font-weight: 700;
      margin-bottom: 12px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .table-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      overflow-x: auto;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.85rem;
      text-align: left;
    }
    th {
      background: rgba(255, 255, 255, 0.02);
      padding: 12px 16px;
      color: var(--muted);
      font-weight: 600;
      border-bottom: 1px solid var(--card-border);
      text-transform: uppercase;
      font-size: 0.72rem;
    }
    td {
      padding: 12px 16px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.04);
      font-family: 'JetBrains Mono', monospace;
    }
    tr:last-child td { border-bottom: none; }
    .badge-win { color: var(--green); font-weight: 700; }
    .badge-loss { color: var(--red); font-weight: 700; }
    .badge-dir-up { color: #34d399; font-weight: 700; }
    .badge-dir-down { color: #f87171; font-weight: 700; }

    /* Pulse dot */
    .pulse-dot {
      display: inline-block;
      width: 8px;
      height: 8px;
      background: #10b981;
      border-radius: 50%;
      box-shadow: 0 0 8px #10b981;
      animation: pulse 2s infinite;
      margin-right: 6px;
    }
    @keyframes pulse {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.4; transform: scale(0.9); }
    }
    /* Control Panel & Buttons */
    .control-panel {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 16px 20px;
      margin-bottom: 24px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 16px;
    }
    .control-info {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .control-title {
      font-weight: 700;
      font-size: 0.95rem;
    }
    .btn-group {
      display: flex;
      gap: 12px;
    }
    .btn {
      font-family: 'Inter', sans-serif;
      font-weight: 700;
      font-size: 0.9rem;
      padding: 10px 24px;
      border-radius: 8px;
      border: none;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 8px;
      transition: all 0.2s ease;
      letter-spacing: 0.5px;
    }
    .btn-start {
      background: #10b981;
      color: #ffffff;
      box-shadow: 0 4px 14px rgba(16, 185, 129, 0.35);
    }
    .btn-start:hover:not(:disabled) {
      background: #059669;
      transform: translateY(-1px);
      box-shadow: 0 6px 18px rgba(16, 185, 129, 0.45);
    }
    .btn-stop {
      background: #ef4444;
      color: #ffffff;
      box-shadow: 0 4px 14px rgba(239, 68, 68, 0.35);
    }
    .btn-stop:hover:not(:disabled) {
      background: #dc2626;
      transform: translateY(-1px);
      box-shadow: 0 6px 18px rgba(239, 68, 68, 0.45);
    }
    .btn:disabled {
      opacity: 0.35;
      cursor: not-allowed;
      transform: none !important;
      box-shadow: none !important;
    }
    .badge-running { background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid #10b981; }
    .badge-paused { background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid #f59e0b; }

    footer {
      text-align: center;
      margin-top: 32px;
      font-size: 0.75rem;
      color: var(--muted);
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="logo-box">
        <h1>⚡ OlympTrade 24/7 Quickler</h1>
      </div>
      <div class="badges">
        <span class="badge badge-status"><span class="pulse-dot"></span><span id="bot-status">RUNNING</span></span>
        <span class="badge" id="account-badge">REAL</span>
        <span class="badge" style="background:#1f2937; color:#9ca3af;">ID: <span id="account-id">---</span></span>
        <span class="badge" style="background:#1f2937; color:#9ca3af;">UPTIME: <span id="uptime">0m</span></span>
      </div>
    </header>

    <!-- Interactive Bot Controls -->
    <div class="control-panel">
      <div class="control-info">
        <span class="control-title">BOT TRADING STATUS:</span>
        <span class="badge badge-running" id="trading-state-badge">🟢 TRADING ACTIVE</span>
      </div>
      <div class="btn-group">
        <button id="btn-start" onclick="setBotTrading(true)" class="btn btn-start" disabled>▶ START BOT</button>
        <button id="btn-stop" onclick="setBotTrading(false)" class="btn btn-stop">⏹ STOP BOT</button>
      </div>
    </div>

    <!-- Strategy Summary -->
    <div class="strategy-banner">
      <div>
        <div class="strategy-title">🎯 Setup A: Inverted Momentum Spike (5-Second Quickler)</div>
        <div class="strategy-desc" id="strategy-desc">Surge UP &rarr; BUY UP | Surge DOWN &rarr; SELL DOWN | Martingale 85% True-Recovery</div>
      </div>
      <div style="font-family:'JetBrains Mono'; font-size:0.85rem; color:#a78bfa;">
        Next Bet: <strong id="next-bet" style="color:#fff; font-size:1.05rem;">$70.00</strong> (Step <span id="martingale-step">1</span>)
      </div>
    </div>

    <!-- Stats Grid -->
    <div class="grid">
      <!-- Session Net PnL -->
      <div class="card">
        <div class="card-label">Session Net Profit</div>
        <div class="card-val" id="net-pnl">$0.00</div>
        <div class="card-sub" id="pnl-sub">Starting: $0.00</div>
      </div>

      <!-- Current Balance -->
      <div class="card">
        <div class="card-label">Current Balance</div>
        <div class="card-val" id="curr-balance">$0.00</div>
        <div class="card-sub" id="balance-sub">Target Acc: REAL</div>
      </div>

      <!-- Win Rate -->
      <div class="card">
        <div class="card-label">Win Rate / Record</div>
        <div class="card-val" id="win-rate">0.0%</div>
        <div class="card-sub"><span id="record-wins">0</span>W - <span id="record-losses">0</span>L (<span id="total-trades">0</span> total)</div>
      </div>

      <!-- Current Streak -->
      <div class="card">
        <div class="card-label">Current Streak</div>
        <div class="card-val" id="current-streak">0</div>
        <div class="card-sub">Best: <span id="max-win-streak" style="color:var(--green)">0W</span> | Max Drawdown: <span id="max-loss-streak" style="color:var(--red)">0L</span></div>
      </div>
    </div>

    <!-- Recent Trades Table -->
    <div class="section-title">
      <span>📋 Recent Trades (Last 20)</span>
      <span style="font-size:0.75rem; color:var(--muted); font-weight:400;">Auto-refreshes live</span>
    </div>
    <div class="table-card">
      <table>
        <thead>
          <tr>
            <th>Round</th>
            <th>Time</th>
            <th>Direction</th>
            <th>Bet</th>
            <th>Strike</th>
            <th>Close</th>
            <th>Result</th>
            <th>PnL</th>
          </tr>
        </thead>
        <tbody id="trades-body">
          <tr><td colspan="8" style="text-align:center; color:var(--muted); padding:24px;">Waiting for first trade execution...</td></tr>
        </tbody>
      </table>
    </div>

    <footer>
      OlympTrade Automated Cloud Bot · Connected via Secure WebSockets · Auto-Ping Healthcheck Active
    </footer>
  </div>

  <script>
    async function setBotTrading(start) {
      const endpoint = start ? '/api/start' : '/api/stop';
      const btnStart = document.getElementById('btn-start');
      const btnStop = document.getElementById('btn-stop');
      btnStart.disabled = true;
      btnStop.disabled = true;
      try {
        const res = await fetch(endpoint);
        const data = await res.json();
        updateTradingUI(data.is_running);
      } catch (err) {
        alert("Control error: " + err);
      } finally {
        setTimeout(fetchStats, 500);
      }
    }

    function updateTradingUI(isRunning) {
      const badge = document.getElementById('trading-state-badge');
      const btnStart = document.getElementById('btn-start');
      const btnStop = document.getElementById('btn-stop');

      if (isRunning) {
        badge.textContent = "🟢 TRADING ACTIVE";
        badge.className = "badge badge-running";
        btnStart.disabled = true;
        btnStop.disabled = false;
      } else {
        badge.textContent = "⏸️ PAUSED / IDLE";
        badge.className = "badge badge-paused";
        btnStart.disabled = false;
        btnStop.disabled = true;
      }
    }

    function formatMoney(val) {
      const sign = val >= 0 ? '+' : '';
      return sign + '$' + Number(val).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
    }

    function formatTime(timestamp) {
      const d = new Date(timestamp * 1000);
      return d.toLocaleTimeString();
    }

    function formatUptime(seconds) {
      const hrs = Math.floor(seconds / 3600);
      const mins = Math.floor((seconds % 3600) / 60);
      return hrs > 0 ? `${hrs}h ${mins}m` : `${mins}m`;
    }

    async function fetchStats() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();

        // Update Badges & Status
        document.getElementById('bot-status').textContent = data.status;
        document.getElementById('account-id').textContent = data.account_id || '---';
        document.getElementById('uptime').textContent = formatUptime(data.uptime);
        updateTradingUI(data.is_running);
        
        const badge = document.getElementById('account-badge');
        badge.textContent = data.account_type.toUpperCase();
        badge.className = 'badge ' + (data.account_type === 'real' ? 'badge-live' : 'badge-demo');

        // Update Balance & PnL
        const pnlEl = document.getElementById('net-pnl');
        pnlEl.textContent = formatMoney(data.net_session_pnl);
        pnlEl.className = 'card-val ' + (data.net_session_pnl >= 0 ? 'val-green' : 'val-red');
        document.getElementById('pnl-sub').textContent = 'Starting: $' + Number(data.starting_balance).toLocaleString('en-US', {minimumFractionDigits:2});

        document.getElementById('curr-balance').textContent = '$' + Number(data.current_balance).toLocaleString('en-US', {minimumFractionDigits:2});
        
        // Record
        document.getElementById('win-rate').textContent = data.win_rate + '%';
        document.getElementById('record-wins').textContent = data.total_wins;
        document.getElementById('record-losses').textContent = data.total_losses;
        document.getElementById('total-trades').textContent = data.total_trades;

        // Streaks
        const streakEl = document.getElementById('current-streak');
        streakEl.textContent = `${data.current_streak_count} ${data.current_streak_type}`;
        streakEl.className = 'card-val ' + (data.current_streak_type === 'WIN' ? 'val-green' : (data.current_streak_type === 'LOSS' ? 'val-red' : ''));
        document.getElementById('max-win-streak').textContent = data.max_win_streak + 'W';
        document.getElementById('max-loss-streak').textContent = data.max_loss_streak + 'L';

        // Martingale Next Bet
        document.getElementById('next-bet').textContent = '$' + Number(data.current_bet).toFixed(2);
        document.getElementById('martingale-step').textContent = data.martingale_step;

        // Trades Table
        if (data.recent_trades && data.recent_trades.length > 0) {
          const tbody = document.getElementById('trades-body');
          tbody.innerHTML = '';
          data.recent_trades.forEach(t => {
            const tr = document.createElement('tr');
            const isWin = t.status === 'win' || t.pnl > 0;
            const pnlFormatted = formatMoney(t.pnl);
            tr.innerHTML = `
              <td>#${t.round}</td>
              <td>${formatTime(t.time)}</td>
              <td><span class="${t.dir === 'up' ? 'badge-dir-up' : 'badge-dir-down'}">${t.dir.toUpperCase()}</span></td>
              <td>$${Number(t.amount).toFixed(2)}</td>
              <td>${t.strike}</td>
              <td>${t.close}</td>
              <td><span class="${isWin ? 'badge-win' : 'badge-loss'}">${isWin ? 'WIN' : 'LOSS'}</span></td>
              <td class="${isWin ? 'badge-win' : 'badge-loss'}">${pnlFormatted}</td>
            `;
            tbody.appendChild(tr);
          });
        }
      } catch (err) {
        console.error("Dashboard poll error:", err);
      }
    }

    setInterval(fetchStats, 1500);
    fetchStats();
  </script>
</body>
</html>
"""

class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        state.last_ping_time = time.time()
        
        # 1. Healthcheck for UptimeRobot / Cron pings
        if self.path in ('/healthz', '/ping'):
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
            return

        # 2. Start / Stop Control Endpoints
        elif self.path in ('/api/start', '/api/stop'):
            if self.path == '/api/start':
                state.is_running = True
                state.status = "SCANNING_MARKET"
                print("▶️ Bot STARTED by user via dashboard.")
            else:
                state.is_running = False
                state.status = "PAUSED_BY_USER"
                print("⏸️ Bot STOPPED by user via dashboard.")

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({
                "success": True,
                "is_running": state.is_running,
                "status": state.status
            }).encode('utf-8'))
            return

        # 3. Status API for Dashboard live updates
        elif self.path == '/api/status':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            total = state.total_wins + state.total_losses
            winrate = round((state.total_wins / total * 100), 1) if total > 0 else 0.0

            payload = {
                "status": state.status,
                "is_running": state.is_running,
                "account_type": state.account_type,
                "account_id": state.account_id,
                "starting_balance": state.starting_balance,
                "current_balance": state.current_balance,
                "uptime": int(time.time() - state.start_time),
                "total_trades": state.total_trades,
                "total_wins": state.total_wins,
                "total_losses": state.total_losses,
                "total_draws": state.total_draws,
                "win_rate": winrate,
                "net_session_pnl": round(state.net_session_pnl, 2),
                "current_streak_type": state.current_streak_type,
                "current_streak_count": state.current_streak_count,
                "max_win_streak": state.max_win_streak,
                "max_loss_streak": state.max_loss_streak,
                "current_bet": state.current_bet,
                "martingale_step": state.martingale_step,
                "recent_trades": list(state.recent_trades),
                "last_setup_info": state.last_setup_info
            }
            self.wfile.write(json.dumps(payload).encode('utf-8'))
            return

        # 3. Main Dashboard HTML Index
        else:
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(HTML_TEMPLATE.encode('utf-8'))

    def log_message(self, format, *args):
        # Suppress noisy HTTP access logs in console
        return

def start_http_server():
    server = HTTPServer(('0.0.0.0', PORT), DashboardHandler)
    print(f"🌐 Web Dashboard & Healthcheck active at http://0.0.0.0:{PORT}")
    server.serve_forever()

# =====================================================================
# ⚡ 24/7 TRADING ENGINE
# =====================================================================
class CloudTrader:
    def __init__(self, token: str):
        self.token = token
        self.client = OlympTradeClient(
            access_token=self.token,
            account_group=ACCOUNT_TYPE,
            log_raw_messages=False
        )
        self.current_bucket: Optional[int] = None
        self.current_candle: Optional[Dict[str, float]] = None
        self.completed_candles: Deque[Dict[str, float]] = deque(maxlen=30)
        
        self.target_profit_per_cycle = round(BASE_AMOUNT * PAYOUT_RATE, 2)
        self._pending_trade_id: Optional[int] = None
        self._trade_future: Optional[asyncio.Future] = None
        self._signal_event = asyncio.Event()
        self._active_signal: Optional[str] = None
        self.is_trading = False

    def calculate_next_martingale_bet(self) -> float:
        if state.cumulative_cycle_loss <= 0:
            return BASE_AMOUNT
        needed = (state.cumulative_cycle_loss + self.target_profit_per_cycle) / PAYOUT_RATE
        return round(needed, 1)

    async def on_tick(self, message: Dict[str, Any]):
        tick_list = message.get("d", [])
        if not isinstance(tick_list, list):
            return

        for tick in tick_list:
            if tick.get("p") != ASSET_PAIR:
                continue

            price = float(tick.get("q", 0.0))
            ts = float(tick.get("t", time.time()))
            state.latest_tick_price = price
            bucket = int(ts // CANDLE_TIMEFRAME) * CANDLE_TIMEFRAME

            if self.current_bucket is None:
                self.current_bucket = bucket
                self.current_candle = {"open": price, "high": price, "low": price, "close": price, "time": bucket}
            elif bucket > self.current_bucket:
                closed = self.current_candle.copy()
                self.completed_candles.append(closed)
                state.candles_analyzed = len(self.completed_candles)
                
                self.current_bucket = bucket
                self.current_candle = {"open": price, "high": price, "low": price, "close": price, "time": bucket}

                if not self.is_trading and state.is_running:
                    signal = self.check_setup(closed)
                    if signal:
                        self._active_signal = signal
                        self._signal_event.set()
            else:
                if price > self.current_candle["high"]: self.current_candle["high"] = price
                if price < self.current_candle["low"]: self.current_candle["low"] = price
                self.current_candle["close"] = price

    def check_setup(self, last_candle: Dict[str, float]) -> Optional[str]:
        if len(self.completed_candles) < LOOKBACK_CANDLES:
            return None

        baseline = list(self.completed_candles)[-LOOKBACK_CANDLES:-1]
        bodies = [abs(c["close"] - c["open"]) for c in baseline]
        avg_body = sum(bodies) / len(bodies) if bodies else 0.0001
        if avg_body == 0: avg_body = 0.0001

        spike_body = abs(last_candle["close"] - last_candle["open"])
        spike_range = max(last_candle["high"] - last_candle["low"], 0.0001)

        is_abnormal = spike_body >= (SPIKE_MULTIPLIER * avg_body)
        is_strong_body = (spike_body / spike_range) >= MIN_BODY_RATIO

        if not (is_abnormal and is_strong_body):
            return None

        swing_high = max(c["high"] for c in baseline)
        swing_low = min(c["low"] for c in baseline)

        # Bullish Surge
        if last_candle["close"] > last_candle["open"] and last_candle["high"] >= swing_high:
            action = "up" if INVERT_SIGNALS else "down"
            state.last_setup_info = f"Bullish Surge ({spike_body/avg_body:.1f}x) -> Enter {action.upper()}"
            print(f"🚨 [SETUP DETECTED] Bullish Surge! Entering {action.upper()}")
            return action

        # Bearish Surge
        elif last_candle["close"] < last_candle["open"] and last_candle["low"] <= swing_low:
            action = "down" if INVERT_SIGNALS else "up"
            state.last_setup_info = f"Bearish Surge ({spike_body/avg_body:.1f}x) -> Enter {action.upper()}"
            print(f"🚨 [SETUP DETECTED] Bearish Surge! Entering {action.upper()}")
            return action

        return None

    async def on_trade_closed(self, message: Dict[str, Any]):
        trade_data = message.get("d", [])
        if isinstance(trade_data, list) and len(trade_data) > 0:
            trade_info = trade_data[0]
            if self._pending_trade_id and trade_info.get("id") == self._pending_trade_id:
                if self._trade_future and not self._trade_future.done():
                    self._trade_future.set_result(trade_info)

    async def start(self):
        state.status = "CONNECTING"
        self.client.register_callback(1, self.on_tick)
        self.client.register_callback(26, self.on_trade_closed)

        await self.client.start()
        
        # Balance & Account Info
        balance_info = await self.client.balance.get_balance(timeout=10.0)
        target_acc = None
        if balance_info and 'd' in balance_info and isinstance(balance_info['d'], list):
            for acc in balance_info['d']:
                if acc.get('group') == ACCOUNT_TYPE:
                    target_acc = acc
                    break

        if not target_acc:
            state.status = "ERROR_NO_ACCOUNT"
            print(f"❌ Error: Account type '{ACCOUNT_TYPE}' not found.")
            return

        state.account_id = target_acc['account_id']
        state.starting_balance = float(target_acc.get('amount', 0.0))
        state.current_balance = state.starting_balance
        state.status = "SCANNING_MARKET"
        print(f"✅ Connected to {ACCOUNT_TYPE.upper()} Account ID: {state.account_id} | Balance: ${state.starting_balance:,.2f}")

        # Subscribe to ticks
        await self.client.market.subscribe_ticks(ASSET_PAIR)

        round_num = 1
        while True:
            # Check pause status
            if not state.is_running:
                state.status = "PAUSED_BY_USER"
                await asyncio.sleep(1)
                continue

            # Martingale Max Step Reset
            if state.martingale_step > MAX_MARTINGALE_STEPS:
                print(f"⚠️ Max Martingale Steps ({MAX_MARTINGALE_STEPS}) reached! Resetting.")
                state.current_bet = BASE_AMOUNT
                state.martingale_step = 1
                state.cumulative_cycle_loss = 0.0

            self._signal_event.clear()
            if state.status != "PAUSED_BY_USER":
                state.status = "SCANNING_MARKET"
            
            try:
                await asyncio.wait_for(self._signal_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if not state.is_running:
                continue

            signal = self._active_signal
            self._active_signal = None

            if signal:
                self.is_trading = True
                state.status = "TRADE_IN_PROGRESS"
                await self.execute_trade(round_num, signal)
                round_num += 1
                self.is_trading = False
                state.status = "COOLING_DOWN"
                await asyncio.sleep(DELAY_AFTER_TRADE)

    async def execute_trade(self, round_num: int, direction: str):
        loop = asyncio.get_running_loop()
        self._trade_future = loop.create_future()

        order_res = await self.client.trade.place_order(
            pair=ASSET_PAIR,
            amount=state.current_bet,
            direction=direction,
            duration=TRADE_DURATION,
            account_id=state.account_id,
            group=ACCOUNT_TYPE
        )

        if not order_res or not order_res.get("id"):
            print(f"❌ Failed to place trade: {order_res}")
            return

        trade_id = order_res["id"]
        self._pending_trade_id = trade_id
        strike = order_res.get("curs_strike", 0.0)

        try:
            trade_closed = await asyncio.wait_for(self._trade_future, timeout=12.0)
            self.process_trade_result(round_num, direction, state.current_bet, strike, trade_closed)
        except asyncio.TimeoutError:
            print("⚠️ Timeout waiting for trade closure event.")
        finally:
            self._pending_trade_id = None
            self._trade_future = None

    def process_trade_result(self, round_num: int, direction: str, amount: float, strike: float, info: Dict[str, Any]):
        status = info.get("status", "").lower()
        balance_change = float(info.get("balance_change", 0.0))
        close_p = float(info.get("curs_close", 0.0))

        state.total_trades += 1

        # WIN
        if status == "win" or balance_change > 0:
            profit = balance_change
            state.net_session_pnl += profit
            state.current_balance += profit
            state.total_wins += 1

            if state.current_streak_type == "WIN":
                state.current_streak_count += 1
            else:
                state.current_streak_type = "WIN"
                state.current_streak_count = 1
            if state.current_streak_count > state.max_win_streak:
                state.max_win_streak = state.current_streak_count

            print(f"✅ WIN (+${profit:.2f}) | Resetting to ${BASE_AMOUNT:.2f}")
            state.current_bet = BASE_AMOUNT
            state.martingale_step = 1
            state.cumulative_cycle_loss = 0.0

            state.recent_trades.appendleft({
                "round": round_num,
                "time": time.time(),
                "dir": direction,
                "amount": amount,
                "strike": strike,
                "close": close_p,
                "status": "win",
                "pnl": profit
            })

        # LOSS
        elif status in ["loose", "loss"] or balance_change < 0:
            loss = amount
            state.net_session_pnl -= loss
            state.current_balance -= loss
            state.total_losses += 1

            if state.current_streak_type == "LOSS":
                state.current_streak_count += 1
            else:
                state.current_streak_type = "LOSS"
                state.current_streak_count = 1
            if state.current_streak_count > state.max_loss_streak:
                state.max_loss_streak = state.current_streak_count

            state.cumulative_cycle_loss += loss
            state.martingale_step += 1
            next_bet = self.calculate_next_martingale_bet()
            state.current_bet = next_bet

            print(f"❌ LOSS (-${loss:.2f}) | Martingale Step {state.martingale_step} -> Next Bet: ${next_bet:.2f}")

            state.recent_trades.appendleft({
                "round": round_num,
                "time": time.time(),
                "dir": direction,
                "amount": amount,
                "strike": strike,
                "close": close_p,
                "status": "loss",
                "pnl": -loss
            })

        # DRAW
        else:
            state.total_draws += 1
            state.recent_trades.appendleft({
                "round": round_num,
                "time": time.time(),
                "dir": direction,
                "amount": amount,
                "strike": strike,
                "close": close_p,
                "status": "draw",
                "pnl": 0.0
            })

if __name__ == "__main__":
    token = get_token()
    
    # Start Web Dashboard on port (daemon thread)
    t = threading.Thread(target=start_http_server, daemon=True)
    t.start()

    # Start Trading Engine in main event loop
    trader = CloudTrader(token)
    asyncio.run(trader.start())
