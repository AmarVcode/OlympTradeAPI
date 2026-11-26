import asyncio
import os
import sys
import time
import json
import math
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import deque
from typing import Optional, Dict, Any, Deque, List, Tuple
from olymptrade_ws import OlympTradeClient

# =====================================================================
# ⚙️ CONFIGURATION & SETTINGS
# =====================================================================
# Automatically load .env file
_env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(_env_path):
    with open(_env_path, "r", encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                _k = _k.strip()
                _v = _v.strip().strip('"').strip("'")
                if _k not in os.environ:
                    os.environ[_k] = _v

PORT = int(os.environ.get("PORT", 10000))
ACCOUNT_TYPE = os.environ.get("ACCOUNT_TYPE", "real").lower() # Default to "real" for Live Trading
BASE_AMOUNT = float(os.environ.get("BASE_AMOUNT", 70.0))     # Starting bet ($70 / ₹70)
MIN_PAYOUT_RATE = float(os.environ.get("MIN_PAYOUT_RATE", 0.75)) # Configurable payout (default 75%+)

# Duration Progression: 8m -> 16m -> 32m -> Stop
DURATIONS = [8 * 60, 16 * 60, 32 * 60]                       # [480s, 960s, 1920s]
MAX_CONSECUTIVE_LOSSES = 3                                   # Stop after 3 consecutive losses
CANDLE_TIMEFRAME = 60                                        # Fixed 1-minute candle timeframe (60s)
CANDLE_HISTORY_COUNT = 180                                   # 180 candles for MA Sabres (TEMA 50) + Echo Forecast

# Indicator Parameters (from TradingView Pine Scripts)
# 1. MA Sabres [LuxAlgo] (Replaces Supertrend)
MA_SABRES_TYPE = "TEMA"                                      # TEMA / EMA / SMA
MA_SABRES_LENGTH = 50                                        # Length
MA_SABRES_COUNT = 20                                         # Previous Trend Duration

# 2. The Echo Forecast [LuxAlgo]
ECHO_EVAL_LENGTH = 50                                        # Evaluation Window
ECHO_FCAST_WINDOW = 50                                       # Forecast Window
ECHO_FMODE = "Similarity"                                    # 'Similarity' or 'Dissimilarity'
ECHO_CMODE = "Cumulative"                                    # 'Cumulative' or 'Mean'

# 3. Hourly Fallback Probability Trading
HOURLY_TRADE_ENABLED = os.environ.get("HOURLY_TRADE_ENABLED", "true").lower() == "true"
HOURLY_TRADE_INTERVAL = int(os.environ.get("HOURLY_TRADE_INTERVAL", "3600"))  # 3600s = 1 hour

# Non-OTC Forex Pairs to monitor
FOREX_PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD",
    "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "CADJPY", "CHFJPY", "EURAUD",
    "EURCAD", "EURCHF", "GBPCHF", "GBPAUD", "GBPCAD", "AUDCAD", "AUDNZD",
    "CADCHF", "EURNZD", "GBPNZD", "NZDCAD", "NZDCHF", "NZDJPY"
]

# Non-OTC Crypto Pairs to monitor (24/7 Market)
CRYPTO_PAIRS = [
    "BTCUSD", "ETHUSD", "BTC", "ETH", "LTCUSD", "XRPUSD", "SOLUSD", "ADAUSD",
    "DOGEUSD", "DOTUSD", "AVAXUSD", "LINKUSD", "BNBUSD", "TRXUSD",
    "BCHUSD", "ATOMUSD", "MATICUSD"
]

TARGET_ASSETS = FOREX_PAIRS + CRYPTO_PAIRS

def is_valid_asset(pair: str) -> bool:
    """Ensure pair is in allowed list and strictly NOT an OTC asset."""
    p = pair.upper()
    if "_OTC" in p or "OTC" in p or "_X" in p or "INDEX" in p:
        return False
    return p in TARGET_ASSETS or (len(p) == 6 and (p in FOREX_PAIRS or p in CRYPTO_PAIRS))
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

import base64
from datetime import datetime, timezone

def inspect_token(token_str: str) -> Dict[str, Any]:
    """Inspects JWT token expiry and health."""
    try:
        parts = token_str.split('.')
        if len(parts) >= 2:
            padded = parts[1] + '=' * (-len(parts[1]) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded.encode('utf-8')).decode('utf-8'))
            exp = payload.get('exp')
            user_id = payload.get('user_id')
            if exp:
                now_ts = time.time()
                is_expired = now_ts > exp
                time_diff = exp - now_ts
                if is_expired:
                    time_left = "EXPIRED"
                    status = "EXPIRED"
                else:
                    hrs = int(time_diff // 3600)
                    mins = int((time_diff % 3600) // 60)
                    time_left = f"{hrs}h {mins}m"
                    status = "VALID"
                
                exp_dt = datetime.fromtimestamp(exp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
                return {
                    "valid": not is_expired,
                    "status": status,
                    "expires_at": exp_dt,
                    "time_left": time_left,
                    "user_id": user_id
                }
    except Exception:
        pass
    return {
        "valid": False,
        "status": "INVALID",
        "expires_at": "---",
        "time_left": "UNKNOWN",
        "user_id": None
    }

# =====================================================================
# 📈 TECHNICAL INDICATORS (MA Sabres [LuxAlgo] + LuxAlgo Echo Forecast)
# =====================================================================
def calc_ema_series(values: List[float], length: int) -> List[Optional[float]]:
    """Calculates Exponential Moving Average series matching Pine Script ta.ema."""
    n = len(values)
    if n < length:
        return [None] * n
    alpha = 2.0 / (length + 1)
    out: List[Optional[float]] = [None] * n
    out[length - 1] = sum(values[:length]) / length
    for i in range(length, n):
        out[i] = alpha * values[i] + (1.0 - alpha) * out[i - 1] # type: ignore
    return out

def calculate_ma_sabres(
    closes: List[float],
    ma_type: str = "TEMA",
    length: int = 50,
    count: int = 20
) -> Optional[Dict[str, Any]]:
    """
    Exact Pine Script implementation of MA Sabres [LuxAlgo] from indicator/ma-sabres.
    ma    = type.ma(len)
    fl    = ta.falling(ma, count)
    rs    = ta.rising(ma, count)
    up    = fl[1] and ma > ma[1]
    dn    = rs[1] and ma < ma[1]
    """
    n = len(closes)
    if n < 30:
        return None

    # Dynamically scale down if series is shorter than 3*length
    eff_len = length
    eff_count = count
    if n < length * 3:
        eff_len = max(10, n // 3)
        eff_count = max(5, eff_len // 2)

    # 1. Moving Average Calculation (Default: TEMA)
    if ma_type == "TEMA":
        ema1 = calc_ema_series(closes, eff_len)
        first_v1 = eff_len - 1
        sub_ema1 = [x for x in ema1[first_v1:] if x is not None]
        sub_ema2 = calc_ema_series(sub_ema1, eff_len)
        ema2 = [None] * first_v1 + sub_ema2

        first_v2 = first_v1 + eff_len - 1
        sub_ema2_valid = [x for x in ema2[first_v2:] if x is not None]
        sub_ema3 = calc_ema_series(sub_ema2_valid, eff_len)
        ema3 = [None] * first_v2 + sub_ema3

        ma: List[Optional[float]] = [None] * n
        for i in range(n):
            if ema1[i] is not None and ema2[i] is not None and ema3[i] is not None:
                ma[i] = (3.0 * ema1[i]) - (3.0 * ema2[i]) + ema3[i] # type: ignore
    elif ma_type == "SMA":
        ma = [None] * n
        for i in range(eff_len - 1, n):
            ma[i] = sum(closes[i - eff_len + 1 : i + 1]) / eff_len
    else:
        ma = calc_ema_series(closes, eff_len)

    # 2. ta.falling & ta.rising for eff_count bars
    fl = [False] * n
    rs = [False] * n
    for i in range(eff_count, n):
        if any(ma[i - k] is None for k in range(eff_count + 1)):
            continue
        fl[i] = all(ma[i - k] < ma[i - k - 1] for k in range(eff_count)) # type: ignore
        rs[i] = all(ma[i - k] > ma[i - k - 1] for k in range(eff_count)) # type: ignore

    # 3. Up and Dn Reversal Triggers
    up = [False] * n
    dn = [False] * n
    for i in range(1, n):
        if ma[i] is not None and ma[i - 1] is not None:
            up[i] = bool(fl[i - 1] and ma[i] > ma[i - 1]) # type: ignore
            dn[i] = bool(rs[i - 1] and ma[i] < ma[i - 1]) # type: ignore

    # 4. State Tracking (UP or DOWN)
    current_direction = "NEUTRAL"
    last_signal_idx = -1
    for i in range(n):
        if up[i]:
            current_direction = "UP"
            last_signal_idx = i
        elif dn[i]:
            current_direction = "DOWN"
            last_signal_idx = i

    # Fallback to current slope if no prior reversal signal in window
    if current_direction == "NEUTRAL" and ma[-1] is not None and ma[-2] is not None:
        current_direction = "UP" if ma[-1] > ma[-2] else "DOWN"

    return {
        "ma": round(ma[-1], 5) if ma[-1] is not None else 0.0,
        "up_trigger": up[-1],
        "dn_trigger": dn[-1],
        "direction": current_direction,
        "bars_since_signal": (n - 1 - last_signal_idx) if last_signal_idx != -1 else 999,
        "is_rising": bool(ma[-1] > ma[-2]) if ma[-1] is not None and ma[-2] is not None else False
    }


def calculate_echo_forecast(
    closes: List[float],
    length: int = 50,
    fcast: int = 50,
    fmode: str = "Similarity",
    cmode: str = "Cumulative"
) -> Optional[Dict[str, Any]]:
    """
    Exact Pine Script LuxAlgo Echo Forecast implementation.
    Matches lines 40-103 of indicator/theechoforecast.
    """
    total_needed = length + fcast * 2 + 1
    if len(closes) < total_needed:
        # Dynamically scale window if at least 60 candles are available
        if len(closes) >= 60:
            fcast = min(50, (len(closes) - 1) // 3)
            length = fcast
            total_needed = length + fcast * 2 + 1
        else:
            return None

    # In Pine Script: src[0] is the latest bar, src[1] is 1 bar ago.
    # A[i] = closes[-(1 + i)]
    A = [closes[-(1 + i)] for i in range(fcast * 2 + length + 1)]

    # d[i] = src[i] - src[i+1]
    d = [closes[-(1 + i)] - closes[-(2 + i)] for i in range(len(closes) - 1)]

    # Reference window: a = A.slice(0, fcast - 1)
    a = A[0 : fcast - 1]

    def mean(lst: List[float]) -> float:
        return sum(lst) / len(lst)

    def std_dev(lst: List[float], m: float) -> float:
        var = sum((x - m) ** 2 for x in lst) / len(lst)
        return math.sqrt(max(var, 1e-14))

    def covariance(x: List[float], y: List[float], mx: float, my: float) -> float:
        return sum((xi - mx) * (yi - my) for xi, yi in zip(x, y)) / len(x)

    mean_a = mean(a)
    std_a = std_dev(a, mean_a)

    best_k = 0
    best_val = None

    # Find historical window with maximum Pearson correlation
    for i in range(length):
        b = A[fcast + i : fcast * 2 + i - 1]
        mean_b = mean(b)
        std_b = std_dev(b, mean_b)

        cov = covariance(a, b, mean_a, mean_b)
        r = cov / (std_a * std_b) if (std_a * std_b) != 0 else 0.0

        if best_val is None:
            best_val = r
            best_k = i
        else:
            if fmode == "Similarity":
                if r >= best_val:
                    best_val = r
                    best_k = i
            else:
                if r <= best_val:
                    best_val = r
                    best_k = i

    # Generate the projected Echo trajectory
    current = closes[-1]
    forecast_points = []

    for i in range(fcast):
        idx = fcast + best_k + (fcast - i - 1)
        e = d[idx]
        if cmode == "Cumulative":
            current += e
        elif cmode == "Mean":
            current = mean_a + e
        forecast_points.append(current)

    start_p = closes[-1]
    final_p = forecast_points[-1]
    proj_change = final_p - start_p
    proj_pct = (proj_change / start_p) * 100 if start_p != 0 else 0.0

    return {
        "best_k": best_k,
        "correlation": round(best_val or 0.0, 4),
        "current_price": start_p,
        "final_price": final_p,
        "projected_change": proj_change,
        "projected_pct": round(proj_pct, 4),
        "direction": "up" if proj_change > 0 else "down",
        "forecast_points": forecast_points
    }


def evaluate_trade_probability(
    closes: List[float],
    sabres_res: Dict[str, Any],
    echo_res: Dict[str, Any],
    payout: float
) -> Dict[str, Any]:
    """
    Computes statistical edge / win probability percentage for an asset.
    Combines:
    1. Echo Forecast historical pattern similarity (Pearson r) & forecast return magnitude
    2. MA Sabres [LuxAlgo] TEMA trend direction, slope (is_rising), and position
    3. Reversal triggers (up_trigger, dn_trigger)
    4. Payout weighting
    """
    corr = echo_res.get("correlation", 0.0)
    echo_dir = echo_res.get("direction", "up").lower()
    proj_pct = echo_res.get("projected_pct", 0.0)

    sabres_dir = sabres_res.get("direction", "UP").upper()
    is_rising = sabres_res.get("is_rising", False)
    ma_val = sabres_res.get("ma", closes[-1])
    price_above_ma = closes[-1] >= ma_val

    # 1. Echo Forecast Score Component (up to 45 pts)
    corr_pts = max(0.0, corr) * 35.0
    slope_pts = min(10.0, max(0.0, abs(proj_pct) * 50.0))
    echo_total = corr_pts + slope_pts

    up_score = 0.0
    down_score = 0.0

    if echo_dir == "up":
        up_score += echo_total
    else:
        down_score += echo_total

    # 2. MA Sabres Trend & Momentum Component (up to 45 pts)
    if sabres_dir == "UP":
        up_score += 25.0
    elif sabres_dir == "DOWN":
        down_score += 25.0

    if is_rising:
        up_score += 10.0
    else:
        down_score += 10.0

    if price_above_ma:
        up_score += 10.0
    else:
        down_score += 10.0

    # 3. Reversal Triggers Bonus (up to 10 pts)
    if sabres_res.get("up_trigger"):
        up_score += 10.0
    if sabres_res.get("dn_trigger"):
        down_score += 10.0

    # 4. Final Probability Calculation
    if up_score >= down_score:
        favored_dir = "up"
        delta = up_score - down_score
    else:
        favored_dir = "down"
        delta = down_score - up_score

    # Map delta (0 to 100) to win probability (50% to 92%)
    prob = round(min(92.0, 50.0 + (delta * 0.42)), 1)
    effective_score = round(prob * (payout / 100.0), 2)

    return {
        "direction": favored_dir,
        "probability": prob,
        "effective_score": effective_score,
        "payout": payout,
        "sabres_dir": sabres_dir,
        "echo_dir": echo_dir.upper(),
        "echo_corr": corr,
        "echo_proj": proj_pct,
        "ma": ma_val
    }

# =====================================================================
# 🌐 GLOBAL BOT STATE
# =====================================================================
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

        # Streaks & Limits
        self.current_streak_type = "None"
        self.current_streak_count = 0
        self.consecutive_losses = 0               # Track consecutive losses (0 to 3)
        self.max_consecutive_losses = MAX_CONSECUTIVE_LOSSES
        self.max_win_streak = 0
        self.max_loss_streak = 0

        # Martingale & Duration State
        self.current_bet = BASE_AMOUNT
        self.current_duration_seconds = DURATIONS[0] # Starts at 8m (480s)
        self.cumulative_cycle_loss = 0.0

        # Hourly Fallback Probability Trading State
        self.last_trade_time = time.time()
        self.time_since_last_trade = 0
        self.time_until_hourly_trade = HOURLY_TRADE_INTERVAL
        self.hourly_trade_enabled = HOURLY_TRADE_ENABLED
        self.last_trade_type = "NONE"             # "CONFLUENCE" or "HOURLY_PROB"
        self.last_evaluated_prob = 0.0

        # API Connection & Token Health
        self.api_connected: bool = False
        self.token_status: str = "CHECKING"        # "VALID", "EXPIRED", "INVALID"
        self.token_expires_at: str = "---"
        self.token_time_left: str = "---"
        self.connection_msg: str = "Connecting to OlympTrade..."

        # Market & Signal State
        self.active_pair = "EURUSD"
        self.active_forex_pair = "EURUSD"           # Backwards compatibility alias
        self.active_asset_type = "FOREX"            # "FOREX" or "CRYPTO"
        self.active_payout = 0.82
        self.eligible_pairs: List[Dict[str, Any]] = []
        self.ma_sabres_direction = "UNKNOWN"       # "UP" or "DOWN"
        self.ma_sabres_value = 0.0
        self.ma_sabres_trigger = "NONE"
        self.supertrend_direction = "UNKNOWN"       # Backwards compatibility alias
        self.echo_direction = "UNKNOWN"             # "UP" or "DOWN"
        self.echo_projection_pct = 0.0
        self.echo_correlation = 0.0
        self.last_setup_info = "Initializing market scan..."
        self.recent_trades: Deque[Dict[str, Any]] = deque(maxlen=20)

state = BotState()

# =====================================================================
# 🌐 HTML DASHBOARD
# =====================================================================
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>OlympTrade Forex & Crypto Bot (MA Sabres + Echo Forecast)</title>
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
    .logo-box h1 { font-size: 1.35rem; font-weight: 700; }
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

    /* Control Panel */
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
    .control-info { display: flex; align-items: center; gap: 12px; }
    .btn-group { display: flex; gap: 12px; }
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
    }
    .btn-start { background: #10b981; color: #fff; box-shadow: 0 4px 14px rgba(16, 185, 129, 0.35); }
    .btn-start:hover:not(:disabled) { background: #059669; }
    .btn-stop { background: #ef4444; color: #fff; box-shadow: 0 4px 14px rgba(239, 68, 68, 0.35); }
    .btn-stop:hover:not(:disabled) { background: #dc2626; }
    .btn:disabled { opacity: 0.35; cursor: not-allowed; }
    .badge-running { background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid #10b981; }
    .badge-paused { background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid #f59e0b; }
    .badge-halted { background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid #ef4444; }

    /* Strategy Banner */
    .strategy-banner {
      background: linear-gradient(135deg, rgba(139, 92, 246, 0.12), rgba(59, 130, 246, 0.08));
      border: 1px solid rgba(139, 92, 246, 0.3);
      border-radius: 12px;
      padding: 16px 20px;
      margin-bottom: 24px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 16px;
    }
    .strategy-title { font-weight: 700; font-size: 0.95rem; color: #c4b5fd; margin-bottom: 4px; }
    .strategy-desc { font-size: 0.85rem; color: var(--muted); }
    .indicator-pill {
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.8rem;
      padding: 4px 8px;
      border-radius: 6px;
      background: rgba(255, 255, 255, 0.05);
      margin-right: 6px;
    }

    /* Stats Grid */
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 18px;
      position: relative;
    }
    .card-label {
      font-size: 0.72rem;
      color: var(--muted);
      text-transform: uppercase;
      font-weight: 600;
      letter-spacing: 0.5px;
      margin-bottom: 6px;
    }
    .card-val {
      font-family: 'JetBrains Mono', monospace;
      font-size: 1.6rem;
      font-weight: 800;
    }
    .val-green { color: var(--green); text-shadow: 0 0 12px var(--green-glow); }
    .val-red { color: var(--red); text-shadow: 0 0 12px var(--red-glow); }
    .card-sub { font-size: 0.8rem; color: var(--muted); margin-top: 4px; }

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
    table { width: 100%; border-collapse: collapse; font-size: 0.85rem; text-align: left; }
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
    /* Connection & Token Health Box */
    .conn-box {
      border-radius: 12px;
      padding: 12px 18px;
      margin-bottom: 20px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 12px;
      font-size: 0.85rem;
      border: 1px solid var(--card-border);
      background: var(--card-bg);
      transition: all 0.3s ease;
    }
    .conn-ok {
      border-color: rgba(16, 185, 129, 0.35);
      background: rgba(16, 185, 129, 0.05);
    }
    .conn-error {
      border-color: rgba(239, 68, 68, 0.6);
      background: rgba(239, 68, 68, 0.15);
      box-shadow: 0 0 15px rgba(239, 68, 68, 0.2);
    }
    .conn-item { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    .conn-label { font-size: 0.72rem; color: var(--muted); font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; }
    .badge-api-connected { background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid #10b981; }
    .badge-api-disconnected { background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid #ef4444; }
    .badge-token-valid { background: rgba(59, 130, 246, 0.2); color: #60a5fa; border: 1px solid #3b82f6; }
    .badge-token-expired { background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid #ef4444; }

    footer { text-align: center; margin-top: 32px; font-size: 0.75rem; color: var(--muted); }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="logo-box">
        <h1>📊 OlympTrade Forex & Crypto Trend Bot</h1>
      </div>
      <div class="badges">
        <span class="badge" id="api-badge"><span class="pulse-dot" id="api-dot"></span><span id="api-status-text">API: CONNECTING</span></span>
        <span class="badge" id="token-badge"><span id="token-status-text">TOKEN: CHECKING</span></span>
        <span class="badge badge-status"><span id="bot-status">RUNNING</span></span>
        <span class="badge" id="account-badge">DEMO</span>
        <span class="badge" style="background:#1f2937; color:#9ca3af;">ID: <span id="account-id">---</span></span>
        <span class="badge" style="background:#1f2937; color:#9ca3af;">UPTIME: <span id="uptime">0m</span></span>
      </div>
    </header>

    <!-- Live Platform & Token Health Bar -->
    <div id="conn-health-box" class="conn-box conn-ok">
      <div class="conn-item">
        <span class="conn-label">BROKER API:</span>
        <strong id="broker-status-text" style="color:#10b981;">CONNECTED (wss://ws.olymptrade.com)</strong>
      </div>
      <div class="conn-item">
        <span class="conn-label">TOKEN AUTH:</span>
        <strong id="token-health-text" style="color:#10b981;">ACTIVE &amp; VALID</strong>
        <span style="color:var(--muted); font-size:0.75rem;">(Expires in: <strong id="token-expiry-left" style="color:#38bdf8;">--</strong> · <span id="token-expiry-date">--</span>)</span>
      </div>
      <div class="conn-item">
        <span class="conn-label">HOURLY FALLBACK:</span>
        <strong id="hourly-status-text" style="color:#38bdf8;">ACTIVE (Every 60m)</strong>
        <span style="color:var(--muted); font-size:0.75rem;">(Next check: <strong id="hourly-countdown-text" style="color:#f59e0b;">60m 00s</strong> · Since last trade: <span id="time-since-last-trade">0m</span>)</span>
      </div>
      <div class="conn-item">
        <span class="conn-label">LIVE STATUS:</span>
        <span id="conn-detail-msg" style="color:var(--muted); font-size:0.8rem;">Ready</span>
      </div>
    </div>

    <!-- Bot Control Panel -->
    <div class="control-panel">
      <div class="control-info">
        <span style="font-weight:700; font-size:0.9rem;">TRADING CONTROL:</span>
        <span class="badge badge-running" id="trading-state-badge">🟢 TRADING ACTIVE</span>
      </div>
      <div class="btn-group">
        <button id="btn-start" onclick="setBotTrading(true)" class="btn btn-start" disabled>▶ START BOT</button>
        <button id="btn-stop" onclick="setBotTrading(false)" class="btn btn-stop">⏹ STOP BOT</button>
      </div>
    </div>

    <!-- Strategy & Indicators Banner -->
    <div class="strategy-banner">
      <div>
        <div class="strategy-title">🎯 MA Sabres [LuxAlgo] (TEMA 50, 20) + LuxAlgo Echo Forecast (50, 50)</div>
        <div class="strategy-desc">
          Fixed 1-Minute Analysis · Real Forex &amp; Crypto (No OTC) · Both Must Confirm UP / DOWN · Max 1 Active Trade
        </div>
        <div style="margin-top: 8px;">
          <span class="indicator-pill">Timeframe: <strong style="color:#38bdf8;">1m FIXED</strong></span>
          <span class="indicator-pill">MA Sabres: <strong id="ind-sabres" style="color:#60a5fa;">SCANNING</strong></span>
          <span class="indicator-pill">Echo Forecast: <strong id="ind-echo" style="color:#a78bfa;">SCANNING</strong></span>
          <span class="indicator-pill">Pair: <strong id="active-pair" style="color:#34d399;">EURUSD</strong> (<span id="pair-payout">82%</span>) [<span id="asset-type" style="color:#f59e0b;">FOREX</span>]</span>
          <span class="indicator-pill">Hourly Fallback: <strong id="pill-hourly-countdown" style="color:#f59e0b;">60m 00s</strong></span>
        </div>
      </div>
      <div style="font-family:'JetBrains Mono'; font-size:0.85rem; text-align:right;">
        <div>Next Bet: <strong id="next-bet" style="color:#fff; font-size:1.15rem;">$70.00</strong></div>
        <div style="color:var(--muted); margin-top:4px;">Duration: <strong id="next-duration" style="color:#38bdf8;">8 min</strong></div>
      </div>
    </div>

    <!-- Stats Grid -->
    <div class="grid">
      <!-- Session Net Profit -->
      <div class="card">
        <div class="card-label">Session Net Profit</div>
        <div class="card-val" id="net-pnl">$0.00</div>
        <div class="card-sub" id="pnl-sub">Starting: $0.00</div>
      </div>

      <!-- Current Balance -->
      <div class="card">
        <div class="card-label">Current Balance</div>
        <div class="card-val" id="curr-balance">$0.00</div>
        <div class="card-sub" id="balance-sub">Account: DEMO</div>
      </div>

      <!-- Consecutive Loss Limit Tracker -->
      <div class="card">
        <div class="card-label">Consecutive Losses (Max 3)</div>
        <div class="card-val" id="consec-losses">0 / 3</div>
        <div class="card-sub" id="consec-sub">Halt trigger at 3 consecutive losses</div>
      </div>

      <!-- Win Rate / Record -->
      <div class="card">
        <div class="card-label">Win Rate / Record</div>
        <div class="card-val" id="win-rate">0.0%</div>
        <div class="card-sub"><span id="record-wins">0</span>W - <span id="record-losses">0</span>L (<span id="total-trades">0</span> total)</div>
      </div>
    </div>

    <!-- Recent Trades Table -->
    <div class="section-title">
      <span>📋 Recent Forex Trades (Last 20)</span>
      <span style="font-size:0.75rem; color:var(--muted);">Auto-refreshes every 2s</span>
    </div>
    <div class="table-card">
      <table>
        <thead>
          <tr>
            <th>Round</th>
            <th>Time</th>
            <th>Type</th>
            <th>Pair</th>
            <th>Direction</th>
            <th>Duration</th>
            <th>Bet</th>
            <th>Strike</th>
            <th>Close</th>
            <th>Result</th>
            <th>PnL</th>
          </tr>
        </thead>
        <tbody id="trades-body">
          <tr><td colspan="10" style="text-align:center; color:var(--muted); padding:24px;">Waiting for first Forex / Crypto trade setup...</td></tr>
        </tbody>
      </table>
    </div>

    <footer>
      OlympTrade Automated Forex & Crypto Cloud Bot · MA Sabres + Echo Forecast Strategy · Auto-KeepAlive Active
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
        updateTradingUI(data.is_running, data.status);
      } catch (err) {
        alert("Control error: " + err);
      } finally {
        setTimeout(fetchStats, 500);
      }
    }

    function updateTradingUI(isRunning, status) {
      const badge = document.getElementById('trading-state-badge');
      const btnStart = document.getElementById('btn-start');
      const btnStop = document.getElementById('btn-stop');

      if (status && status.includes('HALTED')) {
        badge.textContent = "🛑 STOPPED (3 LOSS LIMIT)";
        badge.className = "badge badge-halted";
        btnStart.disabled = false;
        btnStop.disabled = true;
      } else if (isRunning) {
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

        document.getElementById('bot-status').textContent = data.status;
        document.getElementById('account-id').textContent = data.account_id || '---';
        document.getElementById('uptime').textContent = formatUptime(data.uptime);
        updateTradingUI(data.is_running, data.status);

        // Update API & Token Badges & Connection Box
        const apiBadge = document.getElementById('api-badge');
        const apiDot = document.getElementById('api-dot');
        const apiStatusText = document.getElementById('api-status-text');
        const tokenBadge = document.getElementById('token-badge');
        const tokenStatusText = document.getElementById('token-status-text');
        const connBox = document.getElementById('conn-health-box');
        const brokerStatusText = document.getElementById('broker-status-text');
        const tokenHealthText = document.getElementById('token-health-text');

        if (data.api_connected) {
          apiBadge.className = "badge badge-api-connected";
          apiDot.style.background = "#10b981";
          apiStatusText.textContent = "API: CONNECTED";
          brokerStatusText.textContent = "CONNECTED (wss://ws.olymptrade.com)";
          brokerStatusText.style.color = "#10b981";
        } else {
          apiBadge.className = "badge badge-api-disconnected";
          apiDot.style.background = "#ef4444";
          apiStatusText.textContent = "API: DISCONNECTED";
          brokerStatusText.textContent = "DISCONNECTED";
          brokerStatusText.style.color = "#ef4444";
        }

        if (data.token_status === "VALID") {
          tokenBadge.className = "badge badge-token-valid";
          tokenStatusText.textContent = `TOKEN: VALID (${data.token_time_left} left)`;
          tokenHealthText.textContent = "ACTIVE & VALID";
          tokenHealthText.style.color = "#10b981";
          connBox.className = "conn-box conn-ok";
        } else {
          tokenBadge.className = "badge badge-token-expired";
          tokenStatusText.textContent = `TOKEN: ${data.token_status}`;
          tokenHealthText.textContent = `EXPIRED / REJECTED (${data.token_status})`;
          tokenHealthText.style.color = "#ef4444";
          connBox.className = "conn-box conn-error";
        }

        document.getElementById('token-expiry-left').textContent = data.token_time_left || '--';
        document.getElementById('token-expiry-date').textContent = data.token_expires_at || '--';
        document.getElementById('conn-detail-msg').textContent = data.connection_msg || '';

        const badge = document.getElementById('account-badge');
        badge.textContent = data.account_type === 'real' ? '🔴 LIVE REAL MONEY' : '🧪 DEMO ACCOUNT';
        badge.className = 'badge ' + (data.account_type === 'real' ? 'badge-live' : 'badge-demo');

        // PnL & Balance
        const pnlEl = document.getElementById('net-pnl');
        pnlEl.textContent = formatMoney(data.net_session_pnl);
        pnlEl.className = 'card-val ' + (data.net_session_pnl >= 0 ? 'val-green' : 'val-red');
        document.getElementById('pnl-sub').textContent = 'Starting: $' + Number(data.starting_balance).toLocaleString('en-US', {minimumFractionDigits:2});
        document.getElementById('curr-balance').textContent = '$' + Number(data.current_balance).toLocaleString('en-US', {minimumFractionDigits:2});
        const balSub = document.getElementById('balance-sub');
        if (balSub) balSub.textContent = data.account_type === 'real' ? 'Account: LIVE REAL' : 'Account: DEMO';

        // Consecutive Losses
        const lossCount = data.consecutive_losses || 0;
        const lossEl = document.getElementById('consec-losses');
        lossEl.textContent = `${lossCount} / 3`;
        lossEl.className = 'card-val ' + (lossCount >= 2 ? 'val-red' : (lossCount === 1 ? 'card-val' : 'val-green'));
        document.getElementById('consec-sub').textContent = lossCount >= 3 ? 'Max loss limit reached! Trading stopped.' : `Step ${lossCount + 1} of 3`;

        // Record
        document.getElementById('win-rate').textContent = data.win_rate + '%';
        document.getElementById('record-wins').textContent = data.total_wins;
        document.getElementById('record-losses').textContent = data.total_losses;
        document.getElementById('total-trades').textContent = data.total_trades;

        // Indicators & Next Trade
        document.getElementById('next-bet').textContent = '$' + Number(data.current_bet).toFixed(2);
        const durMins = Math.round(data.current_duration_seconds / 60);
        document.getElementById('next-duration').textContent = `${durMins} min`;

        document.getElementById('active-pair').textContent = data.active_pair || data.active_forex_pair || 'EURUSD';
        document.getElementById('asset-type').textContent = data.active_asset_type || 'FOREX';
        document.getElementById('pair-payout').textContent = Math.round((data.active_payout || 0.8) * 100) + '%';
        
        const sabresEl = document.getElementById('ind-sabres');
        sabresEl.textContent = `${data.ma_sabres_direction || 'SCANNING'} (${data.ma_sabres_value || '--'})`;
        sabresEl.style.color = data.ma_sabres_direction === 'UP' ? '#34d399' : (data.ma_sabres_direction === 'DOWN' ? '#f87171' : '#9ca3af');

        const echoEl = document.getElementById('ind-echo');
        echoEl.textContent = `${data.echo_direction} (${data.echo_projection_pct > 0 ? '+' : ''}${data.echo_projection_pct}%)`;
        echoEl.style.color = data.echo_direction === 'UP' ? '#34d399' : (data.echo_direction === 'DOWN' ? '#f87171' : '#9ca3af');

        // Update Hourly Fallback Timers
        const timeUntilHourly = (data.time_until_hourly_trade !== undefined) ? data.time_until_hourly_trade : 3600;
        const timeSinceTrade = (data.time_since_last_trade !== undefined) ? data.time_since_last_trade : 0;
        const minsLeft = Math.floor(timeUntilHourly / 60);
        const secsLeft = timeUntilHourly % 60;
        const countdownStr = `${minsLeft}m ${secsLeft.toString().padStart(2, '0')}s`;
        const minsSince = Math.floor(timeSinceTrade / 60);
        const sinceStr = minsSince > 0 ? `${minsSince}m ago` : 'Just now';

        const hourlyCountdownEl = document.getElementById('hourly-countdown-text');
        if (hourlyCountdownEl) hourlyCountdownEl.textContent = countdownStr;
        const pillHourlyEl = document.getElementById('pill-hourly-countdown');
        if (pillHourlyEl) pillHourlyEl.textContent = countdownStr;
        const timeSinceEl = document.getElementById('time-since-last-trade');
        if (timeSinceEl) timeSinceEl.textContent = sinceStr;

        // Trades Table
        if (data.recent_trades && data.recent_trades.length > 0) {
          const tbody = document.getElementById('trades-body');
          tbody.innerHTML = '';
          data.recent_trades.forEach(t => {
            const tr = document.createElement('tr');
            const isWin = t.status === 'win' || t.pnl > 0;
            const pnlFormatted = formatMoney(t.pnl);
            const dur = Math.round((t.duration || 480) / 60);
            const typeBadge = t.type === 'HOURLY_PROB'
              ? `<span class="badge" style="background:rgba(245,158,11,0.2); color:#fbbf24; border:1px solid #f59e0b; font-size:0.65rem;">HOURLY (${t.prob || 0}%)</span>`
              : `<span class="badge" style="background:rgba(59,130,246,0.2); color:#60a5fa; border:1px solid #3b82f6; font-size:0.65rem;">CONFLUENCE</span>`;
            tr.innerHTML = `
              <td>#${t.round}</td>
              <td>${formatTime(t.time)}</td>
              <td>${typeBadge}</td>
              <td><strong>${t.pair || 'EURUSD'}</strong></td>
              <td><span class="${t.dir === 'up' ? 'badge-dir-up' : 'badge-dir-down'}">${t.dir.toUpperCase()}</span></td>
              <td>${dur}m</td>
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

    setInterval(fetchStats, 2000);
    fetchStats();
  </script>
</body>
</html>
"""

# =====================================================================
# 🌐 WEB DASHBOARD HTTP HANDLER
# =====================================================================
class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        state.last_ping_time = time.time()

        if self.path in ('/healthz', '/ping'):
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
            return

        elif self.path in ('/api/start', '/api/stop'):
            if self.path == '/api/start':
                state.is_running = True
                if state.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
                    # User clicked start after halt: reset consecutive loss cycle
                    state.consecutive_losses = 0
                    state.current_bet = BASE_AMOUNT
                    state.current_duration_seconds = DURATIONS[0]
                    state.cumulative_cycle_loss = 0.0
                state.status = "SCANNING_FOREX"
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
                "api_connected": state.api_connected,
                "token_status": state.token_status,
                "token_expires_at": state.token_expires_at,
                "token_time_left": state.token_time_left,
                "connection_msg": state.connection_msg,
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
                "consecutive_losses": state.consecutive_losses,
                "max_consecutive_losses": state.max_consecutive_losses,
                "current_bet": state.current_bet,
                "current_duration_seconds": state.current_duration_seconds,
                "active_pair": state.active_pair,
                "active_forex_pair": state.active_pair,
                "active_asset_type": state.active_asset_type,
                "active_payout": state.active_payout,
                "ma_sabres_direction": state.ma_sabres_direction,
                "ma_sabres_value": state.ma_sabres_value,
                "ma_sabres_trigger": state.ma_sabres_trigger,
                "timeframe": "1m",
                "supertrend_direction": state.ma_sabres_direction,
                "echo_direction": state.echo_direction,
                "echo_projection_pct": state.echo_projection_pct,
                "last_setup_info": state.last_setup_info,
                "last_trade_time": int(state.last_trade_time),
                "time_since_last_trade": int(time.time() - state.last_trade_time),
                "time_until_hourly_trade": max(0, HOURLY_TRADE_INTERVAL - int(time.time() - state.last_trade_time)),
                "hourly_trade_enabled": state.hourly_trade_enabled,
                "last_trade_type": state.last_trade_type,
                "last_evaluated_prob": state.last_evaluated_prob,
                "recent_trades": list(state.recent_trades)
            }
            self.wfile.write(json.dumps(payload).encode('utf-8'))
            return

        else:
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(HTML_TEMPLATE.encode('utf-8'))

    def log_message(self, format, *args):
        return

def start_http_server():
    server = HTTPServer(('0.0.0.0', PORT), DashboardHandler)
    print(f"🌐 Web Dashboard & Healthcheck active at http://0.0.0.0:{PORT}")
    server.serve_forever()

# =====================================================================
# ⚡ 24/7 FOREX TRADING ENGINE
# =====================================================================
class ForexTrader:
    def __init__(self, token: str):
        self.token = token
        self.client = OlympTradeClient(
            access_token=self.token,
            account_group=ACCOUNT_TYPE,
            log_raw_messages=False
        )
        self.is_trading = False
        self._pending_trade_id: Optional[int] = None
        self._trade_future: Optional[asyncio.Future] = None
        self.payout_cache: Dict[str, float] = {}
        self.last_trade_time = time.time()

    def update_connection_health(self):
        """Update live API connection state & token expiry on state container."""
        token_info = inspect_token(self.token)
        state.token_status = token_info["status"]
        state.token_expires_at = token_info["expires_at"]
        state.token_time_left = token_info["time_left"]

        is_connected = bool(self.client.connection and self.client.connection.is_connected)
        state.api_connected = is_connected and token_info["valid"]

        if not token_info["valid"]:
            state.connection_msg = f"Token {token_info['status']}: Please refresh token in .env"
        elif not is_connected:
            state.connection_msg = "WebSocket reconnecting to OlympTrade platform..."
        else:
            state.connection_msg = "API Connected & Token Valid (OlympTrade WebSockets Active)"

    def calculate_next_martingale_bet(self) -> float:
        """Calculate recovery bet for 80%+ payout."""
        payout = state.active_payout if state.active_payout >= 0.5 else 0.80
        target_profit = BASE_AMOUNT * payout
        if state.cumulative_cycle_loss <= 0:
            return BASE_AMOUNT
        needed = (state.cumulative_cycle_loss + target_profit) / payout
        return round(needed, 1)

    async def update_eligible_assets(self):
        """Filters non-OTC Forex & Crypto pairs with profitability >= 80%."""
        try:
            p_data = await self.client.market.get_profitability(state.account_id)
            if p_data and isinstance(p_data, list):
                for item in p_data:
                    pair_name = item.get("pair") or item.get("p") or item.get("name")
                    val = item.get("value") or item.get("profit") or item.get("payout") or item.get("win")
                    if not pair_name or val is None:
                        continue
                    # Convert to decimal payout
                    payout_dec = float(val) / 100.0 if float(val) > 1.0 else float(val)
                    self.payout_cache[str(pair_name).upper()] = payout_dec
        except Exception as e:
            print(f"⚠️ Note on profitability check: {e}")

        # Filter standard Forex & Crypto pairs (Strictly Non-OTC)
        eligible = []
        for pair in TARGET_ASSETS:
            if not is_valid_asset(pair):
                continue
            # Payout defaults to 82% during open hours if not cached
            payout = self.payout_cache.get(pair.upper(), 0.82)
            if payout >= MIN_PAYOUT_RATE:
                asset_type = "CRYPTO" if pair in CRYPTO_PAIRS else "FOREX"
                eligible.append({"pair": pair, "payout": payout, "type": asset_type})

        # Sort highest payout first
        eligible.sort(key=lambda x: x["payout"], reverse=True)
        state.eligible_pairs = eligible
        if eligible:
            state.active_pair = eligible[0]["pair"]
            state.active_forex_pair = eligible[0]["pair"]
            state.active_asset_type = eligible[0].get("type", "FOREX")
            state.active_payout = eligible[0]["payout"]

    async def scan_for_signal(self, pair: str) -> Optional[str]:
        """
        Fetches 1m candles for pair and computes MA Sabres [LuxAlgo] + Echo Forecast.
        Only one trade can be active at a time. Returns 'up', 'down', or None.
        """
        if self.is_trading:
            return None

        candles = await self.client.market.get_candles(
            pair=pair,
            size=CANDLE_TIMEFRAME, # Fixed 60s (1m) timeframe
            count=CANDLE_HISTORY_COUNT
        )

        if not candles or len(candles) < 30:
            print(f"⚠️ [{pair}] Candle feed returned {len(candles) if candles else 0} bars. Skipping.")
            return None

        closes = [float(c["close"]) for c in candles]

        # 1. Calculate MA Sabres [LuxAlgo] (TEMA 50, count 20) on 1-minute closes
        sabres_res = calculate_ma_sabres(
            closes=closes,
            ma_type=MA_SABRES_TYPE,
            length=MA_SABRES_LENGTH,
            count=MA_SABRES_COUNT
        )
        if not sabres_res:
            return None

        state.ma_sabres_value = sabres_res["ma"]
        state.ma_sabres_direction = sabres_res["direction"] # "UP" or "DOWN"
        state.supertrend_direction = sabres_res["direction"] # Compatibility
        state.ma_sabres_trigger = "UP" if sabres_res["up_trigger"] else ("DOWN" if sabres_res["dn_trigger"] else "NONE")

        # 2. Calculate Echo Forecast [LuxAlgo] (50, 50) on 1-minute closes
        echo_res = calculate_echo_forecast(
            closes=closes,
            length=ECHO_EVAL_LENGTH,
            fcast=ECHO_FCAST_WINDOW,
            fmode=ECHO_FMODE,
            cmode=ECHO_CMODE
        )
        if not echo_res:
            return None

        state.echo_direction = echo_res["direction"].upper()
        state.echo_projection_pct = echo_res["projected_pct"]
        state.echo_correlation = echo_res["correlation"]

        # 3. Combine Indicator Signals: BOTH MA Sabres and Echo Forecast must say UP or DOWN
        # Both say UP -> Place BUY UP
        if sabres_res["direction"] == "UP" and echo_res["direction"] == "up":
            state.last_setup_info = f"[{pair} 1m] MA Sabres UP (TEMA: {sabres_res['ma']}) & Echo Forecast UP (+{echo_res['projected_pct']}%)"
            print(f"🎯 [ALIGNED - BUY UP] {state.last_setup_info}")
            return "up"

        # Both say DOWN -> Place SELL DOWN
        elif sabres_res["direction"] == "DOWN" and echo_res["direction"] == "down":
            state.last_setup_info = f"[{pair} 1m] MA Sabres DOWN (TEMA: {sabres_res['ma']}) & Echo Forecast DOWN ({echo_res['projected_pct']}%)"
            print(f"🎯 [ALIGNED - SELL DOWN] {state.last_setup_info}")
            return "down"

        else:
            print(f"👀 [1m SCAN {pair}] MA Sabres: {sabres_res['direction']} ({sabres_res['ma']}) vs Echo: {state.echo_direction} ({echo_res['projected_pct']:+.2f}%, Corr: {echo_res['correlation']:.2f}) -> Waiting alignment")

        return None

    async def find_highest_probability_trade(self) -> Optional[Dict[str, Any]]:
        """
        Scans all open eligible Forex & Crypto assets (with payout >= MIN_PAYOUT_RATE)
        and evaluates their win probability based on:
        1. Echo Forecast historical pattern correlation (r) and projected return slope %
        2. MA Sabres [LuxAlgo] TEMA trend direction, reversal triggers, and momentum
        3. Broker payout rate weighting
        Returns the asset and direction with the highest statistical probability.
        """
        if not state.eligible_pairs:
            await self.update_eligible_assets()

        candidates = state.eligible_pairs if state.eligible_pairs else [{"pair": state.active_pair, "payout": 0.82}]
        evaluated: List[Dict[str, Any]] = []

        print(f"\n📊 [HOURLY PROBABILITY SCAN] Evaluating {len(candidates)} assets to pick highest statistical probability...")

        for item in candidates[:8]:  # Evaluate top 8 available pairs
            pair = item["pair"]
            payout = item.get("payout", 0.82)
            try:
                candles = await self.client.market.get_candles(
                    pair=pair,
                    size=CANDLE_TIMEFRAME,
                    count=CANDLE_HISTORY_COUNT
                )
                if not candles or len(candles) < 30:
                    continue

                closes = [float(c["close"]) for c in candles]

                sabres_res = calculate_ma_sabres(
                    closes=closes,
                    ma_type=MA_SABRES_TYPE,
                    length=MA_SABRES_LENGTH,
                    count=MA_SABRES_COUNT
                )
                echo_res = calculate_echo_forecast(
                    closes=closes,
                    length=ECHO_EVAL_LENGTH,
                    fcast=ECHO_FCAST_WINDOW,
                    fmode=ECHO_FMODE,
                    cmode=ECHO_CMODE
                )

                if not sabres_res or not echo_res:
                    continue

                payout_pct = payout * 100.0 if payout <= 1.0 else payout
                prob_info = evaluate_trade_probability(closes, sabres_res, echo_res, payout_pct)
                prob_info["pair"] = pair
                evaluated.append(prob_info)
                print(f"   • {pair}: Win Prob {prob_info['probability']}% for {prob_info['direction'].upper()} (Score: {prob_info['effective_score']}, Echo: {prob_info['echo_dir']} [r={prob_info['echo_corr']:.2f}], Sabres: {prob_info['sabres_dir']})")
                await asyncio.sleep(0.4)
            except Exception as e:
                print(f"⚠️ Error evaluating probability for {pair}: {e}")

        if not evaluated:
            return None

        # Sort highest effective score first
        evaluated.sort(key=lambda x: x["effective_score"], reverse=True)
        return evaluated[0]

    async def on_trade_closed(self, message: Dict[str, Any]):
        """Event 26: Trade Closed Callback"""
        trade_data = message.get("d", [])
        if isinstance(trade_data, list) and len(trade_data) > 0:
            trade_info = trade_data[0]
            if self._pending_trade_id and trade_info.get("id") == self._pending_trade_id:
                if self._trade_future and not self._trade_future.done():
                    self._trade_future.set_result(trade_info)

    async def start(self):
        state.status = "CONNECTING"
        self.update_connection_health()
        self.client.register_callback(26, self.on_trade_closed)

        await self.client.start()
        self.update_connection_health()

        # Balance & Account Info
        balance_info = await self.client.balance.get_balance(timeout=10.0)
        target_acc = None
        if balance_info and 'd' in balance_info and isinstance(balance_info['d'], list):
            for acc in balance_info['d']:
                if acc.get('group') == ACCOUNT_TYPE:
                    target_acc = acc
                    break

        if not target_acc and self.client._latest_balance and 'd' in self.client._latest_balance:
            for acc in self.client._latest_balance['d']:
                if acc.get('group') == ACCOUNT_TYPE:
                    target_acc = acc
                    break

        if not target_acc and self.client.account_id:
            # Use actual client account if detected by connection
            detected_group = getattr(self.client, 'account_group', ACCOUNT_TYPE)
            if detected_group == ACCOUNT_TYPE:
                target_acc = {'account_id': self.client.account_id, 'amount': 0.0, 'group': ACCOUNT_TYPE}

        if not target_acc:
            state.status = f"ERROR_{ACCOUNT_TYPE.upper()}_ACCOUNT_NOT_FOUND"
            print(f"❌ Error: Account type '{ACCOUNT_TYPE}' not found in OlympTrade account list.")
            return

        state.account_id = target_acc['account_id']
        state.starting_balance = float(target_acc.get('amount', 0.0))
        state.current_balance = state.starting_balance
        state.status = "SCANNING_MARKET"
        
        if ACCOUNT_TYPE == "real":
            print("=" * 65)
            print("🔴 [LIVE REAL ACCOUNT ACTIVATED] TRADING WITH REAL CAPITAL")
            print(f"💰 Account ID: {state.account_id} | Live Balance: ${state.starting_balance:,.2f}")
            print(f"🎯 Strategy: MA Sabres (TEMA 50, 20) + Echo Forecast (50, 50)")
            print(f"💵 Base Bet: ${BASE_AMOUNT:.2f} | Consecutive Loss Limit: {MAX_CONSECUTIVE_LOSSES}")
            print("=" * 65)
            if state.starting_balance < BASE_AMOUNT:
                print(f"⚠️ [WARNING] Live balance (${state.starting_balance:.2f}) is lower than the starting bet (${BASE_AMOUNT:.2f})!")
        else:
            print(f"✅ Connected to DEMO Account ID: {state.account_id} | Balance: ${state.starting_balance:,.2f}")

        # Identify eligible Forex & Crypto pairs with >= 80% payout
        await self.update_eligible_assets()
        forex_count = sum(1 for p in state.eligible_pairs if p.get("type") == "FOREX")
        crypto_count = sum(1 for p in state.eligible_pairs if p.get("type") == "CRYPTO")
        print(f"🔍 Found {len(state.eligible_pairs)} eligible non-OTC assets ({forex_count} Forex, {crypto_count} Crypto) with >= 80% payout. Primary: {state.active_pair}")

        round_num = 1
        last_assets_refresh = time.time()
        self.last_trade_time = time.time() # Start hourly timer upon successful connection

        while True:
            self.update_connection_health()
            # Periodically refresh asset payouts every 5 minutes
            if time.time() - last_assets_refresh > 300:
                await self.update_eligible_assets()
                last_assets_refresh = time.time()

            # 1. Check if paused by user
            if not state.is_running:
                if state.consecutive_losses < MAX_CONSECUTIVE_LOSSES:
                    state.status = "PAUSED_BY_USER"
                await asyncio.sleep(1)
                continue

            # 2. Check Max Consecutive Losses Limit
            if state.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
                state.status = "HALTED: MAX 3 CONSECUTIVE LOSSES REACHED"
                state.is_running = False
                print(f"🛑 [STOP TRIGGERED] Bot halted after {state.consecutive_losses} consecutive losses! Trading stopped.")
                await asyncio.sleep(2)
                continue

            state.status = "SCANNING_MARKET"

            # 3. Standard Dual-Indicator Confluence Scan
            candidate_pairs = [p["pair"] for p in state.eligible_pairs[:6]] if state.eligible_pairs else [state.active_pair]
            signal_found = None
            selected_pair = state.active_pair

            for pair in candidate_pairs:
                if not state.is_running:
                    break
                state.active_pair = pair
                state.active_forex_pair = pair
                state.active_asset_type = "CRYPTO" if pair in CRYPTO_PAIRS else "FOREX"
                sig = await self.scan_for_signal(pair)
                if sig:
                    signal_found = sig
                    selected_pair = pair
                    break
                # Short pause between scanning pairs
                await asyncio.sleep(1.0)

            # Update hourly timer state
            elapsed_since_last_trade = time.time() - self.last_trade_time
            state.time_since_last_trade = int(elapsed_since_last_trade)
            state.time_until_hourly_trade = max(0, HOURLY_TRADE_INTERVAL - int(elapsed_since_last_trade))

            if signal_found and state.is_running and state.consecutive_losses < MAX_CONSECUTIVE_LOSSES:
                self.is_trading = True
                state.status = "TRADE_IN_PROGRESS"
                state.last_trade_type = "CONFLUENCE"
                await self.execute_trade(round_num, selected_pair, signal_found, trade_type="CONFLUENCE")
                round_num += 1
                self.is_trading = False
                self.last_trade_time = time.time()
                state.status = "COOLING_DOWN"
                await asyncio.sleep(5)

            elif HOURLY_TRADE_ENABLED and elapsed_since_last_trade >= HOURLY_TRADE_INTERVAL and state.is_running and state.consecutive_losses < MAX_CONSECUTIVE_LOSSES and not self.is_trading:
                # 4. Hourly Probability Fallback Trigger: No trades in the last hour!
                mins_elapsed = round(elapsed_since_last_trade / 60)
                print(f"\n⏰ [HOURLY PROBABILITY TRIGGER] No trades placed in the last {mins_elapsed} minutes!")
                print("🔍 Scanning eligible Forex & Crypto pairs to punch trade with HIGHEST statistical probability...")
                state.status = "SCANNING_HOURLY_PROBABILITY"

                best_candidate = await self.find_highest_probability_trade()
                if best_candidate and state.is_running and state.consecutive_losses < MAX_CONSECUTIVE_LOSSES:
                    print(f"🎯 [HOURLY TRADE SELECTED] Pair: {best_candidate['pair']} | Dir: {best_candidate['direction'].upper()} | Win Prob: {best_candidate['probability']}% | Payout: {best_candidate['payout']}%")
                    print(f"   Reason: Echo {best_candidate['echo_dir']} (Corr: {best_candidate['echo_corr']:.2f}, Proj: {best_candidate['echo_proj']:+.2f}%) & MA Sabres {best_candidate['sabres_dir']}")
                    state.last_setup_info = f"[HOURLY {best_candidate['pair']} 1m] Prob: {best_candidate['probability']}% {best_candidate['direction'].upper()} (Echo: {best_candidate['echo_dir']}, Sabres: {best_candidate['sabres_dir']})"

                    self.is_trading = True
                    state.status = "TRADE_IN_PROGRESS"
                    state.last_trade_type = "HOURLY_PROB"
                    state.last_evaluated_prob = best_candidate['probability']
                    await self.execute_trade(
                        round_num,
                        best_candidate['pair'],
                        best_candidate['direction'],
                        trade_type="HOURLY_PROB",
                        prob=best_candidate['probability']
                    )
                    round_num += 1
                    self.is_trading = False
                    self.last_trade_time = time.time()
                    state.status = "COOLING_DOWN"
                    await asyncio.sleep(5)
                else:
                    print("⚠️ Could not find suitable hourly probability candidate. Will re-evaluate in next cycle.")
                    await asyncio.sleep(15)
            else:
                # Wait before next candle scan cycle (15s)
                await asyncio.sleep(15)

    async def execute_trade(
        self,
        round_num: int,
        pair: str,
        direction: str,
        trade_type: str = "CONFLUENCE",
        prob: float = 0.0
    ):
        duration = state.current_duration_seconds
        amount = state.current_bet

        # Live Real Account Risk & Balance Protection
        if state.account_type == "real" and state.current_balance > 0 and state.current_balance < amount:
            alert_msg = f"🛑 [LIVE RISK GUARD] Live balance (${state.current_balance:.2f}) is below trade amount (${amount:.2f})! Bot paused to protect capital."
            print(alert_msg)
            state.status = "PAUSED: LOW LIVE BALANCE"
            state.last_setup_info = alert_msg
            state.is_running = False
            return

        dur_mins = round(duration / 60)
        mode_prefix = "🔴 [LIVE TRADE]" if state.account_type == "real" else "🧪 [DEMO TRADE]"
        prob_str = f" [Win Prob: {prob}%]" if trade_type == "HOURLY_PROB" else ""
        print(f"\n🚀 {mode_prefix} [TRADE #{round_num} ({trade_type}){prob_str}] Pair: {pair} | Dir: {direction.upper()} | Bet: ${amount:.2f} | Duration: {dur_mins}m ({duration}s)")

        loop = asyncio.get_running_loop()
        self._trade_future = loop.create_future()

        order_res = await self.client.trade.place_order(
            pair=pair,
            amount=amount,
            direction=direction,
            duration=duration,
            account_id=state.account_id,
            group=ACCOUNT_TYPE
        )

        if not order_res or not order_res.get("id"):
            err_msg = order_res.get("message") or order_res.get("error") if isinstance(order_res, dict) else str(order_res)
            print(f"❌ Failed to place trade on {pair}: {err_msg}")
            state.last_setup_info = f"Order failed on {pair}: {err_msg}"
            return

        trade_id = order_res["id"]
        self._pending_trade_id = trade_id
        strike = order_res.get("curs_strike", 0.0)

        try:
            # Wait for trade to close (duration + 60s buffer for broker callback)
            timeout = duration + 60.0
            print(f"⏳ Trade #{round_num} open (ID: {trade_id}). Waiting for {dur_mins}m expiry...")
            trade_closed = await asyncio.wait_for(self._trade_future, timeout=timeout)
            self.process_trade_result(round_num, pair, direction, amount, strike, duration, trade_closed, trade_type, prob)
        except asyncio.TimeoutError:
            print(f"⚠️ Timeout waiting for trade closure event after {dur_mins}m.")
        finally:
            self._pending_trade_id = None
            self._trade_future = None

    def process_trade_result(
        self,
        round_num: int,
        pair: str,
        direction: str,
        amount: float,
        strike: float,
        duration: int,
        info: Dict[str, Any],
        trade_type: str = "CONFLUENCE",
        prob: float = 0.0
    ):
        status = info.get("status", "").lower()
        balance_change = float(info.get("balance_change", 0.0))
        close_p = float(info.get("curs_close", 0.0))

        state.total_trades += 1

        # ----------------- WIN -----------------
        if status == "win" or balance_change > 0:
            profit = balance_change
            state.net_session_pnl += profit
            state.current_balance += profit
            state.total_wins += 1

            # Reset consecutive losses and restore defaults
            state.consecutive_losses = 0
            state.current_bet = BASE_AMOUNT
            state.current_duration_seconds = DURATIONS[0] # Reset to 8m (480s)
            state.cumulative_cycle_loss = 0.0

            if state.current_streak_type == "WIN":
                state.current_streak_count += 1
            else:
                state.current_streak_type = "WIN"
                state.current_streak_count = 1
            if state.current_streak_count > state.max_win_streak:
                state.max_win_streak = state.current_streak_count

            print(f"\n🎉 [TRADE #{round_num} WIN] Profit: +${profit:.2f}!")
            print(f"🔄 Everything RESET: Trade Amount -> ${BASE_AMOUNT:.2f} | Duration -> 8m (480s) | Consecutive Losses -> 0")

            state.recent_trades.appendleft({
                "round": round_num,
                "time": time.time(),
                "type": trade_type,
                "prob": prob,
                "pair": pair,
                "dir": direction,
                "duration": duration,
                "amount": amount,
                "strike": strike,
                "close": close_p,
                "status": "win",
                "pnl": profit
            })

        # ----------------- LOSS -----------------
        elif status in ["loose", "loss"] or balance_change < 0:
            loss = amount
            state.net_session_pnl -= loss
            state.current_balance -= loss
            state.total_losses += 1
            state.consecutive_losses += 1
            state.cumulative_cycle_loss += loss

            if state.current_streak_type == "LOSS":
                state.current_streak_count += 1
            else:
                state.current_streak_type = "LOSS"
                state.current_streak_count = 1
            if state.current_streak_count > state.max_loss_streak:
                state.max_loss_streak = state.current_streak_count

            print(f"\n❌ [TRADE #{round_num} LOSS] Loss: -${loss:.2f}! (Consecutive Losses: {state.consecutive_losses} / {MAX_CONSECUTIVE_LOSSES})")

            # Check Progression Steps
            if state.consecutive_losses == 1:
                # Next trade: 16 minutes (960s)
                state.current_duration_seconds = DURATIONS[1] # 16m
                state.current_bet = self.calculate_next_martingale_bet()
                print(f"➡️ [STEP 2] Loss 1: Next Duration -> 16m (960s) | Next Bet -> ${state.current_bet:.2f}")

            elif state.consecutive_losses == 2:
                # Next trade: 32 minutes (1920s)
                state.current_duration_seconds = DURATIONS[2] # 32m
                state.current_bet = self.calculate_next_martingale_bet()
                print(f"➡️ [STEP 3] Loss 2: Next Duration -> 32m (1920s) | Next Bet -> ${state.current_bet:.2f}")

            elif state.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
                # 3 Consecutive Losses -> STOP TRADING!
                state.is_running = False
                state.status = "HALTED: MAX 3 CONSECUTIVE LOSSES REACHED"
                print(f"\n🛑 [MAX LOSS LIMIT REACHED] 3 Consecutive Losses hit! Bot is now STOPPED completely.")

            state.recent_trades.appendleft({
                "round": round_num,
                "time": time.time(),
                "type": trade_type,
                "prob": prob,
                "pair": pair,
                "dir": direction,
                "duration": duration,
                "amount": amount,
                "strike": strike,
                "close": close_p,
                "status": "loss",
                "pnl": -loss
            })

        # ----------------- DRAW -----------------
        else:
            state.total_draws += 1
            state.recent_trades.appendleft({
                "round": round_num,
                "time": time.time(),
                "type": trade_type,
                "prob": prob,
                "pair": pair,
                "dir": direction,
                "duration": duration,
                "amount": amount,
                "strike": strike,
                "close": close_p,
                "status": "draw",
                "pnl": 0.0
            })

if __name__ == "__main__":
    # 1. Start Web Dashboard HTTP server
    t = threading.Thread(target=start_http_server, daemon=True)
    t.start()

    # 2. Resilient Auto-Reconnect Trading Engine
    async def run_trader_loop():
        while True:
            try:
                token = get_token()
                trader = ForexTrader(token)
                await trader.start()
            except Exception as e:
                err = str(e)
                print(f"\n❌ [CONNECTION ERROR] {err}")
                if "invalid_token" in err.lower() or "1008" in err.lower():
                    state.status = "ERROR: TOKEN EXPIRED / INVALID"
                    state.last_setup_info = "Access token is expired! Please paste a fresh token into .env"
                    print("🛑 [TOKEN EXPIRED] Your OlympTrade access token has expired!")
                    print("👉 Update OLYMP_ACCESS_TOKEN in .env or Render Environment Variables.")
                else:
                    state.status = "DISCONNECTED - RETRYING IN 10s"
                await asyncio.sleep(10)

    asyncio.run(run_trader_loop())
