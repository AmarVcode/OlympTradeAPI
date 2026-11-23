import asyncio
import os
import sys
import time
from collections import deque
from typing import Optional, Dict, Any, Deque, List
from olymptrade_ws import OlympTradeClient

# =====================================================================
# ⚙️ EXHAUSTION SPIKE REVERSAL (SETUP A) + MARTINGALE CONFIGURATION
# =====================================================================
ACCOUNT_TYPE = "real"          # "real" for Live Account, "demo" for Demo Account
BASE_AMOUNT = 70.0             # Starting base bet ($70)
PAYOUT_RATE = 0.85             # Quickler payout rate (85%)
ASSET_PAIR = "ULTRA_X"         # Quickler 5-second asset ticker
TRADE_DURATION = 5             # 5 seconds
CANDLE_TIMEFRAME = 5           # 5-second candle construction

# Setup A Parameters
SPIKE_MULTIPLIER = 2.5         # Spike candle body must be >= 2.5x avg body
MIN_BODY_RATIO = 0.65          # Marubozu filter: body / total range >= 65%
LOOKBACK_CANDLES = 10          # Number of previous 5s candles to evaluate baseline
INVERT_SIGNALS = True          # True = INVERTED MODE (Surge UP -> BUY UP, Surge DOWN -> SELL DOWN)
                               # False = Reversal Mode (Surge UP -> SELL DOWN, Surge DOWN -> BUY UP)

# Risk & Bankroll Management
MAX_MARTINGALE_STEPS = 4       # Max consecutive losses before safety reset (70 -> 153 -> 333 -> 725)
STOP_LOSS = 1500.0             # Max session drawdown before automatic emergency halt ($)
TAKE_PROFIT = 500.0            # Target session profit to lock in and stop ($)
MAX_ROUNDS = 0                 # 0 = run continuously until Ctrl+C or SL/TP hit
DELAY_AFTER_TRADE = 3          # Buffer seconds after a trade closes before scanning setups
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

    print("[!] No access token found. Please set OLYMP_ACCESS_TOKEN in .env")
    sys.exit(1)

class ExhaustionSpikeBot:
    def __init__(self, token: str, account_type: str = ACCOUNT_TYPE):
        self.token = token
        self.account_type = account_type.lower()
        self.client = OlympTradeClient(
            access_token=self.token, 
            account_group=self.account_type, 
            log_raw_messages=False
        )
        self.account_id: Optional[int] = None
        self.starting_balance: float = 0.0
        self.current_balance: float = 0.0

        # Martingale State
        self.current_amount = BASE_AMOUNT
        self.target_profit_per_cycle = round(BASE_AMOUNT * PAYOUT_RATE, 2) # e.g. $59.50
        self.cumulative_cycle_loss = 0.0
        self.consecutive_losses = 0

        # Session Metrics
        self.total_wins = 0
        self.total_losses = 0
        self.total_draws = 0
        self.net_session_pnl = 0.0

        # 5-Second Candle Construction State
        self.current_bucket: Optional[int] = None
        self.current_candle: Optional[Dict[str, float]] = None
        self.completed_candles: Deque[Dict[str, float]] = deque(maxlen=30)

        # Execution Signals & Locks
        self.is_trading = False
        self._pending_trade_id: Optional[int] = None
        self._trade_future: Optional[asyncio.Future] = None
        self._signal_event = asyncio.Event()
        self._active_signal: Optional[str] = None # "up" or "down"

    def calculate_next_martingale_bet(self) -> float:
        """Calculate exact bet needed to recover all streak losses + lock in target profit."""
        if self.cumulative_cycle_loss <= 0:
            return BASE_AMOUNT
        needed_bet = (self.cumulative_cycle_loss + self.target_profit_per_cycle) / PAYOUT_RATE
        return round(needed_bet, 1)

    async def on_tick(self, message: Dict[str, Any]):
        """Build real-time 5-second candles from incoming price ticks."""
        tick_list = message.get("d", [])
        if not isinstance(tick_list, list):
            return

        for tick in tick_list:
            if tick.get("p") != ASSET_PAIR:
                continue

            price = float(tick.get("q", 0.0))
            ts = float(tick.get("t", time.time()))
            bucket = int(ts // CANDLE_TIMEFRAME) * CANDLE_TIMEFRAME

            # New 5-second candle boundary reached
            if self.current_bucket is None:
                self.current_bucket = bucket
                self.current_candle = {"open": price, "high": price, "low": price, "close": price, "time": bucket}
            elif bucket > self.current_bucket:
                # Close the completed 5s candle
                closed_candle = self.current_candle.copy()
                self.completed_candles.append(closed_candle)
                c_body = abs(closed_candle['close'] - closed_candle['open'])
                print(f"🕯️ [5s Candle #{len(self.completed_candles)}] O: {closed_candle['open']:.2f} | H: {closed_candle['high']:.2f} | L: {closed_candle['low']:.2f} | C: {closed_candle['close']:.2f} | Body: {c_body:.2f}")

                # Initialize next 5s candle
                self.current_bucket = bucket
                self.current_candle = {"open": price, "high": price, "low": price, "close": price, "time": bucket}

                # Check Setup A on the closed candle
                if not self.is_trading:
                    signal = self.check_setup_a_exhaustion_spike(closed_candle)
                    if signal:
                        self._active_signal = signal
                        self._signal_event.set()
            else:
                # Update current active 5s candle
                if price > self.current_candle["high"]:
                    self.current_candle["high"] = price
                if price < self.current_candle["low"]:
                    self.current_candle["low"] = price
                self.current_candle["close"] = price

    def check_setup_a_exhaustion_spike(self, last_candle: Dict[str, float]) -> Optional[str]:
        """
        Setup A: The Exhaustion Spike Reversal (Snap-Back)
        - Price was moving steadily in baseline candles.
        - A single abnormal candle (>= 2.5x-4x larger than previous baseline) surges into swing high/low.
        - High body-to-range ratio (marubozu surge).
        - Trigger: Mean-reversion snap-back trade in OPPOSITE direction.
        """
        if len(self.completed_candles) < LOOKBACK_CANDLES:
            return None

        # Extract baseline (previous 6 to 10 candles excluding the spike candle)
        all_candles = list(self.completed_candles)
        baseline = all_candles[-LOOKBACK_CANDLES:-1]

        # Calculate average body size of previous candles
        bodies = [abs(c["close"] - c["open"]) for c in baseline]
        avg_body = sum(bodies) / len(bodies) if bodies else 0.0001
        if avg_body == 0:
            avg_body = 0.0001

        # Spike candle metrics
        spike_open = last_candle["open"]
        spike_close = last_candle["close"]
        spike_high = last_candle["high"]
        spike_low = last_candle["low"]
        spike_body = abs(spike_close - spike_open)
        spike_range = max(spike_high - spike_low, 0.0001)

        # 1. Abnormal size filter (3x-4x average body)
        is_abnormal_size = spike_body >= (SPIKE_MULTIPLIER * avg_body)

        # 2. Strong directional body (Marubozu / momentum filter)
        body_ratio = spike_body / spike_range
        is_strong_body = body_ratio >= MIN_BODY_RATIO

        if not (is_abnormal_size and is_strong_body):
            return None

        # 3. Check for surge into swing high or swing low
        swing_high = max(c["high"] for c in baseline)
        swing_low = min(c["low"] for c in baseline)

        # Bullish Surge (Break of Swing High)
        if spike_close > spike_open and spike_high >= swing_high:
            trade_action = "up" if INVERT_SIGNALS else "down"
            mode_label = "INVERTED MOMENTUM (BUY UP)" if INVERT_SIGNALS else "MEAN-REVERSION SNAP-BACK (SELL DOWN)"
            print(f"\n🚨 [SETUP DETECTED] Bullish Spike Surge UP!")
            print(f"   Spike Body: {spike_body:.4f} vs Avg Body: {avg_body:.4f} ({spike_body/avg_body:.1f}x larger)")
            print(f"   Broke Swing High: {swing_high:.4f} | Body Ratio: {body_ratio*100:.1f}%")
            print(f"   🎯 Strategy Action: {mode_label} -> Enter {trade_action.upper()}")
            return trade_action

        # Bearish Surge (Break of Swing Low)
        elif spike_close < spike_open and spike_low <= swing_low:
            trade_action = "down" if INVERT_SIGNALS else "up"
            mode_label = "INVERTED MOMENTUM (SELL DOWN)" if INVERT_SIGNALS else "MEAN-REVERSION SNAP-BACK (BUY UP)"
            print(f"\n🚨 [SETUP DETECTED] Bearish Spike Surge DOWN!")
            print(f"   Spike Body: {spike_body:.4f} vs Avg Body: {avg_body:.4f} ({spike_body/avg_body:.1f}x larger)")
            print(f"   Broke Swing Low: {swing_low:.4f} | Body Ratio: {body_ratio*100:.1f}%")
            print(f"   🎯 Strategy Action: {mode_label} -> Enter {trade_action.upper()}")
            return trade_action

        return None

    async def on_trade_closed(self, message: Dict[str, Any]):
        """Capture broker Event 26 (Trade Closed)."""
        trade_data = message.get("d", [])
        if not isinstance(trade_data, list) or len(trade_data) == 0:
            return
            
        trade_info = trade_data[0]
        trade_id = trade_info.get("id")
        
        if self._pending_trade_id and trade_id == self._pending_trade_id:
            if self._trade_future and not self._trade_future.done():
                self._trade_future.set_result(trade_info)

    async def start(self):
        print("=" * 72)
        print("⚡ OLYMPTRADE 5-SECOND EXHAUSTION SPIKE REVERSAL BOT")
        print("=" * 72)
        print(f"🌐 Target Account: {self.account_type.upper()}")
        print(f"📊 Asset: {ASSET_PAIR} (Quickler 5s)")
        mode_name = "INVERTED MOMENTUM (Surge UP -> BUY UP | Surge DOWN -> SELL DOWN)" if INVERT_SIGNALS else "REVERSAL SNAP-BACK (Surge UP -> SELL DOWN | Surge DOWN -> BUY UP)"
        print(f"🎯 Strategy: {mode_name}")
        print(f"💰 Base Bet: ${BASE_AMOUNT:.2f} | Payout: {int(PAYOUT_RATE * 100)}%")
        print(f"🧮 Martingale: Exact True-Recovery ((Losses + ${self.target_profit_per_cycle:.2f}) / 0.85)")
        print(f"🛡️ Safety: Max {MAX_MARTINGALE_STEPS} Steps | Stop-Loss: -${STOP_LOSS:.2f} | Take-Profit: +${TAKE_PROFIT:.2f}")
        print("=" * 72)

        # Safety confirmation for LIVE accounts
        if self.account_type == "real":
            print("\n" + "!" * 72)
            print("🚨 ATTENTION: YOU ARE RUNNING IN LIVE ACCOUNT MODE (REAL FUNDS)!")
            print("   Trades placed by this bot will use REAL MONEY.")
            print("!" * 72)
            # Check if auto-approved via env
            auto_confirm = os.environ.get("CONFIRM_LIVE_TRADING", "false").lower() == "true"
            if not auto_confirm:
                confirm = input("⚠️ Type 'CONFIRM' to start trading real funds on Live Account: ").strip()
                if confirm != "CONFIRM":
                    print("🛑 Live trading cancelled by user.")
                    return

        # Register callbacks
        self.client.register_callback(1, self.on_tick)          # Tick receiver
        self.client.register_callback(26, self.on_trade_closed) # Trade outcome receiver

        print("\n[1/3] Connecting to OlympTrade WebSocket...")
        await self.client.start()

        # Retrieve balance and account ID
        balance_info = await self.client.balance.get_balance(timeout=10.0)
        target_acc = None
        if balance_info and 'd' in balance_info and isinstance(balance_info['d'], list):
            for acc in balance_info['d']:
                if acc.get('group') == self.account_type:
                    target_acc = acc
                    break

        if not target_acc:
            print(f"❌ Error: Could not locate '{self.account_type}' account on this token!")
            await self.client.stop()
            return

        self.account_id = target_acc['account_id']
        self.starting_balance = float(target_acc.get('amount', 0.0))
        self.current_balance = self.starting_balance
        print(f"✅ Connected to {self.account_type.upper()} Account ID: {self.account_id}")
        print(f"💵 Starting Balance: ${self.starting_balance:,.2f}")

        # Subscribe to Quickler ticks
        print(f"\n[2/3] Subscribing to live {ASSET_PAIR} tick stream...")
        await self.client.market.subscribe_ticks(ASSET_PAIR)

        print("\n[3/3] Scanning live 5-second candles for Setup A Exhaustion Spikes...\n")

        round_num = 1
        try:
            while True:
                if MAX_ROUNDS > 0 and round_num > MAX_ROUNDS:
                    print(f"\n🏁 Target of {MAX_ROUNDS} rounds reached.")
                    break

                # Risk Limit Checks
                if self.net_session_pnl <= -STOP_LOSS:
                    print(f"\n🛑 STOP-LOSS HIT (-${abs(self.net_session_pnl):.2f})! Halting bot to preserve capital.")
                    break

                if self.net_session_pnl >= TAKE_PROFIT:
                    print(f"\n🎉 TAKE-PROFIT TARGET HIT (+${self.net_session_pnl:.2f})! Locking in session profit.")
                    break

                # Martingale Safety Cap Check
                if self.consecutive_losses >= MAX_MARTINGALE_STEPS:
                    print(f"\n⚠️ Max Martingale Steps ({MAX_MARTINGALE_STEPS}) reached!")
                    print(f"🔄 Resetting bet to base amount ${BASE_AMOUNT:.2f} to protect bankroll.\n")
                    self.current_amount = BASE_AMOUNT
                    self.cumulative_cycle_loss = 0.0
                    self.consecutive_losses = 0

                # Wait for next valid Setup A signal
                self._signal_event.clear()
                print(f"🔍 Monitoring 5s candles... (History: {len(self.completed_candles)} candles | Bet: ${self.current_amount:.2f} | Step {self.consecutive_losses + 1})")
                await self._signal_event.wait()

                signal_direction = self._active_signal
                self._active_signal = None

                if signal_direction:
                    self.is_trading = True
                    await self.execute_spike_trade(round_num, signal_direction)
                    round_num += 1
                    self.is_trading = False

                    print(f"⏳ Cooling down {DELAY_AFTER_TRADE}s before scanning for next spike...")
                    await asyncio.sleep(DELAY_AFTER_TRADE)

        except KeyboardInterrupt:
            print("\n🛑 Bot stopped manually by user.")
        finally:
            self.print_summary()
            await self.client.stop()
            print("🔌 Client disconnected.")

    async def execute_spike_trade(self, round_num: int, direction: str):
        print("-" * 72)
        print(f"🎯 EXECUTING TRADE #{round_num} | Bet: ${self.current_amount:.2f} | Direction: {direction.upper()} | Step: {self.consecutive_losses + 1}")
        print("-" * 72)

        loop = asyncio.get_running_loop()
        self._trade_future = loop.create_future()

        # Place 5-second Snap-Back order
        order_res = await self.client.trade.place_order(
            pair=ASSET_PAIR,
            amount=self.current_amount,
            direction=direction,
            duration=TRADE_DURATION,
            account_id=self.account_id,
            group=self.account_type
        )

        if not order_res or not order_res.get("id"):
            print(f"❌ Failed to place order: {order_res}")
            return

        trade_id = order_res["id"]
        self._pending_trade_id = trade_id
        strike_price = order_res.get("curs_strike", "N/A")
        print(f"⚡ Order Placed! ID: {trade_id} | Strike: {strike_price} | Duration: 5s")
        print(f"⏳ Waiting for snap-back pullback...")

        try:
            trade_closed = await asyncio.wait_for(self._trade_future, timeout=12.0)
            self.process_trade_result(trade_closed)
        except asyncio.TimeoutError:
            print("⚠️ Timeout waiting for trade closure event from broker.")
        finally:
            self._pending_trade_id = None
            self._trade_future = None

    def process_trade_result(self, trade_info: Dict[str, Any]):
        status = trade_info.get("status", "").lower()
        balance_change = float(trade_info.get("balance_change", 0.0))
        open_price = trade_info.get("curs_open", 0.0)
        close_price = trade_info.get("curs_close", 0.0)

        # WIN
        if status == "win" or balance_change > 0:
            profit = balance_change
            self.net_session_pnl += profit
            self.total_wins += 1

            net_cycle_pnl = profit - self.cumulative_cycle_loss
            print(f"✅ WIN! 🚀 | Open: {open_price} -> Close: {close_price} | Trade Profit: +${profit:.2f}")
            print(f"🎉 Snap-Back Successful! Cycle Net: +${net_cycle_pnl:.2f}")
            print(f"🔁 Martingale Action: RESET to base amount ${BASE_AMOUNT:.2f}")

            # Reset Martingale state
            self.current_amount = BASE_AMOUNT
            self.cumulative_cycle_loss = 0.0
            self.consecutive_losses = 0

        # LOSS
        elif status in ["loose", "loss"] or balance_change < 0:
            loss = self.current_amount
            self.net_session_pnl -= loss
            self.total_losses += 1
            self.consecutive_losses += 1
            self.cumulative_cycle_loss += loss

            # Calculate mathematically exact recovery bet
            next_bet = self.calculate_next_martingale_bet()
            print(f"❌ LOSS! 💥 | Open: {open_price} -> Close: {close_price} | Trade Loss: -${loss:.2f}")
            print(f"📉 Cumulative Streak Loss: -${self.cumulative_cycle_loss:.2f} (Loss streak: {self.consecutive_losses})")
            print(f"📈 Next Martingale Bet: ${next_bet:.2f} on next Exhaustion Spike")
            self.current_amount = next_bet

        # DRAW / STANDOFF
        else:
            self.total_draws += 1
            print(f"⚖️ DRAW / STANDOFF | Open: {open_price} == Close: {close_price} | PnL: $0.00")
            print(f"🔁 Martingale Action: Repeat same bet ${self.current_amount:.2f}")

        print(f"💼 Session PnL: {'+' if self.net_session_pnl >= 0 else ''}${self.net_session_pnl:.2f} | Record: {self.total_wins}W / {self.total_losses}L / {self.total_draws}D\n")

    def print_summary(self):
        total_trades = self.total_wins + self.total_losses + self.total_draws
        print("\n" + "=" * 72)
        print("📋 EXHAUSTION SPIKE REVERSAL SESSION SUMMARY")
        print("=" * 72)
        print(f"Account: {self.account_type.upper()} | ID: {self.account_id}")
        print(f"Total Trades: {total_trades}")
        print(f"Wins: {self.total_wins} | Losses: {self.total_losses} | Draws: {self.total_draws}")
        winrate = (self.total_wins / total_trades * 100) if total_trades > 0 else 0
        print(f"Win Rate: {winrate:.1f}%")
        print(f"Net Session Profit / Loss: {'+' if self.net_session_pnl >= 0 else ''}${self.net_session_pnl:.2f}")
        print("=" * 72)

if __name__ == "__main__":
    token = get_token()
    
    # Check CLI arguments or fallback to ACCOUNT_TYPE
    selected_account = ACCOUNT_TYPE
    if "--demo" in sys.argv:
        selected_account = "demo"
    elif "--real" in sys.argv or "--live" in sys.argv:
        selected_account = "real"

    bot = ExhaustionSpikeBot(token, account_type=selected_account)
    asyncio.run(bot.start())
