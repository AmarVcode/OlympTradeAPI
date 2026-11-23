import asyncio
import os
import sys
import logging
from collections import deque
from typing import Optional, Dict, Any, Deque
from olymptrade_ws import OlympTradeClient

# =====================================================================
# ⚙️ TREND-BASED 85% MARTINGALE BOT FOR QUICKLER (5s)
# =====================================================================
BASE_AMOUNT = 70.0         # Starting base amount ($70)
PAYOUT_RATE = 0.85         # Quickler payout rate (85%)
ASSET_PAIR = "ULTRA_X"     # "Quickler" in OlympTrade (5-second index)
TRADE_DURATION = 5         # 5 seconds
MAX_MARTINGALE_STEPS = 6   # Max consecutive loss steps before safety reset
MAX_ROUNDS = 0             # Total trades (0 = run continuously until Ctrl+C)
DELAY_BETWEEN_TRADES = 2   # Seconds to pause between trades
TICK_WINDOW_SIZE = 20      # Number of ticks used for trend detection
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

class TrendMartingaleQuicklerBot:
    def __init__(self, token: str):
        self.token = token
        self.client = OlympTradeClient(access_token=self.token, log_raw_messages=False)
        self.account_id: Optional[int] = None
        self.demo_balance: float = 0.0

        # Mathematical Martingale tracking
        self.current_amount = BASE_AMOUNT
        self.target_profit_per_cycle = round(BASE_AMOUNT * PAYOUT_RATE, 2) # e.g. $59.50
        self.cumulative_cycle_loss = 0.0  # Sum of losses in current streak
        self.consecutive_losses = 0
        
        # Overall Session Stats
        self.total_wins = 0
        self.total_losses = 0
        self.total_draws = 0
        self.net_session_pnl = 0.0

        # Live Tick Stream & Trend Analysis
        self.tick_history: Deque[float] = deque(maxlen=TICK_WINDOW_SIZE)
        
        # Trade completion listener
        self._pending_trade_id: Optional[int] = None
        self._trade_future: Optional[asyncio.Future] = None

    async def on_tick(self, message: Dict[str, Any]):
        """Capture live price ticks for Quickler (Event 1)."""
        tick_list = message.get("d", [])
        if isinstance(tick_list, list):
            for tick in tick_list:
                if tick.get("p") == ASSET_PAIR:
                    price = tick.get("q")
                    if price is not None:
                        self.tick_history.append(float(price))

    async def on_trade_closed(self, message: Dict[str, Any]):
        """Callback triggered when broker sends Event 26 (Trade Closed)."""
        trade_data = message.get("d", [])
        if not isinstance(trade_data, list) or len(trade_data) == 0:
            return
            
        trade_info = trade_data[0]
        trade_id = trade_info.get("id")
        
        if self._pending_trade_id and trade_id == self._pending_trade_id:
            if self._trade_future and not self._trade_future.done():
                self._trade_future.set_result(trade_info)

    def calculate_trend_direction(self) -> str:
        """
        Determines market trend based on live tick momentum and moving averages.
        Returns: 'up' or 'down'
        """
        if len(self.tick_history) < 6:
            return "up"  # Default if not enough ticks yet

        ticks = list(self.tick_history)
        
        # 1. Fast vs Slow Moving Average on ticks
        fast_window = ticks[-4:]  # Last 4 ticks
        slow_window = ticks[-12:] if len(ticks) >= 12 else ticks # Last 12 ticks
        
        fast_sma = sum(fast_window) / len(fast_window)
        slow_sma = sum(slow_window) / len(slow_window)

        # 2. Momentum (current price vs price 5 ticks ago)
        lookback = min(5, len(ticks) - 1)
        momentum = ticks[-1] - ticks[-1 - lookback]

        # Trend Decision
        if fast_sma > slow_sma and momentum >= 0:
            return "up"
        elif fast_sma < slow_sma and momentum <= 0:
            return "down"
        else:
            # Fallback to direct price momentum
            return "up" if momentum >= 0 else "down"

    def calculate_next_martingale_bet(self) -> float:
        """
        Calculates the exact bet needed to fully recover ALL previous cycle losses
        AND lock in the target profit on an 85% payout.
        Formula: (Cumulative Losses + Target Profit) / 0.85
        """
        if self.cumulative_cycle_loss == 0:
            return BASE_AMOUNT

        needed_bet = (self.cumulative_cycle_loss + self.target_profit_per_cycle) / PAYOUT_RATE
        # Round up to 1 decimal place or whole integer for clean execution
        return round(needed_bet, 1)

    async def start(self):
        print("=" * 70)
        print("⚡ TREND-BASED 85% MARTINGALE BOT (QUICKLER 5s)")
        print("=" * 70)
        print(f"📊 Asset: {ASSET_PAIR} (Quickler - 5s duration)")
        print(f"🎯 Base Bet: ${BASE_AMOUNT:.2f} | Payout: {int(PAYOUT_RATE * 100)}%")
        print(f"📈 Direction Strategy: Real-time Live Tick Trend & Momentum")
        print(f"🧮 Martingale Formula: (Losses + ${self.target_profit_per_cycle:.2f}) / 0.85")
        print(f"🛡️ Safety Guard: Max {MAX_MARTINGALE_STEPS} consecutive loss steps")
        print("=" * 70)

        # Register callbacks
        self.client.register_callback(1, self.on_tick)          # Price Ticks
        self.client.register_callback(26, self.on_trade_closed) # Trade Closed

        print("\n[1/3] Connecting to OlympTrade WebSocket...")
        await self.client.start()

        # Retrieve Demo Balance
        balance_info = await self.client.balance.get_balance(timeout=10.0)
        demo_acc = None
        if balance_info and 'd' in balance_info and isinstance(balance_info['d'], list):
            for acc in balance_info['d']:
                if acc.get('group') == 'demo':
                    demo_acc = acc
                    break

        if not demo_acc:
            print("❌ Error: Could not locate Demo account!")
            await self.client.stop()
            return

        self.account_id = demo_acc['account_id']
        self.demo_balance = float(demo_acc['amount'])
        print(f"✅ Demo Account ID: {self.account_id}")
        print(f"💵 Starting Demo Balance: ${self.demo_balance:,.2f}")

        # Subscribe to live tick stream
        print(f"\n[2/3] Subscribing to live {ASSET_PAIR} tick stream for trend analysis...")
        await self.client.market.subscribe_ticks(ASSET_PAIR)
        
        # Warm up tick history (2 seconds)
        print("⏳ Gathering initial tick data for trend detection...")
        await asyncio.sleep(2.0)

        print("\n[3/3] Starting Trend Martingale trading loop...\n")

        round_num = 1
        try:
            while True:
                if MAX_ROUNDS > 0 and round_num > MAX_ROUNDS:
                    print(f"\n🏁 Target of {MAX_ROUNDS} rounds reached.")
                    break

                # Safety guard: Check max consecutive loss streak
                if self.consecutive_losses >= MAX_MARTINGALE_STEPS:
                    print(f"\n⚠️ Max consecutive losses ({MAX_MARTINGALE_STEPS}) reached!")
                    print(f"🔄 Resetting bet to base amount ${BASE_AMOUNT:.2f} to protect bankroll.\n")
                    self.current_amount = BASE_AMOUNT
                    self.cumulative_cycle_loss = 0.0
                    self.consecutive_losses = 0

                await self.execute_trade_round(round_num)
                round_num += 1

                print(f"⏳ Waiting {DELAY_BETWEEN_TRADES}s before analyzing next entry...")
                await asyncio.sleep(DELAY_BETWEEN_TRADES)

        except KeyboardInterrupt:
            print("\n🛑 Bot stopped manually by user.")
        finally:
            self.print_summary()
            await self.client.stop()
            print("🔌 Client disconnected.")

    async def execute_trade_round(self, round_num: int):
        # 1. Analyze live trend direction
        trend_direction = self.calculate_trend_direction()
        current_price = self.tick_history[-1] if self.tick_history else 0.0

        print("-" * 70)
        print(f"🎯 ROUND #{round_num} | Bet: ${self.current_amount:.2f} | Step: {self.consecutive_losses + 1}")
        print(f"📊 Market Trend: {trend_direction.upper()} (Current Tick: {current_price})")
        print("-" * 70)

        # Prepare future to await trade completion
        loop = asyncio.get_running_loop()
        self._trade_future = loop.create_future()

        # Place 5-second order on Quickler
        order_res = await self.client.trade.place_order(
            pair=ASSET_PAIR,
            amount=self.current_amount,
            direction=trend_direction,
            duration=TRADE_DURATION,
            account_id=self.account_id,
            group="demo"
        )

        if not order_res or not order_res.get("id"):
            print(f"❌ Failed to place trade: {order_res}")
            return

        trade_id = order_res["id"]
        self._pending_trade_id = trade_id
        strike_price = order_res.get("curs_strike", "N/A")
        print(f"⚡ Order Placed! ID: {trade_id} | Strike: {strike_price} | Direction: {trend_direction.upper()}")
        print(f"⏳ Trade running (5 seconds)...")

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
            
            # True-Recovery Calculation
            net_cycle_pnl = profit - self.cumulative_cycle_loss
            print(f"✅ WIN! 🚀 | Open: {open_price} -> Close: {close_price} | Trade Profit: +${profit:.2f}")
            print(f"🎉 Streak Recovered! Net Cycle PnL: +${net_cycle_pnl:.2f}")
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

            # Calculate mathematically exact next bet
            next_bet = self.calculate_next_martingale_bet()
            print(f"❌ LOSS! 💥 | Open: {open_price} -> Close: {close_price} | Trade Loss: -${loss:.2f}")
            print(f"📉 Cumulative Streak Loss: -${self.cumulative_cycle_loss:.2f} (Loss streak: {self.consecutive_losses})")
            print(f"📈 Next Bet: ${next_bet:.2f} (to recover ${self.cumulative_cycle_loss:.2f} + make ${self.target_profit_per_cycle:.2f} profit)")
            self.current_amount = next_bet

        # DRAW / STANDOFF
        else:
            self.total_draws += 1
            print(f"⚖️ DRAW / STANDOFF | Open: {open_price} == Close: {close_price} | PnL: $0.00")
            print(f"🔁 Martingale Action: Repeat same bet ${self.current_amount:.2f}")

        print(f"💼 Net Session PnL: {'+' if self.net_session_pnl >= 0 else ''}${self.net_session_pnl:.2f} | Record: {self.total_wins}W / {self.total_losses}L / {self.total_draws}D")

    def print_summary(self):
        total_trades = self.total_wins + self.total_losses + self.total_draws
        print("\n" + "=" * 70)
        print("📋 TREND MARTINGALE SESSION SUMMARY")
        print("=" * 70)
        print(f"Total Trades: {total_trades}")
        print(f"Wins: {self.total_wins} | Losses: {self.total_losses} | Draws: {self.total_draws}")
        winrate = (self.total_wins / total_trades * 100) if total_trades > 0 else 0
        print(f"Win Rate: {winrate:.1f}%")
        print(f"Net Session Profit / Loss: {'+' if self.net_session_pnl >= 0 else ''}${self.net_session_pnl:.2f}")
        print("=" * 70)

if __name__ == "__main__":
    token = get_token()
    bot = TrendMartingaleQuicklerBot(token)
    asyncio.run(bot.start())
