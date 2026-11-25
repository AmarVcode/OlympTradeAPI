"""
=====================================================================
🚀 OLYMPTRADE DEMO TRADING BOT LAUNCHER
Strategy: Supertrend (10, 3.0) + LuxAlgo Echo Forecast (50, 50)
Target: Real Forex & Crypto Pairs (Payout >= 80%, No OTC)
Durations: 8 min -> 16 min -> 32 min -> Stop at 3 Consecutive Losses
=====================================================================
"""
import os
import sys

# Force Demo Account Mode
os.environ["ACCOUNT_TYPE"] = "demo"

# Set base bet (defaults to 70)
if "BASE_AMOUNT" not in os.environ:
    os.environ["BASE_AMOUNT"] = "70.0"

# Import bot engine
from render_bot import ForexTrader, start_http_server, get_token, PORT, BASE_AMOUNT
import asyncio
import threading

def main():
    print("=" * 65)
    print("🧪 INITIALIZING OLYMPTRADE DEMO TRADER")
    print("=" * 65)
    print(f"• Account Mode      : DEMO (Paper Trading)")
    print(f"• Starting Bet      : ${BASE_AMOUNT:.2f}")
    print(f"• Trade Durations   : 8m (Step 1) -> 16m (Step 2) -> 32m (Step 3)")
    print(f"• Max Loss Limit    : Stop completely after 3 consecutive losses")
    print(f"• Strategy          : Supertrend (10, 3.0) + LuxAlgo Echo Forecast (50, 50)")
    print(f"• Assets Filter     : Real Forex & Crypto Pairs (No OTC) with Payout >= 80%")
    print(f"• Local Dashboard   : http://localhost:{PORT}")
    print("=" * 65)

    token = get_token()

    # 1. Start Web Dashboard in background thread
    dash_thread = threading.Thread(target=start_http_server, daemon=True)
    dash_thread.start()

    # 2. Run Trading Engine
    trader = ForexTrader(token)
    try:
        asyncio.run(trader.start())
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user (Ctrl+C).")

if __name__ == "__main__":
    main()
