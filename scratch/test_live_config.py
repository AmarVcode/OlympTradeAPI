import sys
import os

sys.path.insert(0, r"c:\Users\Amar\Documents\GitHub\OlympTradeAPI")
import render_bot

print("--- Checking Live Bot Configuration ---")
print(f"ACCOUNT_TYPE: {render_bot.ACCOUNT_TYPE}")
print(f"BASE_AMOUNT: {render_bot.BASE_AMOUNT}")
print(f"MIN_PAYOUT_RATE: {render_bot.MIN_PAYOUT_RATE}")
print(f"HOURLY_TRADE_ENABLED: {render_bot.HOURLY_TRADE_ENABLED}")
print(f"HOURLY_TRADE_INTERVAL: {render_bot.HOURLY_TRADE_INTERVAL}s")

assert render_bot.ACCOUNT_TYPE == "real", f"Expected ACCOUNT_TYPE == 'real', got {render_bot.ACCOUNT_TYPE}"
assert render_bot.BASE_AMOUNT == 70.0, f"Expected BASE_AMOUNT == 70.0, got {render_bot.BASE_AMOUNT}"

print("\n✅ Live Account configuration verified successfully!")
