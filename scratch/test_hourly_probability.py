import sys
import os
import time

sys.path.insert(0, r"c:\Users\Amar\Documents\GitHub\OlympTradeAPI")
from render_bot import (
    evaluate_trade_probability,
    calculate_ma_sabres,
    calculate_echo_forecast,
    BotState,
    HOURLY_TRADE_ENABLED,
    HOURLY_TRADE_INTERVAL,
    state
)

print("--- Testing Hourly Probability Evaluation ---")
# Generate sample closing prices (uptrend)
closes_up = [1.0800 + i * 0.0005 + (0.0001 if i % 2 == 0 else -0.0001) for i in range(160)]

sabres_res = calculate_ma_sabres(closes_up)
echo_res = calculate_echo_forecast(closes_up)

assert sabres_res is not None, "MA Sabres returned None"
assert echo_res is not None, "Echo Forecast returned None"

prob_info = evaluate_trade_probability(closes_up, sabres_res, echo_res, payout=82.0)
print(f"Uptrend Result:")
print(f"  • Direction: {prob_info['direction'].upper()}")
print(f"  • Win Probability: {prob_info['probability']}%")
print(f"  • Effective Score: {prob_info['effective_score']}")
print(f"  • Echo Dir: {prob_info['echo_dir']} (Corr: {prob_info['echo_corr']})")
print(f"  • Sabres Dir: {prob_info['sabres_dir']}")

assert prob_info["direction"] == "up", f"Expected direction 'up', got {prob_info['direction']}"
assert prob_info["probability"] >= 50.0, f"Expected probability >= 50%, got {prob_info['probability']}"

# Generate sample closing prices (downtrend)
closes_down = [1.1500 - i * 0.0005 + (0.0001 if i % 2 == 0 else -0.0001) for i in range(160)]
sabres_down = calculate_ma_sabres(closes_down)
echo_down = calculate_echo_forecast(closes_down)

prob_down = evaluate_trade_probability(closes_down, sabres_down, echo_down, payout=80.0)
print(f"\nDowntrend Result:")
print(f"  • Direction: {prob_down['direction'].upper()}")
print(f"  • Win Probability: {prob_down['probability']}%")
print(f"  • Effective Score: {prob_down['effective_score']}")
assert prob_down["direction"] == "down", f"Expected direction 'down', got {prob_down['direction']}"

print(f"\nConfiguration Check:")
print(f"  • HOURLY_TRADE_ENABLED: {HOURLY_TRADE_ENABLED}")
print(f"  • HOURLY_TRADE_INTERVAL: {HOURLY_TRADE_INTERVAL}s ({HOURLY_TRADE_INTERVAL//60} mins)")
print(f"  • state.time_until_hourly_trade: {state.time_until_hourly_trade}s")

print("\n✅ All Hourly Probability tests passed successfully!")
