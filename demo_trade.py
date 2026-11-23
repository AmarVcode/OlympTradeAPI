import asyncio
import os
import sys
from olymptrade_ws import OlympTradeClient

def get_token() -> str:
    # 1. Check environment variable
    token = os.environ.get("OLYMP_ACCESS_TOKEN")
    if token and len(token.strip()) > 50:
        return token.strip()
    
    # 2. Check .env file
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("OLYMP_ACCESS_TOKEN="):
                    token = line.split("=", 1)[1].strip().strip('"').strip("'")
                    if len(token) > 50:
                        return token

    # 3. Check simple_bot.py / config
    print("\n[!] No access token found in environment or .env file.")
    token = input("Please paste your OlympTrade access_token: ").strip()
    return token

async def run_demo_trade(token: str):
    print("=" * 60)
    print("🚀 Initializing OlympTrade Demo Trader...")
    print("=" * 60)

    client = OlympTradeClient(
        access_token=token,
        log_raw_messages=False
    )

    try:
        print("[1/4] Connecting to WebSocket...")
        await client.start()
        print("Connected successfully!")

        print("[2/4] Retrieving demo account balance...")
        balance_info = await client.balance.get_balance(timeout=15.0)
        
        demo_account = None
        if balance_info and 'd' in balance_info and isinstance(balance_info['d'], list):
            for acc in balance_info['d']:
                if acc.get('group') == 'demo':
                    demo_account = acc
                    break

        if not demo_account:
            print("❌ Could not find a Demo account in the account list.")
            print(f"Server response: {balance_info}")
            return

        account_id = demo_account.get('account_id')
        currency = demo_account.get('currency', 'USD')
        amount = demo_account.get('amount', 0)
        print(f" Demo Account ID: {account_id}")
        print(f" Demo Balance: {amount} {currency}")

        print("\n[3/4] Fetching latest market candles for LATAM_X (1-minute timeframe)...")
        pair = "LATAM_X"
        candles = await client.market.get_candles(pair, size=60, count=5)
        
        direction = "up"
        if candles and len(candles) > 0:
            last = candles[-1]
            close_p = last.get('close', 0)
            open_p = last.get('open', 0)
            direction = "up" if close_p >= open_p else "down"
            print(f" Last candle: Open={open_p}, Close={close_p} -> Signal: {direction.upper()}")
        else:
            print(f"⚠️ Could not retrieve candles, defaulting direction to '{direction}'.")

        print(f"\n[4/4] Placing Demo Order: Pair={pair}, Direction={direction.upper()}, Amount=1, Duration=60s...")
        order_result = await client.trade.place_order(
            pair=pair,
            amount=1,
            direction=direction,
            duration=60,
            account_id=account_id,
            group="demo"
        )

        print("\n" + "=" * 60)
        print("🎉 TRADE PLACED SUCCESSFULLY ON DEMO ACCOUNT!")
        print("=" * 60)
        print(f"Order Result: {order_result}\n")

    except Exception as e:
        print(f"\n❌ Error during demo trading: {e}")
    finally:
        await client.stop()
        print("🔌 Client disconnected.")

if __name__ == "__main__":
    token = get_token()
    if not token or len(token) < 50:
        print("❌ Invalid token provided. Token must be a valid JWT.")
        sys.exit(1)
    asyncio.run(run_demo_trade(token))
