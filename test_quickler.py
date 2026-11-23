import asyncio
import os
from olymptrade_ws import OlympTradeClient

def get_token():
    with open('.env', 'r', encoding='utf-8') as f:
        for line in f:
            if line.startswith('OLYMP_ACCESS_TOKEN='):
                return line.split('=', 1)[1].strip()
    return None

async def main():
    token = get_token()
    client = OlympTradeClient(access_token=token, log_raw_messages=False)
    
    trade_closed_event = asyncio.Event()
    closed_data = []

    async def on_trade_closed(msg):
        print("\n🔔 EVENT 26 RECEIVED (Trade Closed):")
        print(msg)
        closed_data.append(msg)
        trade_closed_event.set()

    # Register for Trade Closed (Event 26)
    client.register_callback(26, on_trade_closed)

    await client.start()
    balance_info = await client.balance.get_balance()
    demo_acc = next(acc for acc in balance_info['d'] if acc.get('group') == 'demo')
    account_id = demo_acc['account_id']
    print(f"Demo Account ID: {account_id}, Current Demo Balance: {demo_acc['amount']}")

    print("\n⚡ Testing 1 trade on ULTRA_X (Quickler, 5 seconds)...")
    res = await client.trade.place_order(
        pair="ULTRA_X",
        amount=1,
        direction="up",
        duration=5,
        account_id=account_id,
        group="demo"
    )
    print(f"Order placement result: {res}")

    if res and res.get('id'):
        trade_id = res['id']
        print(f"Tracking trade ID {trade_id}... waiting for completion (5s duration)...")
        try:
            await asyncio.wait_for(trade_closed_event.wait(), timeout=12.0)
            print("Trade completed and confirmed!")
        except asyncio.TimeoutError:
            print("Timeout waiting for event 26.")
    
    await client.stop()
    print("Done.")

if __name__ == "__main__":
    asyncio.run(main())
