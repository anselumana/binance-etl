from datetime import datetime, timezone
import requests

def log(s: str):
    print(f'[{datetime.now(tz=timezone.utc)}] {s}')

def get_order_book_snapshot(url: str, symbol: str, limit: int=1000):
    """
    Function to fetch the initial order book snapshot
    """
    params = {'symbol': symbol, 'limit': limit}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        log("failed to fetch order book snapshot:", response.status_code)
        return None