from datetime import datetime, timezone
import requests
from consts import BINANCE_REST_URL

def log(s: str):
    print(f'[{datetime.now(tz=timezone.utc)}] {s}')

def flatten(list_of_lists: list):
    return sum(list_of_lists, [])

def get_order_book_snapshot(symbol: str, limit: int=1000):
    """
    Function to fetch the initial order book snapshot
    """
    params = {'symbol': symbol.upper(), 'limit': limit}
    response = requests.get(BINANCE_REST_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        log(f'failed to fetch order book snapshot: {response.json()}')
        return None