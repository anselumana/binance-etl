import json
import requests
from binance_etl.library.logger import get_logger
from binance_etl.library.consts import BINANCE_REST_URL


logger = get_logger(__name__)

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
        logger.warning(f'failed to fetch order book snapshot: {response.json()}')
        return None

def is_none_or_empty(s: str) -> bool:
    return s is None or s.strip() == ''

def load_config(path: str = None) -> dict:
    with open(path or './config.json') as config:
        return json.loads(config.read())