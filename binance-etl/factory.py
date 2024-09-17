import os
from order_book_manager import OrderBookManager


def get_order_book_manager():
    symbol = os.getenv('SYMBOL')
    price_resolution = os.getenv('PRICE_RESOLUTION')
    max_depth = os.getenv('MAX_DEPTH')
    return OrderBookManager(symbol,
                            float(price_resolution),
                            int(max_depth))
