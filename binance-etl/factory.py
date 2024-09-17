import os
from order_book_manager import OrderBookManager


def get_order_book_manager():
    symbol = os.getenv('SYMBOL')
    return OrderBookManager(symbol)