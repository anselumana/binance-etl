import os
from order_book_manager import OrderBookManager
from storage import CsvStorage


def get_order_book_manager():
    symbol = os.getenv('SYMBOL')
    price_resolution = os.getenv('PRICE_RESOLUTION')
    max_depth = os.getenv('MAX_DEPTH')
    time_resolution_in_seconds = os.getenv('TIME_RESOLUTION_IN_SECONDS')
    storage_batch_size = os.getenv('STORAGE_BATCH_SIZE')
    csv_storage_directory = os.getenv('CSV_STORAGE_DIRECTORY')
    storage = CsvStorage(directory=csv_storage_directory)
    return OrderBookManager(symbol=symbol,
                            price_resolution=float(price_resolution),
                            max_depth=int(max_depth),
                            time_resolution_in_seconds=int(time_resolution_in_seconds),
                            storage_provider=storage,
                            storage_batch_size=int(storage_batch_size))
