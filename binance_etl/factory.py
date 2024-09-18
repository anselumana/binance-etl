import os
from order_book_manager import OrderBookManager
from storage import StorageProvider, CsvStorage
import consts
from utils import is_none_or_empty


def get_order_book_manager() -> OrderBookManager:
    # get env
    symbol = os.getenv(consts.ENV_VAR_SYMBOL)
    levels_per_side = os.getenv(consts.ENV_VAR_LEVELS_PER_SIDE)
    price_resolution = os.getenv(consts.ENV_VAR_PRICE_RESOLUTION)
    time_resolution_in_seconds = os.getenv(consts.ENV_VAR_TIME_RESOLUTION_IN_SECONDS)
    storage_batch_size = os.getenv(consts.ENV_VAR_STORAGE_BATCH_SIZE)
    # validate env
    raise_if_invalid_env(symbol=symbol,
                         price_resolution=price_resolution,
                         time_resolution_in_seconds=time_resolution_in_seconds,
                         levels_per_side=levels_per_side,
                         storage_batch_size=storage_batch_size)
    # instanciate storage provider
    storage = get_storage_provider()
    return OrderBookManager(symbol=symbol,
                            price_resolution=float(price_resolution),
                            time_resolution_in_seconds=int(time_resolution_in_seconds),
                            levels_per_side=int(levels_per_side),
                            storage_provider=storage,
                            storage_batch_size=int(storage_batch_size))

def get_storage_provider() -> StorageProvider:
    csv_storage_directory = os.getenv(consts.ENV_VAR_CSV_STORAGE_DIRECTORY)
    return CsvStorage(directory=csv_storage_directory or '.')


def raise_if_invalid_env(symbol: str | None,
                        price_resolution: str | None,
                        time_resolution_in_seconds: str | None,
                        levels_per_side: str | None,
                        storage_batch_size: str | None):
    if is_none_or_empty(symbol):
        raise Exception(f'env variable {consts.ENV_VAR_SYMBOL} cannot be None or empty')
    if is_none_or_empty(price_resolution):
        raise Exception(f'env variable {consts.ENV_VAR_PRICE_RESOLUTION} cannot be None or empty')
    if is_none_or_empty(time_resolution_in_seconds):
        raise Exception(f'env variable {consts.ENV_VAR_TIME_RESOLUTION_IN_SECONDS} cannot be None or empty')
    if is_none_or_empty(levels_per_side):
        raise Exception(f'env variable {consts.ENV_VAR_LEVELS_PER_SIDE} cannot be None or empty')
    if is_none_or_empty(storage_batch_size):
        raise Exception(f'env variable {consts.ENV_VAR_STORAGE_BATCH_SIZE} cannot be None or empty')