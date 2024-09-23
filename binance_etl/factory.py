import os
import binance_etl.consts as consts
from binance_etl.model import ETLBase
from binance_etl.spot_depth_updates_etl import SpotDepthUpdatesETL
from binance_etl.storage import StorageProvider, CsvStorage
from binance_etl.utils import is_none_or_empty, load_config


def get_incremental_book_etl() -> ETLBase:
    # get env
    symbol = os.getenv(consts.ENV_VAR_SYMBOL)
    storage_batch_size = os.getenv(consts.ENV_VAR_STORAGE_BATCH_SIZE)
    # validate env
    raise_if_invalid_env(symbol=symbol,
                         storage_batch_size=storage_batch_size)
    # instanciate storage provider
    storage = get_storage_provider()
    return SpotDepthUpdatesETL(symbol=symbol,
                            storage=storage,
                            storage_batch_size=int(storage_batch_size))

def get_storage_provider(symbol_id: str) -> StorageProvider:
    config = load_config()
    storage_config = config['storage']
    if bool(storage_config['csv_storage_enabled']):
        batch_size = storage_config['csv_batch_size']
        base_path = storage_config['csv_base_path']
        return CsvStorage(symbol_id, batch_size, base_path)
    if bool(storage_config['bigquery_storage_enabled']):
        # todo: support bigquery
        pass
    raise Exception(f'could not find any enabled storage providers in config')


def raise_if_invalid_env(symbol: str | None,
                        storage_batch_size: str | None):
    if is_none_or_empty(symbol):
        raise Exception(f'env variable {consts.ENV_VAR_SYMBOL} cannot be None or empty')
    if is_none_or_empty(storage_batch_size):
        raise Exception(f'env variable {consts.ENV_VAR_STORAGE_BATCH_SIZE} cannot be None or empty')