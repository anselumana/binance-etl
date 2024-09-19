import os
import consts
from model import ETLBase
from utils import is_none_or_empty
from storage import StorageProvider, CsvStorage
from incremental_book_etl import IncrementalBookETL


def get_incremental_book_etl() -> ETLBase:
    # get env
    symbol = os.getenv(consts.ENV_VAR_SYMBOL)
    storage_batch_size = os.getenv(consts.ENV_VAR_STORAGE_BATCH_SIZE)
    # validate env
    raise_if_invalid_env(symbol=symbol,
                         storage_batch_size=storage_batch_size)
    # instanciate storage provider
    storage = get_storage_provider()
    return IncrementalBookETL(symbol=symbol,
                            storage_provider=storage,
                            storage_batch_size=int(storage_batch_size))

def get_storage_provider() -> StorageProvider:
    csv_storage_directory = os.getenv(consts.ENV_VAR_CSV_STORAGE_DIRECTORY)
    return CsvStorage(directory=csv_storage_directory or '.')


def raise_if_invalid_env(symbol: str | None,
                        storage_batch_size: str | None):
    if is_none_or_empty(symbol):
        raise Exception(f'env variable {consts.ENV_VAR_SYMBOL} cannot be None or empty')
    if is_none_or_empty(storage_batch_size):
        raise Exception(f'env variable {consts.ENV_VAR_STORAGE_BATCH_SIZE} cannot be None or empty')