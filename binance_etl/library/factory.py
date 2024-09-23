from binance_etl.library.storage import StorageProvider, CsvStorage
from binance_etl.library.utils import load_config


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