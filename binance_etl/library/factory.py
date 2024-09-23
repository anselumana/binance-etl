from typing import List
from binance_etl.etls.spot_depth_etl import SpotDepthETL
from binance_etl.etls.spot_trades_etl import SpotTradesETL
from binance_etl.etls.base import BinanceETL
from binance_etl.library.storage import StorageProvider, CsvStorage
from binance_etl.library.utils import load_config


def get_etls() -> List[BinanceETL]:
    etls: List[BinanceETL] = []
    config = load_config()
    events: List[str] = config['events']
    for event in events:
        exchange, market, symbol, event_type = event.split('.')
        etl = get_etl(market, symbol, event_type)
        etls.append(etl)
    return etls

def get_etl(market: str, symbol: str, event_type: str):
    storage = get_storage_provider(market, symbol)
    if market == 'spot':
        if event_type == 'trade':
            return SpotTradesETL(symbol, storage)
        if event_type == 'depth':
            return SpotDepthETL(symbol, storage)
    if market == 'usdm_futures':
        pass
    if market == 'coinm_futures':
        pass
    raise Exception(f'No ETL found for @{event_type} events on {market} {symbol}')


def get_storage_provider(market: str, symbol: str) -> StorageProvider:
    config = load_config()
    storage = config['storage']
    if storage['enabled'] == 'csv':
        batch_size = storage['csv']['batch_size']
        base_path = storage['csv']['base_path']
        return CsvStorage(market, symbol, batch_size, base_path)
    if storage['enabled'] == 'bigquery':
        # todo: support bigquery
        pass
    raise Exception(f'could not find any enabled storage providers in config')