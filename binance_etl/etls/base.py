from typing import List
from binance_etl.library.model import ETLBase
from binance_etl.etls.spot_depth_updates_etl import SpotDepthUpdatesETL
from binance_etl.library.factory import get_storage_provider
from binance_etl.library.logger import get_logger
from binance_etl.library.utils import logger_name_with_symbol


logger = get_logger(__name__)

class SymbolETL(ETLBase):
    """
    Manages trading events ETL for a single symbol.
    """
    def __init__(self,
                 symbol: str,
                 markets: List[str],
                 events: List[str]):
        # params
        self.symbol = symbol
        self.markets = markets
        self.events = events
        # logger
        self.logger = get_logger(logger_name_with_symbol(__name__, self.symbol))
        # build ETLs
        self.etls = self._get_etls()
    
    def start(self):
        self.logger.info(f'starting ETLs for binance:{self.symbol}')
        for etl in self.etls:
            etl.start()
    
    def stop(self):
        self.logger.info(f'stopping ETLs for binance:{self.symbol}')
        for etl in self.etls:
            etl.stop()

    def _get_etls(self) -> List[ETLBase]:
        etls = []
        if 'depth_updates' in self.events and 'spot' in self.markets:
            # get storage provider
            symbol_id = f'{self.symbol}.spot'
            storage = get_storage_provider(symbol_id)
            etls.append(SpotDepthUpdatesETL(self.symbol, storage))
        if 'trades' in self.events and 'spot' in self.markets:
            # self.etls.append(SpotTradesETL(self.symbol))
            pass
        if 'depth_updates' in self.events and 'futures' in self.markets:
            # self.etls.append(FuturesDepthUpdatesETL(self.symbol))
            pass
        if 'trades' in self.events and 'futures' in self.markets:
            # self.etls.append(FuturesTradesETL(self.symbol))
            pass
        return etls