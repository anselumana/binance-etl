import os
import pandas as pd
from binance_etl.library.logger import get_logger
from binance_etl.library.utils import get_logger_name


class StorageProvider:
    """
    Base class for storage provider implementations.
    """
    def __init__(self, market: str, symbol: str, batch_size: int):
        self.market = market
        self.symbol = symbol
        self.batch_size = batch_size
        self.depth_updates_df = pd.DataFrame()
        self.trades_df = pd.DataFrame()
        self.depth_updates_batches_saved = 0
        self.trades_batches_saved = 0
        # logger
        self.logger = get_logger(get_logger_name(__name__, market, symbol))

    def add_depth_updates(self, depth_updates: pd.DataFrame):
        self.depth_updates_df = pd.concat([self.depth_updates_df, depth_updates])
        if len(self.depth_updates_df) >= self.batch_size:
            self._save_depth_updates()
            self.depth_updates_batches_saved += 1
            self.logger.info(f'chunk #{self.depth_updates_batches_saved}: saved {len(self.depth_updates_df)} depth updates')
            self.depth_updates_df = pd.DataFrame()
    
    def add_trades(self, trades: pd.DataFrame):
        self.trades_df = pd.concat([self.trades_df, trades])
        if len(self.trades_df) >= self.batch_size:
            self._save_trades()
            self.trades_batches_saved += 1
            self.logger.info(f'chunk #{self.trades_batches_saved}: saved {len(self.trades_df)} trades')
            self.trades_df = pd.DataFrame()
    
    def _save_depth_updates(self):
        "To implement in subclasses"
        raise NotImplementedError()
    
    def _save_trades(self):
        "To implement in subclasses"
        raise NotImplementedError()


class CsvStorage(StorageProvider):
    """
    CSV implementation of storage.
    """
    def __init__(self,
                 market: str,
                 symbol: str,
                 batch_size: int,
                 base_path: str):
        super().__init__(market, symbol, batch_size)
        symbol_id = f'{self.symbol}.{self.market}'
        self.depth_updates_path = os.path.join(base_path, f'{symbol_id}.depth.csv')
        self.trades_path = os.path.join(base_path, f'{symbol_id}.trades.csv')
        self._create_file_with_directories(self.depth_updates_path)
        self._create_file_with_directories(self.trades_path)

    def _save_depth_updates(self):
        """
        Saves depth updates to csv.
        """
        write_header = self.depth_updates_batches_saved == 0
        self.depth_updates_df.to_csv(self.depth_updates_path, index=False, mode='a', header=write_header)

    def _save_trades(self):
        """
        Saves trades to csv.
        """
        write_header = self.trades_batches_saved == 0
        self.trades_df.to_csv(self.trades_path, index=False, mode='a', header=write_header)
        
    def _create_file_with_directories(self, file_path: str):
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(file_path, 'w') as file:
            file.write('')
    