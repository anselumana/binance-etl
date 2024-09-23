import os
import pandas as pd
from binance_etl.library.logger import get_logger
from binance_etl.library.utils import logger_name_with_symbol


class StorageProvider:
    """
    Base class for storage provider implementations.
    """
    def __init__(self, symbol_id: str, batch_size: int):
        self.symbol_id = symbol_id
        self.batch_size = batch_size
        self.depth_updates_buffer = pd.DataFrame()
        self.trades_buffer = pd.DataFrame()
        self.depth_updates_batches_saved = 0
        self.trades_batches_saved = 0
        # logger
        self.logger = get_logger(logger_name_with_symbol(__name__, self.symbol_id))

    def add_depth_updates(self, depth_updates: pd.DataFrame):
        self.depth_updates_buffer = pd.concat([self.depth_updates_buffer, depth_updates])
        if len(self.depth_updates_buffer) >= self.batch_size:
            self._save_depth_updates()
            self.depth_updates_batches_saved += 1
            self.logger.info(f'chunk #{self.depth_updates_batches_saved}: saved {len(self.depth_updates_buffer)} depth updates')
            self.depth_updates_buffer = pd.DataFrame()
    
    def add_trades(self, trades: pd.DataFrame):
        self.trades_buffer = pd.concat([self.trades_buffer, trades])
        if len(self.trades_buffer) >= self.batch_size:
            self._save_trades()
            self.trades_batches_saved += 1
            self.logger.info(f'chunk #{self.trades_batches_saved}: saved {len(self.trades_buffer)} trades')
            self.trades_buffer = pd.DataFrame()
    
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
    def __init__(self, symbol_id: str, batch_size: int, base_path: str):
        super().__init__(symbol_id, batch_size)
        self.depth_updates_path = os.path.join(base_path, f'{self.symbol_id}.depth_updates.csv')
        self.trades_path = os.path.join(base_path, f'{self.symbol_id}.trades.csv')
        self._create_file_with_directories(self.depth_updates_path)
        self._create_file_with_directories(self.trades_path)

    def _save_depth_updates(self):
        """
        Saves depth updates to csv.
        """
        write_header = self.depth_updates_batches_saved == 0
        self.depth_updates_buffer.to_csv(self.depth_updates_path, index=False, mode='a', header=write_header)
        
    def _create_file_with_directories(self, file_path: str):
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(file_path, 'w') as file:
            file.write('')
    