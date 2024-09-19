import os
import pandas as pd
from datetime import datetime, timezone
from logger import get_logger
from utils import flatten


logger = get_logger(__name__)

class StorageProvider:
    """
    Base interface for storage providers.
    """
    def __init__(self):
        pass
    def save_updates(self, updates: list):
        pass
    def save_snapshot(self, snapshot: dict):
        pass

class CsvStorage(StorageProvider):
    """
    Storage implementation for CSV files.
    """
    def __init__(self, directory: str = '.'):
        self.directory = directory
        self.output_path = os.path.join(self.directory, 'incremental_book.csv')
        self.total_chunks_saved = 0
        self._create_file_with_directories(self.output_path)

    def save_updates(self, updates: list):
        """
        Saves book incremental updates to csv in append mode.
        """
        logger.info(f'saving order book updates...')
        # build dataframes
        bids = pd.DataFrame({
            'timestamp': flatten([[x['timestamp'] for y in x['bids']] for x in updates]),
            'local_timestamp': flatten([[x['local_timestamp'] for y in x['bids']] for x in updates]),
            'side': ['bid' for _ in range(sum([len(x['bids']) for x in updates]))],
            'price': flatten([[y[0] for y in x['bids']] for x in updates]),
            'quantity': flatten([[y[1] for y in x['bids']] for x in updates]),
            'is_snapshot': [False for _ in range(sum([len(x['bids']) for x in updates]))],
        })
        asks = pd.DataFrame({
            'timestamp': flatten([[x['timestamp'] for y in x['asks']] for x in updates]),
            'local_timestamp': flatten([[x['local_timestamp'] for y in x['asks']] for x in updates]),
            'side': ['ask' for _ in range(sum([len(x['asks']) for x in updates]))],
            'price': flatten([[y[0] for y in x['asks']] for x in updates]),
            'quantity': flatten([[y[1] for y in x['asks']] for x in updates]),
            'is_snapshot': [False for _ in range(sum([len(x['asks']) for x in updates]))],
        })
        full_df = pd.concat([bids, asks]).sort_values(by=['timestamp', 'side'])
        # append to csv
        full_df.to_csv(self.output_path, index=False, mode='a', header=False)
        # update stats
        self.total_chunks_saved += 1
        # log
        logger.info(f'chunk #{self.total_chunks_saved}: saved {len(updates)} updates to {self.output_path}')
    
    def save_snapshot(self, snapshot: dict):
        logger.info(f'saving order snapshot...')
        # build dataframes
        bids = pd.DataFrame({
            'timestamp': [snapshot['timestamp'] for x in snapshot['bids']],
            'local_timestamp': [snapshot['local_timestamp'] for x in snapshot['bids']],
            'side': ['bid' for _ in range(len(snapshot['bids']))],
            'price': [x[0] for x in snapshot['bids']],
            'quantity': [x[1] for x in snapshot['bids']],
            'is_snapshot': [True for _ in range(len(snapshot['bids']))],
        })
        asks = pd.DataFrame({
            'timestamp': [snapshot['timestamp'] for x in snapshot['asks']],
            'local_timestamp': [snapshot['local_timestamp'] for x in snapshot['asks']],
            'side': ['ask' for _ in range(len(snapshot['asks']))],
            'price': [x[0] for x in snapshot['asks']],
            'quantity': [x[1] for x in snapshot['asks']],
            'is_snapshot': [True for _ in range(len(snapshot['asks']))],
        })
        full_df = pd.concat([bids, asks]).sort_values(by=['timestamp', 'side'])
        # append to csv
        full_df.to_csv(self.output_path, index=False, mode='a')
        # log
        logger.info(f'snapshot saved')
        
    
    def _create_file_with_directories(self, file_path: str):
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(file_path, 'w') as file:
            file.write('')
    