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

class CsvStorage(StorageProvider):
    """
    Storage implementation for CSV files.
    """
    def __init__(self, directory: str = '.'):
        self.directory = directory
        self.is_first_save = True
        self.total_chunks_saved = 0

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
        })
        asks = pd.DataFrame({
            'timestamp': flatten([[x['timestamp'] for y in x['asks']] for x in updates]),
            'local_timestamp': flatten([[x['local_timestamp'] for y in x['asks']] for x in updates]),
            'side': ['ask' for _ in range(sum([len(x['asks']) for x in updates]))],
            'price': flatten([[y[0] for y in x['asks']] for x in updates]),
            'quantity': flatten([[y[1] for y in x['asks']] for x in updates]),
        })
        full_df = pd.concat([bids, asks]).sort_values(by='timestamp')
        # create path if it doesn't exist
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
        output_path = os.path.join(self.directory, 'incremental_book_updates.csv')
        # save to csv
        if self.is_first_save:
            # write mode if its the first write (override any existing files)
            full_df.to_csv(output_path, index=False)
            self.is_first_save = False
        else:
            # append mode without header for subsequent writes
            full_df.to_csv(output_path, index=False, mode='a', header=False)
        # update stats
        self.total_chunks_saved += 1
        # log
        logger.info(f'chunk #{self.total_chunks_saved}: saved {len(updates)} updates to {output_path}')
    