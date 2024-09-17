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

    def save(self, snapshots: list):
        pass

class CsvStorage(StorageProvider):
    """
    Storage implementation for CSV files.
    """
    def __init__(self, directory: str = '.'):
        self.directory = directory
        self.is_first_save = True
        self.total_chunks_saved = 0

    def save(self, snapshots: list):
        """
        Saves book snapshots to csv in append mode.
        """
        logger.info(f'saving order book...')
        # build dataframes
        bids = pd.DataFrame({
            'time': flatten([[datetime.fromtimestamp(x['t'] // 1000, tz=timezone.utc) for y in x['bids']] for x in snapshots]),
            'price': flatten([[y['p'] for y in x['bids']] for x in snapshots]),
            'quantity': flatten([[y['q'] for y in x['bids']] for x in snapshots]),
        })
        asks = pd.DataFrame({
            'time': flatten([[datetime.fromtimestamp(x['t'] // 1000, tz=timezone.utc) for y in x['asks']] for x in snapshots]),
            'price': flatten([[y['p'] for y in x['asks']] for x in snapshots]),
            'quantity': flatten([[y['q'] for y in x['asks']] for x in snapshots]),
        })
        # create path if it doesn't exist
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
        bids_csv = os.path.join(self.directory, 'bids.csv')
        asks_csv = os.path.join(self.directory, 'asks.csv')
        # save to csv
        if self.is_first_save:
            # write mode if its the first write (override any existing files)
            bids.to_csv(bids_csv, index=False)
            asks.to_csv(asks_csv, index=False)
            self.is_first_save = False
        else:
            # append mode without header for subsequent writes
            bids.to_csv(bids_csv, index=False, mode='a', header=False)
            asks.to_csv(asks_csv, index=False, mode='a', header=False)
        # update stats
        self.total_chunks_saved += 1
        # log
        logger.info(f'chunk #{self.total_chunks_saved}: saved {len(snapshots)} book snapshots to {bids_csv} and {asks_csv}')
    