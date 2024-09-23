import json
import time
from typing import Any
import pandas as pd
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
from binance_etl.library.model import ETLBase
from binance_etl.library.storage import StorageProvider
from binance_etl.library.logger import get_logger
from binance_etl.library.utils import get_order_book_snapshot, logger_name_with_symbol


class SpotDepthUpdatesETL(ETLBase):
    """
    Manages the recording of incremental order book L2 updates.
    """
    def __init__(self,
                 symbol: str,
                 storage: StorageProvider):
        # set params
        self.symbol = symbol
        self.storage = storage
        # logger
        self.logger = get_logger(logger_name_with_symbol(__name__, self.symbol))
        # binance websocket manager
        self.binance_ws_client = SpotWebsocketStreamClient(on_message=self._process_message)
        # book synchronizer
        self.book_synchronizer = OrderBookSynchronizer(symbol)
        # state variables
        self.local_timestamp = 0             # arrival timestamp of websocket messages in ms
        self.last_book_update: dict = None   # last order book update
        # debug stats
        self.total_messages: int = 0
        self.total_bids: int = 0
        self.total_asks: int = 0
    
    def start(self):
        """
        Starts order book recording.
        """
        self.logger.info(f'starting depth updates ETL for binance:{self.symbol} spot pair')
        # connect to depth stream with our message handler
        self.binance_ws_client.diff_book_depth(symbol=self.symbol)
    
    def stop(self):
        """
        Gracefully stops order book recording.
        """
        self.logger.info(f'stopping depth updates ETL for binance:{self.symbol} spot pair')
        # close websocket connection
        self.binance_ws_client.stop()
        self._log_debug_stats()

    def _process_message(self, _: Any, message: str):
        """
        Order book depth update handler.
        """
        # update message arrival timestamp
        self.local_timestamp = int(time.time() * 1_000)
        # deresialize order book update
        update = self._deserialize_depth_message(message)
        if update is None:
            return
        self.logger.debug(f'processing update {update['last_update_id']} ({len(update['bids']) + len(update['asks'])} deltas)')
        # ensure current update is consistent with last received
        if not self._is_consistent(update):
            raise Exception('Unable to process update: it\'s not consistent with the last one received.')
        # try to sync the order book until successful
        if not self.book_synchronizer.is_synced:
            self.book_synchronizer.try_to_sync_book(update)
            if not self.book_synchronizer.is_synced:
                return
            else:
                # save updates
                self._save_initial_snapshot(self.book_synchronizer.initial_book_snapshot)
                for _update in self.book_synchronizer.book_updates:
                    self._save_update(_update)
        # save updates to storage
        self._save_update(update)
        # update debug stats
        self._update_debug_stats(update)
    
    def _deserialize_depth_message(self, message: str):
        """
        Deserializes depth update message and maps it to our model.
        """
        try:
            update = json.loads(message)
            return {
                'timestamp': update['E'], # ms
                'local_timestamp': self.local_timestamp,
                'bids': update['b'],
                'asks': update['a'],
                'first_update_id': update['U'],
                'last_update_id': update['u'],
            }
        except Exception as ex:
            self.logger.warning(f'Unable to deserialize depth update message: {ex}\nMessage body:\n{message}')
    
    def _is_consistent(self, update: dict):
        """
        Returns wether the current delta is consistent with the last received.
        """
        is_consistent = True
        if self.last_book_update is not None:
            first_update_id = update['first_update_id']
            previous_delta_last_update_id = self.last_book_update['last_update_id']
            if first_update_id != previous_delta_last_update_id + 1:
                self.logger.warning(f'inconsistent update: current update has a diff of {first_update_id - previous_delta_last_update_id + 1} updates from the last ({first_update_id} vs {previous_delta_last_update_id})')
                is_consistent = False
        self.last_book_update = update
        return is_consistent

    def _save_update(self, update, is_initial_snapshot = False):
        bids_range = range(len(update['bids']))
        bids = pd.DataFrame({
            'timestamp': [update['timestamp'] for _ in bids_range],
            'local_timestamp': [update['local_timestamp'] for _ in bids_range],
            'side': ['bid' for _ in bids_range],
            'price': [x[0] for x in update['bids']],
            'quantity': [x[1] for x in update['bids']],
            'is_snapshot': [is_initial_snapshot for _ in bids_range],
        })
        asks_range = range(len(update['asks']))
        asks = pd.DataFrame({
            'timestamp': [update['timestamp'] for _ in asks_range],
            'local_timestamp': [update['local_timestamp'] for _ in asks_range],
            'side': ['ask' for _ in asks_range],
            'price': [x[0] for x in update['asks']],
            'quantity': [x[1] for x in update['asks']],
            'is_snapshot': [is_initial_snapshot for _ in asks_range],
        })
        df = pd.concat([bids, asks]).sort_values(by=['timestamp', 'side'])
        self.storage.add_depth_updates(df)
    
    def _save_initial_snapshot(self, snapshot: dict):
        # we do -1 else it would have the same timestamp of the first update
        snapshot_timestamp = self.local_timestamp - 1
        mapped = {
            'timestamp': snapshot_timestamp,
            'local_timestamp': snapshot_timestamp,
            'bids': snapshot['bids'],
            'asks': snapshot['asks'],
        }
        self._save_update(mapped, is_initial_snapshot=True)
    
    def _update_debug_stats(self, update: dict):
        self.total_messages += 1
        self.total_bids += len(update['bids'])
        self.total_asks += len(update['asks'])

    def _log_debug_stats(self):
        self.logger.debug('')
        self.logger.debug(f'total depth update messages:     {self.total_messages}')
        self.logger.debug(f'total bids processed:            {self.total_bids}')
        self.logger.debug(f'total asks processed:            {self.total_asks}')
        self.logger.debug(f'average bids+asks per message:   {(self.total_bids + self.total_asks) / (self.total_messages or 1):.1f}')




class OrderBookSynchronizer:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.is_synced: bool = False
        self.initial_book_snapshot: dict = None
        self.book_updates: list = []
        # logger
        self.logger = get_logger(logger_name_with_symbol(__name__, self.symbol))

    def try_to_sync_book(self, update: dict):
        """
        Retrieves the initial order book snapshot, finds the first valid update
        updates state to reflect initial snapshot and valid updates relative to the snapshot.
        """
        self.logger.info(f'trying to sync order book...')
        # append update
        self.book_updates.append(update)
        # try to fetch initial snapshot
        if self.initial_book_snapshot is None:
            self.logger.info(f'fetching order book snapshot from the REST API')
            self.initial_book_snapshot = get_order_book_snapshot(self.symbol, limit=1000)
            if self.initial_book_snapshot is None:
                self.logger.warning(f'failed to sync: unable to get initial snapshot from the REST API')
                return
            self.logger.info(f'snapshot fetched (last update id: {self.initial_book_snapshot['lastUpdateId']})')
        last_update_id = self.initial_book_snapshot['lastUpdateId']
        valid_updates = [x for x in self.book_updates if x['last_update_id'] > last_update_id]
        if len(valid_updates) == 0:
            self.logger.warning(f'failed to sync: all buffered deltas are older than the snapshot')
            return
        # find the first update based on the initial snapshot
        first_update_to_process = None
        for update in valid_updates:
            if update['first_update_id'] <= last_update_id + 1 and update['last_update_id'] >= last_update_id + 1:
                first_update_to_process = update
                break
        if first_update_to_process is None:
            self.logger.warning(f'failed to sync: didn\'t find first update to process')
            return
        # keep only updates from first_update_to_process onward
        self.book_updates = [x for x in self.book_updates if x['first_update_id'] >= first_update_to_process['first_update_id']]
        self.logger.info(f'successfully synced book to first_update_id = {first_update_to_process['first_update_id']}')
        self.is_synced = True