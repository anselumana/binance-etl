import json
import time
from binance_websocket import BinanceWebSocket
from logger import get_logger
from utils import get_order_book_snapshot
from storage import StorageProvider
from model import ETLBase


logger = get_logger(__name__)

class IncrementalBookETL(ETLBase):
    """
    Manages the recording of incremental order book L2 updates.
    """
    def __init__(self,
                 symbol: str,
                 storage_provider: StorageProvider,
                 storage_batch_size: int = 100):
        # params validation
        self._raise_if_invalid_params(symbol, storage_provider, storage_batch_size)
        # set params
        self.symbol = symbol
        self.storage_provider = storage_provider
        self.storage_batch_size = storage_batch_size
        # binance websocket manager
        self.binance_websocket = BinanceWebSocket(on_message=self._process_message)
        # book synchronizer
        self.book_synchronizer = OrderBookSynchronizer(symbol)
        # state variables
        self.book_updates: list = []               # order book updates
        self.last_book_update: dict = None         # last order book update
        self.initial_book_snapshot: dict = None    # initial snapshot fetched from the REST API
        self.local_timestamp = 0                   # arrival timestamp of updates in ms
        # debug stats
        self.total_updates: int = 0
        self.total_bids: int = 0
        self.total_asks: int = 0
    
    def start(self):
        """
        Starts order book recording.
        """
        logger.info(f'starting order book recording for binance:{self.symbol}')
        # connect to depth stream with our message handler
        self.binance_websocket.connect_to_depth_stream(self.symbol)
    
    def stop(self):
        """
        Gracefully stops order book recording.
        """
        logger.info(f'stopping order book recording for binance:{self.symbol}')
        # close websocket connection
        self.binance_websocket.close_connection()
        self._log_debug_stats()

    def _process_message(self, message: str):
        """
        Order book depth update handler.
        """
        # update message arrival timestamp
        self.local_timestamp = int(time.time() * 1_000)
        # deresialize order book update
        update = self._deserialize_depth_message(message)
        # ensure current update is consistent with last received
        if not self._is_consistent(update):
            raise Exception('Unable to process update: it\'s not consistent with the last one received.')
        # try to sync the order book until successful
        if not self.book_synchronizer.is_synced:
            self.book_synchronizer.try_to_sync_book(update)
            if self.book_synchronizer.is_synced:
                self.initial_book_snapshot = self.book_synchronizer.initial_book_snapshot
                self.book_updates = self.book_synchronizer.book_updates
                self._save_snapshot(self.initial_book_snapshot)
            return
        # record update
        self.book_updates.append(update)
        logger.debug(f'processed update {update['last_update_id']} ({len(update['bids']) + len(update['asks'])} deltas)')
        # save updates to storage
        if self._should_save_updates():
            self._save_updates()
    
    def _deserialize_depth_message(self, message: str):
        """
        Deserializes depth update message and maps it to our model.
        """
        update = json.loads(message)
        return {
            'timestamp': update['E'], # ms
            'local_timestamp': self.local_timestamp,
            'bids': update['b'],
            'asks': update['a'],
            'first_update_id': update['U'],
            'last_update_id': update['u'],
        }
    
    def _is_consistent(self, update: dict):
        """
        Returns wether the current delta is consistent with the last received.
        """
        is_consistent = True
        if self.last_book_update is not None:
            first_update_id = update['first_update_id']
            previous_delta_last_update_id = self.last_book_update['last_update_id']
            if first_update_id != previous_delta_last_update_id + 1:
                logger.warning(f'inconsistent update: current update has a diff of {first_update_id - previous_delta_last_update_id + 1} updates from the last ({first_update_id} vs {previous_delta_last_update_id})')
                is_consistent = False
        self.last_book_update = update
        return is_consistent

    def _should_save_updates(self):
        return len(self.book_updates) == self.storage_batch_size

    def _save_updates(self):
        updates = self.book_updates
        # save current batch
        self.storage_provider.save_updates(updates)
        # update stats
        self.total_updates += len(updates)
        self.total_bids += sum([len(x['bids']) for x in updates])
        self.total_asks += sum([len(x['asks']) for x in updates])
        # clear memory
        self.book_updates = []
    
    def _save_snapshot(self, snapshot: dict):
        # we do -1 else it would have the same timestamp of the first update
        snapshot_timestamp = self.local_timestamp - 1
        mapped = {
            'timestamp': snapshot_timestamp,
            'local_timestamp': snapshot_timestamp,
            'bids': snapshot['bids'],
            'asks': snapshot['asks'],
        }
        self.storage_provider.save_snapshot(mapped)
    
    def _raise_if_invalid_params(self,
                                 symbol: str,
                                 storage_provider: StorageProvider | None,
                                 storage_batch_size: int):
        if not isinstance(storage_provider, StorageProvider):
            raise Exception(f'invalid parameter \'storage_provider\': must implement the StorageProvider interface.')
        if storage_batch_size <= 0:
            raise Exception(f'invalid parameter \'storage_batch_size\': must be > 0.')
        if storage_batch_size > 1_000_000:
            raise Exception(f'invalid parameter \'storage_batch_size\': must be <= 1\'000\'000.')
    
    def _log_debug_stats(self):
        logger.debug('')
        logger.debug(f'total book updates processed:           {self.total_updates}')
        logger.debug(f'total bids processed:                   {self.total_bids}')
        logger.debug(f'total asks processed:                   {self.total_asks}')
        logger.debug(f'average bids+asks per update: {(self.total_bids + self.total_asks) / (self.total_updates or 1):.1f}')




class OrderBookSynchronizer:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.is_synced: bool = False
        self.initial_book_snapshot: dict = None
        self.book_updates: list = []

    def try_to_sync_book(self, update: dict):
        """
        Retrieves the initial order book snapshot, finds the first valid update
        updates state to reflect initial snapshot and valid updates relative to the snapshot.
        """
        logger.info(f'trying to sync order book...')
        # append update
        self.book_updates.append(update)
        # try to fetch initial snapshot
        if self.initial_book_snapshot is None:
            logger.info(f'fetching order book snapshot from the REST API')
            self.initial_book_snapshot = get_order_book_snapshot(self.symbol, limit=1000)
            if self.initial_book_snapshot is None:
                logger.warning(f'failed to sync: unable to get initial snapshot from the REST API')
                return
            logger.info(f'snapshot fetched (last update id: {self.initial_book_snapshot['lastUpdateId']})')
        last_update_id = self.initial_book_snapshot['lastUpdateId']
        valid_updates = [x for x in self.book_updates if x['last_update_id'] > last_update_id]
        if len(valid_updates) == 0:
            logger.warning(f'failed to sync: all buffered deltas are older than the snapshot')
            return
        # find the first update based on the initial snapshot
        first_update_to_process = None
        for update in valid_updates:
            if update['first_update_id'] <= last_update_id + 1 and update['last_update_id'] >= last_update_id + 1:
                first_update_to_process = update
                break
        if first_update_to_process is None:
            logger.warning(f'failed to sync: didn\'t find first update to process')
            return
        # keep only updates from first_update_to_process onward
        self.book_updates = [x for x in self.book_updates if x['first_update_id'] >= first_update_to_process['first_update_id']]
        logger.info(f'successfully synced book to first_update_id = {first_update_to_process['first_update_id']}')
        self.is_synced = True