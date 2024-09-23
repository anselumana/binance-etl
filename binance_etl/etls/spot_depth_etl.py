import json
import pandas as pd
from binance_etl.etls.base import BinanceETL
from binance_etl.library.storage import StorageProvider
from binance_etl.library.book_utils import OrderBookSynchronizer


class SpotDepthETL(BinanceETL):
    """
    ETL to record spot order book depth updates.
    """
    def __init__(self, symbol: str, storage: StorageProvider):
        super().__init__('spot', symbol, 'depth', storage)
        # book synchronizer for init sync
        self.book_synchronizer = OrderBookSynchronizer('spot', symbol)
        # last order book update
        self.last_book_update: dict = None
    
    def start(self):
        """
        Starts depth recording.
        """
        self.logger.info(f'starting depth ETL')
        # connect to depth stream
        self.binance_ws_client.diff_book_depth(symbol=self.symbol)
        super().start()
    
    def stop(self):
        """
        Gracefully stops order book recording.
        """
        self.logger.info(f'stopping depth ETL')
        super().stop()

    def _handle_message(self, message: dict):
        """
        Depth message handler.
        """
        self.logger.debug(f'processing update {message['last_update_id']} ({len(message['bids']) + len(message['asks'])} deltas)')
        # ensure current update is consistent with last received
        if not self._is_consistent(message):
            raise Exception('Unable to process update: it\'s not consistent with the last one received.')
        # try to sync the order book until successful
        if not self.book_synchronizer.is_synced:
            self.book_synchronizer.try_to_sync_book(message)
            if not self.book_synchronizer.is_synced:
                return
            else:
                # save updates
                self._save_initial_snapshot(self.book_synchronizer.initial_book_snapshot)
                for _update in self.book_synchronizer.book_updates:
                    self._save_update(_update)
        # save updates to storage
        self._save_update(message)
    
    def _deserialize_message(self, message: str) -> dict:
        """
        Deserializes depth message.\n
        https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#diff-depth-stream
        """
        res = None
        try:
            update = json.loads(message)
            # ignore events that are not 'depthUpdate' (which is usually only the first message)
            if 'e' in update.keys() and update['e'] == 'depthUpdate':
                res = {
                    'timestamp': update['E'], # ms
                    'local_timestamp': self.local_timestamp,
                    'bids': update['b'],
                    'asks': update['a'],
                    'first_update_id': update['U'],
                    'last_update_id': update['u'],
                }
        except Exception as ex:
            self.logger.warning(f'Unable to deserialize depth update message: {ex}\nMessage body:\n{message}')
        return res
    
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

    def _save_update(self, update: dict, is_initial_snapshot = False):
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
