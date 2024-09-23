from binance.spot import Spot
from binance_etl.library.logger import get_logger
from binance_etl.library.utils import get_logger_name

class OrderBookSynchronizer:
    def __init__(self, market: str, symbol: str):
        self.market = market
        self.symbol = symbol
        self.is_synced: bool = False
        self.initial_book_snapshot: dict = None
        self.book_updates: list = []
        # binance REST client
        self.binance_client = Spot()
        # logger
        self.logger = get_logger(get_logger_name(__name__, market, symbol, 'depth'))

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
            try:
                self.initial_book_snapshot = self.binance_client.depth(self.symbol.upper(), limit=1000)
                self.logger.info(f'snapshot fetched (last update id: {self.initial_book_snapshot['lastUpdateId']})')
            except Exception as ex:
                self.logger.warning(f'failed to sync: unable to get initial snapshot from the REST API: {ex}')
                return
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