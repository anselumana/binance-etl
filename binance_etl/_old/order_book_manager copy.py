import copy
import json
from datetime import datetime, timezone
import pandas as pd
import websocket
from logger import get_logger
from utils import get_order_book_snapshot
from consts import BINANCE_WEBSOCKET_URL
from storage import StorageProvider


logger = get_logger(__name__)

class OrderBookManager:
    """
    Manages the local order book mirror.\n
    Ref: https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly.
    """
    def __init__(self,
                 symbol: str,
                 price_resolution: float = 1,
                 time_resolution_in_seconds: int = 1,
                 levels_per_side = 50,
                 storage_provider: StorageProvider = None,
                 storage_batch_size: int = 100):
        # params validation
        self._raise_if_invalid_params(
            symbol,
            price_resolution,
            time_resolution_in_seconds,
            levels_per_side,
            storage_provider,
            storage_batch_size)
        # set params
        self.symbol = symbol
        self.price_resolution = price_resolution
        self.time_resolution_in_seconds = time_resolution_in_seconds
        self.levels_per_side = levels_per_side
        self.storage_provider = storage_provider
        self.storage_batch_size = storage_batch_size
        # web socket client
        self.ws: websocket.WebSocketApp = None
        # state variables
        self.current_book: dict = None             # current state of the order book at tick-level resolution
        self.previous_book: dict = None            # previous state of the order book at tick-level resolution
        self.book_snapshots: list = []             # order book snapshots with resolution = price_resolution * time_resolution_in_seconds
        self.last_book_update: dict = None         # last order book update (used to check consistency of subsequent updates)
        self.initial_book_snapshot: dict = None    # initial snapshot fetched from the REST API     (only used for initial sync)
        self.book_updates: list = []               # order book updates buffer                      (only used for initial sync)
        self.initial_sync_successful: bool = False # flag to indicate if inital sync was successful (only used for initial sync)
        # debug stats
        self.total_snapshots: int = 0
        self.total_bids: int = 0
        self.total_asks: int = 0
    
    def start(self):
        """
        Starts order book recording.
        """
        logger.info(f'starting order book recording for binance:{self.symbol}')
        url = BINANCE_WEBSOCKET_URL.format(self.symbol.lower())
        logger.info(f'connecting to {url}')
        self.ws = websocket.WebSocketApp(url,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=self._on_open)
        self.ws.run_forever()
    
    def stop(self):
        """
        Gracefully stops order book recording.
        """
        logger.info(f'stopping order book recording for binance:{self.symbol}')
        # close websocket connection
        self.ws.close()
        # log stats
        logger.debug('')
        logger.debug(f'total book snapshots:           {self.total_snapshots}')
        logger.debug(f'total bids in all snapshots:    {self.total_bids}')
        logger.debug(f'total asks in all snapshots:    {self.total_asks}')
        logger.debug(f'average bids+asks per snapshot: {(self.total_bids + self.total_asks) / (self.total_snapshots or 1):.1f}')

    def _on_message(self, ws, message):
        """
        WebSocket on message handler.
        """
        # deresialize order book update
        update = json.loads(message)
        # record update
        self._append_update(update)
        # try to sync the order book until successful
        if not self.initial_sync_successful:
            self.initial_sync_successful = self._sync_book(update)
            if self.initial_sync_successful:
                self.initial_book_snapshot = self.current_book # remember initial state (will be saved later)
                self.book_updates = [] # clear updates
            return
        # update the book state
        self._update_book(update)
        # # handle snapshots
        # if self._should_take_snapshot():
        #     self._take_snapshot()
        # # save snapshots to storage
        # if self._should_save_snapshots():
        #     self._save_snapshots()
    
    def _append_update(self, update: dict):
        self.book_updates.append({
            'timestamp': update['E'],
            'local_timestamp': datetime.now(tz=timezone.utc),
            'bids': update['b'],
            'asks': update['a'],
            'first_update_id': update['U'],
            'last_update_id': update['u'],
        })

    def _on_error(self, ws, error):
        """
        WebSocket on error handler.
        """
        logger.error(f"websocket error: {error}")

    def _on_close(self, ws, x, y):
        """
        WebSocket on close handler.
        """
        logger.info("websocket closed")

    def _on_open(self, ws):
        """
        WebSocket on open handler.
        """
        logger.info("websocket connected")
    
    def _sync_book(self) -> bool:
        """
        Retrieves the initial order book snapshot, finds the first valid update
        and applies it along with the subsequent buffered ones.\n
        Once this operation returns successfully, the local book is synced and
        new updates can be applied.
        """
        logger.info(f'trying to sync order book...')
        if self.initial_book_snapshot is None:
            logger.info(f'fetching order book snapshot from the REST API')
            self.initial_book_snapshot = get_order_book_snapshot(self.symbol, limit=1000)
            if self.initial_book_snapshot is None:
                logger.warning(f'failed to sync: unable to get initial snapshot from the REST API')
                return False
            logger.info(f'snapshot fetched (last update id: {self.initial_book_snapshot['lastUpdateId']})')
        last_update_id = self.initial_book_snapshot['lastUpdateId']
        valid_updates = [x for x in self.book_updates if x['u'] > last_update_id]
        if len(valid_updates) == 0:
            logger.warning(f'failed to sync: all buffered deltas are older than the snapshot')
            return False
        found_first_event_to_process = False
        for update in valid_updates:
            if update['U'] <= last_update_id + 1 and update['u'] >= last_update_id + 1:
                # we found the first delta to process, so we can init the book
                # with the init snapshot and apply the current delta
                found_first_event_to_process = True
                self._set_initial_state(initial_snapshot=self.initial_book_snapshot)
                self._update_book(update)
            else:
                # if we found the first event to process, we go on and apply 
                # all subsequent buffered deltas
                if found_first_event_to_process:
                    self._update_book(update)
        if found_first_event_to_process:
            logger.info(f'successfully synced order book')
        # if we found the first events to process, we'll also have updated our
        # local book (as you can see in the above for loop), so we're in sync
        return found_first_event_to_process

    def _set_initial_state(self, initial_snapshot: dict):
        """
        Sets the initial state of the order book.
        'initial_state' is shaped as https://api.binance.com/api/v3/depth response.
        """
        self.current_book = {
            't': 0,
            'bids': [{'p': float(bid[0]), 'q': float(bid[1])} for bid in initial_snapshot['bids']],
            'asks': [{'p': float(ask[0]), 'q': float(ask[1])} for ask in initial_snapshot['asks']],
        }
        logger.info(f'initialized local order book with snapshot.last_update_id = {initial_snapshot['lastUpdateId']}')

    def _update_book(self, update: dict):
        """
        Updates the order book with the given delta.
        This method should only be called once the initial state has been set.
        'delta' is shaped as wss://stream.binance.com:9443/ws/{symbol}@depth messages.
        """
        if self.current_book is None:
            raise Exception('cannot process order book updates if current state is None.')
        if not self._is_consistent(update):
            raise Exception('failed to update order book: received inconsistent delta')
        bids = update['b']
        asks = update['a']
        # clone current state
        next_book: dict = copy.deepcopy(self.current_book)
        # update time
        next_book['t'] = update['E']
        # update bids
        for bid in bids:
            price = float(bid[0])
            quantity = float(bid[1])
            # start by removing the price level
            next_book['bids'] = [bid for bid in next_book['bids'] if price not in bid]
            # then, if quantity is not 0, set the new one
            if quantity != 0:
                next_book['bids'].append({'p': price, 'q': quantity})
        # update asks
        for ask in asks:
            price = float(ask[0])
            quantity = float(ask[1])
            # start by removing the price level
            next_book['asks'] = [ask for ask in next_book['asks'] if price not in ask]
            # then, if quantity is not 0, set the new one
            if quantity != 0:
                next_book['asks'].append({'p': price, 'q': quantity})
        # update state
        self.previous_book = self.current_book
        self.current_book = next_book
        # logs
        # logger.debug(f'updated local book to t = {delta['E']} with {delta['u'] - delta['U']} new events from {delta['U']} to {delta['u']} ({len(bids)} new bids, {len(asks)} new asks)')
        logger.debug(f'updated local book to update id {update['u']} ({len(bids) + len(asks)} deltas)')
    
    def _aggregate_book(self, book: dict) -> pd.DataFrame:
        """
        Returns the order book snapshot aggregated by price levels.\n
        Aggregation in based on self.price_resolution.
        """
        # determine min and max price of aggregated book
        best_bid = book['bids'][0]['p']
        best_ask = book['asks'][0]['p']
        mid_price = best_bid + best_ask / 2
        trunc_mid_price = mid_price // self.price_resolution * self.price_resolution
        min_price = trunc_mid_price - self.price_resolution * self.levels_per_side
        max_price = trunc_mid_price + self.price_resolution * self.levels_per_side
        # get all ticks in the book
        levels = book['bids'] + book['asks']
        # discard all ticks outside of the min/max price range
        levels = [x for x in levels if x >= min_price and x < max_price]
        # group ticks by price range, summing quantities
        df = pd.DataFrame(levels)
        df['price_range'] = (df['p'] // self.price_resolution) * self.price_resolution
        df = df.groupby('price_range').agg({'q': 'sum'}).reset_index()
        df = df.rename(columns={'price_range': 'p'})
        df = df.sort_values(by='p')
        # fill price gaps setting 0 as quantity
        full_price_range = pd.DataFrame({ 'p': range(min_price, max_price, self.price_resolution) })
        df = pd.merge(full_price_range, df, on='p', how='left').fillna(0)
        return df
    
    def _build_snapshot(self, book: dict):
        aggregated_book_df = self._aggregate_book(book)
        ############
        # order book by range or by tick???????
        ############
        return {
            'timestamp': book['t'],
            **{f'bid_{i}': 0 for i in range(self.levels_per_side)},
            **{f'ask_{i}': 0 for i in range(self.levels_per_side)}
        }
    
    def _is_consistent(self, delta: dict):
        """
        Returns wether the current delta is consistent with the last received.
        """
        is_consistent = True
        if self.last_book_update is not None:
            first_update_id = delta['U']
            previous_delta_last_update_id = self.last_book_update['u']
            if first_update_id != previous_delta_last_update_id + 1:
                logger.info(f'warning: current delta has a diff of {first_update_id - previous_delta_last_update_id + 1} updates from the last ({first_update_id} vs {previous_delta_last_update_id})')
                is_consistent = False
        self.last_book_update = delta
        return is_consistent

    def _should_save_snapshots(self):
        return len(self.book_snapshots) == self.storage_batch_size

    def _save_snapshots(self):
        snapshots = self.book_snapshots
        # save current batch
        self.storage_provider.save_updates(snapshots)
        # update stats
        self.total_snapshots += len(snapshots)
        self.total_bids += sum([len(x['bids']) for x in snapshots])
        self.total_asks += sum([len(x['asks']) for x in snapshots])
        # clear memory
        self.book_snapshots = []

    def _should_take_snapshot(self):
        if self.previous_book is None:
            return False
        previous_timestamp = self.previous_book['t']
        current_timestamp = self.current_book['t']
        if previous_timestamp == 0 or current_timestamp == 0:
            return False
        ms_per_step = self.time_resolution_in_seconds * 1000
        aligned_ts = current_timestamp // ms_per_step * ms_per_step
        return current_timestamp >= aligned_ts and previous_timestamp < aligned_ts

    def _take_snapshot(self):
        # aggregate book before appending
        aggregated_book = self._build_snapshot(self.current_book)
        # append to list
        self.book_snapshots.append(aggregated_book)
    
    def _raise_if_invalid_params(self,
                                 symbol: str,
                                 price_resolution: float,
                                 time_resolution_in_seconds: int,
                                 levels_per_side: int,
                                 storage_provider: StorageProvider | None,
                                 storage_batch_size: int):
        if price_resolution <= 0:
            raise Exception(f'invalid parameter \'price_resolution\': must be > 0.')
        if levels_per_side <= 0:
            raise Exception(f'invalid parameter \'max_depth\': must be > 0.')
        if time_resolution_in_seconds < 1:
            raise Exception(f'invalid parameter \'time_resolution_in_seconds\': must be > 1.')
        max_levels_per_side = 1000
        if levels_per_side > max_levels_per_side:
            raise Exception(f'invalid parameter \'levels_per_side\': must be < {max_levels_per_side}')
        if not isinstance(storage_provider, StorageProvider):
            raise Exception(f'invalid parameter \'storage_provider\': must implement the StorageProvider interface.')
        if storage_batch_size <= 0:
            raise Exception(f'invalid parameter \'storage_batch_size\': must be > 0.')
        if storage_batch_size > 1_000_000:
            raise Exception(f'invalid parameter \'storage_batch_size\': must be <= 1\'000\'000.')