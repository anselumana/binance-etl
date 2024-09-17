import copy
import json
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
                 max_depth = 50,
                 time_resolution_in_seconds: int = 1,
                 storage_provider: StorageProvider = None,
                 storage_batch_size: int = 100):
        # params validation
        self._raise_if_invalid_params(
            symbol,
            price_resolution,
            max_depth,
            time_resolution_in_seconds,
            storage_provider,
            storage_batch_size)
        # set params
        self.symbol = symbol
        self.price_resolution = price_resolution
        self.max_depth = max_depth
        self.time_resolution_in_seconds = time_resolution_in_seconds
        self.storage_provider = storage_provider
        self.storage_batch_size = storage_batch_size
        # web socket client
        self.ws: websocket.WebSocketApp = None
        # current state of the order book at full resolution
        self.current_book: dict = None
        # local order book history
        self.book_history: list = []
        # last received delta (used to check that the subsequent one is correct)
        self.last_delta: dict = None
        # (only used for initial sync) initial snapshot fetched from the REST API
        self.initial_book_snapshot: dict = None
        # (only used for initial sync) buffered deltas
        self.deltas_buffer: list = []
        # (only used for initial sync) flag to indicate if inital sync was successful
        self.initial_sync_successful: bool = False
        # stats
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
        
    def get_order_book_history(self) -> list:
        """
        Returns the local order book history.
        """
        return self.book_history

    def _on_message(self, ws, message):
        """
        WebSocket on message handler.
        """
        delta = json.loads(message)
        if not self.initial_sync_successful:
            self.initial_sync_successful = self._sync_book(delta)
            return
        self._process_update(delta)
        self._save_if_needed()

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
    
    def _sync_book(self, delta: dict) -> bool:
        """
        Retrieves the initial order book snapshot, finds the first valid delta
        and applies it along with the subsequent buffered ones.\n
        Once this operation returns successfully, the local book is synced and
        new updates can be applied.
        """
        logger.info(f'trying to sync order book...')
        self.deltas_buffer.append(delta)
        if self.initial_book_snapshot is None:
            logger.info(f'fetching order book snapshot from the REST API')
            self.initial_book_snapshot = get_order_book_snapshot(self.symbol, limit=1000)
            if self.initial_book_snapshot is None:
                logger.warning(f'failed to sync: unable to get initial snapshot from the REST API')
                return False
            logger.info(f'snapshot fetched (last update id: {self.initial_book_snapshot['lastUpdateId']})')
        last_update_id = self.initial_book_snapshot['lastUpdateId']
        valid_deltas = [x for x in self.deltas_buffer if x['u'] > last_update_id]
        if len(valid_deltas) == 0:
            logger.warning(f'failed to sync: all buffered deltas are older than the snapshot')
            return False
        found_first_event_to_process = False
        for d in valid_deltas:
            if d['U'] <= last_update_id + 1 and d['u'] >= last_update_id + 1:
                # we found the first delta to process, so we can init the book
                # with the init snapshot and apply the current delta
                found_first_event_to_process = True
                self._set_initial_state(initial_snapshot=self.initial_book_snapshot)
                self._process_update(d)
            else:
                # if we found the first event to process, we go on and apply 
                # all subsequent buffered deltas
                if found_first_event_to_process:
                    self._process_update(d)
        # clear memory if we've synced successfully
        if found_first_event_to_process:
            self.deltas_buffer = []
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

    def _process_update(self, delta: dict):
        """
        Updates the order book with the given delta.
        This method should only be called once the initial state has been set.
        'delta' is shaped as wss://stream.binance.com:9443/ws/{symbol}@depth messages.
        """
        if self.current_book is None:
            raise Exception('cannot process order book updates if current state is None.')
        if not self._is_consistent(delta):
            raise Exception('failed to update order book: received inconsistent delta')
        bids = delta['b']
        asks = delta['a']
        # get and clone last state
        next_book: dict = copy.deepcopy(self.current_book)
        # update time
        next_book['t'] = delta['E']
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
        # save new state
        self.current_book = next_book
        # aggregate current book
        aggregated_book = self._aggregate_book(next_book)
        # append current book to history
        self.book_history.append(aggregated_book)
        # logs
        logger.debug(f'updated local book to t = {delta['E']} with {delta['u'] - delta['U']} new events from {delta['U']} to {delta['u']} ({len(bids)} new bids, {len(asks)} new asks)')
    
    def _aggregate_book(self, book: dict):
        """
        Returns the order book snapshot aggregated by price levels.\n
        Aggregation in based on self.price_range_for_aggregation.
        """
        aggregated = copy.deepcopy(book)
        aggregated['bids'] = self._aggregate_page(book['bids'], is_bids=True)
        aggregated['asks'] = self._aggregate_page(book['asks'], is_bids=False)
        return aggregated

    def _aggregate_page(self, levels: list, is_bids: bool):
        """
        Aggregates a book page (bids or asks).
        """
        best_of_book_price = levels[0]['p']
        max_abs_price = self.price_resolution * self.max_depth
        cutoff = best_of_book_price - max_abs_price if is_bids else best_of_book_price + max_abs_price
        levels = [x for x in levels if x['p'] > cutoff] if is_bids else [x for x in levels if x['p'] < cutoff]
        # build dataframe
        df = pd.DataFrame(levels)
        # create culumn with truncated prices based on price range
        df['price_range'] = (df['p'] // self.price_resolution) * self.price_resolution
        # group by price range
        aggregated = df.groupby('price_range').agg({'q': 'sum'}).reset_index()
        # rename back to 'p'
        aggregated.rename(columns={'price_range': 'p'}, inplace=True)
        # sort asc/desc based on asks/bids
        aggregated = aggregated.sort_values(by='p', ascending=not is_bids)
        # return back to dict
        return aggregated.to_dict(orient='records')

    
    def _is_consistent(self, delta: dict):
        """
        Returns wether the current delta is consistent with the last received.
        """
        is_consistent = True
        if self.last_delta is not None:
            first_update_id = delta['U']
            previous_delta_last_update_id = self.last_delta['u']
            if first_update_id != previous_delta_last_update_id + 1:
                logger.info(f'warning: current delta has a diff of {first_update_id - previous_delta_last_update_id + 1} updates from the last ({first_update_id} vs {previous_delta_last_update_id})')
                is_consistent = False
        self.last_delta = delta
        return is_consistent

    def _save_if_needed(self):
        history = self.get_order_book_history()
        if len(history) != self.storage_batch_size:
            return
        # save current batch
        self.storage_provider.save(history)
        # update stats
        self.total_snapshots += len(history)
        self.total_bids += sum([len(x['bids']) for x in history])
        self.total_asks += sum([len(x['asks']) for x in history])
        # clear memory
        self.book_history = []
    
    def _raise_if_invalid_params(self,
                                 symbol: str,
                                 price_resolution: float,
                                 max_depth: int,
                                 time_resolution_in_seconds: int,
                                 storage_provider: StorageProvider | None,
                                 storage_batch_size: int):
        if price_resolution <= 0:
            raise Exception(f'invalid parameter \'price_resolution\': must be > 0.')
        if max_depth <= 0:
            raise Exception(f'invalid parameter \'max_depth\': must be > 0.')
        max_max_depth = 1000
        if max_depth > max_max_depth:
            raise Exception(f'invalid parameter \'max_depth\': must be < {max_max_depth}')
        if time_resolution_in_seconds < 1:
            raise Exception(f'invalid parameter \'time_resolution_in_seconds\': must be > 1.')
        if time_resolution_in_seconds > 1 and time_resolution_in_seconds % 60 != 0:
            raise Exception(f'invalid parameter \'time_resolution_in_seconds\': can either be 1 second or be must be a multiple of 60.')
        if not isinstance(storage_provider, StorageProvider):
            raise Exception(f'invalid parameter \'storage_provider\': must implement the StorageProvider interface.')
        if storage_batch_size <= 0:
            raise Exception(f'invalid parameter \'storage_batch_size\': must be > 0.')
        if storage_batch_size > 1_000_000:
            raise Exception(f'invalid parameter \'storage_batch_size\': must be <= 1\'000\'000.')