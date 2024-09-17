import copy
import json
import pandas as pd
import websocket
from utils import get_order_book_snapshot, log, flatten
from consts import BINANCE_WEBSOCKET_URL


class OrderBookManager:
    """
    Manages the local order book mirror.\n
    Ref: https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly.
    """
    def __init__(self, symbol: str, price_range_for_aggregation: float = 100):
        # web socket client
        self.ws: websocket.WebSocketApp = None
        # symbol
        self.symbol = symbol
        # price range used for aggregating the book
        self.price_range_for_aggregation: float = price_range_for_aggregation
        # current state of the order book at full resolution
        self.current_state: dict = None
        # local order book history
        self.history: list = []
        # last received delta (used to check that the subsequent one is correct)
        self.last_delta: dict = None
        # (only used for initial sync) initial snapshot fetched from the REST API
        self.initial_snapshot: dict = None
        # (only used for initial sync) buffered deltas
        self.deltas_buffer: list = []
        # (only used for initial sync) flag to indicate if inital sync was successful
        self.initial_sync_successful: bool = False
    
    def start(self):
        """
        Starts order book recording.
        """
        log(f'starting order book recording for binance:{self.symbol}')
        url = BINANCE_WEBSOCKET_URL.format(self.symbol.lower())
        log(f'connecting to {url}')
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
        log(f'stopping order book recording for binance:{self.symbol}')
        self.ws.close()
        # save to csv
        self.to_csv('./output/')
        # log stats
        history = self.get_order_book_history()
        log('')
        log(f'total raw book snapshots:       {len(self.history)}')
        log(f'total book snapshots:           {len(history)}')
        log(f'total bids in all snapshots:    {sum([len(x['bids']) for x in history])}')
        log(f'total asks in all snapshots:    {sum([len(x['asks']) for x in history])}')
        log(f'average bids+asks per snapshot: {sum([len(x['bids']) + len(x['asks']) for x in history]) / (len(history) or 1):.2f}')
        
    def get_order_book_history(self) -> list:
        """
        Returns the local order book history.
        """
        return self.history

    def _on_message(self, ws, message):
        """
        WebSocket on message handler.
        """
        delta = json.loads(message)
        if not self.initial_sync_successful:
            self.initial_sync_successful = self._sync_book(delta)
            return
        self._process_update(delta)

    def _on_error(self, ws, error):
        """
        WebSocket on error handler.
        """
        log(f"websocket error: {error}")

    def _on_close(self, ws, x, y):
        """
        WebSocket on close handler.
        """
        log("websocket closed")

    def _on_open(self, ws):
        """
        WebSocket on open handler.
        """
        log("websocket connected")
    
    def _sync_book(self, delta: dict) -> bool:
        """
        Retrieves the initial order book snapshot, finds the first valid delta
        and applies it along with the subsequent buffered ones.\n
        Once this operation returns successfully, the local book is synced and
        new updates can be applied.
        """
        log(f'trying to sync order book...')
        self.deltas_buffer.append(delta)
        if self.initial_snapshot is None:
            log(f'fetching order book snapshot from the REST API')
            self.initial_snapshot = get_order_book_snapshot(self.symbol, limit=1000)
            if self.initial_snapshot is None:
                log(f'failed to sync: unable to get initial snapshot from the REST API')
                return False
            log(f'snapshot fetched (last update id: {self.initial_snapshot['lastUpdateId']})')
        last_update_id = self.initial_snapshot['lastUpdateId']
        valid_deltas = [x for x in self.deltas_buffer if x['u'] > last_update_id]
        if len(valid_deltas) == 0:
            log(f'failed to sync: all buffered deltas are older than the snapshot')
            return False
        found_first_event_to_process = False
        for d in valid_deltas:
            if d['U'] <= last_update_id + 1 and d['u'] >= last_update_id + 1:
                # we found the first delta to process, so we can init the book
                # with the init snapshot and apply the current delta
                found_first_event_to_process = True
                self._set_initial_state(initial_snapshot=self.initial_snapshot)
                self._process_update(d)
            else:
                # if we found the first event to process, we go on and apply 
                # all subsequent buffered deltas
                if found_first_event_to_process:
                    self._process_update(d)
        # clear memory if we've synced successfully
        if found_first_event_to_process:
            self.deltas_buffer = []
            log(f'successfully synced order book')
        # if we found the first events to process, we'll also have updated our
        # local book (as you can see in the above for loop), so we're in sync
        return found_first_event_to_process

    def _set_initial_state(self, initial_snapshot: dict):
        """
        Sets the initial state of the order book.
        'initial_state' is shaped as https://api.binance.com/api/v3/depth response.
        """
        self.current_state = {
            't': 0,
            'bids': [{'p': float(bid[0]), 'q': float(bid[1])} for bid in initial_snapshot['bids']],
            'asks': [{'p': float(ask[0]), 'q': float(ask[1])} for ask in initial_snapshot['asks']],
        }
        log(f'initialized local order book with snapshot.last_update_id = {initial_snapshot['lastUpdateId']}')

    def _process_update(self, delta: dict):
        """
        Updates the order book with the given delta.
        This method should only be called once the initial state has been set.
        'delta' is shaped as wss://stream.binance.com:9443/ws/{symbol}@depth messages.
        """
        if self.current_state is None:
            raise Exception('cannot process order book updates if current state is None.')
        if not self._is_consistent(delta):
            raise Exception('failed to update order book: received inconsistent delta')
        bids = delta['b']
        asks = delta['a']
        # get and clone last state
        next_state: dict = copy.deepcopy(self.current_state)
        # update time
        next_state['t'] = delta['E']
        # update bids
        for bid in bids:
            price = float(bid[0])
            quantity = float(bid[1])
            # start by removing the price level
            next_state['bids'] = [bid for bid in next_state['bids'] if price not in bid]
            # then, if quantity is not 0, set the new one
            if quantity != 0:
                next_state['bids'].append({'p': price, 'q': quantity})
        # update asks
        for ask in asks:
            price = float(ask[0])
            quantity = float(ask[1])
            # start by removing the price level
            next_state['asks'] = [ask for ask in next_state['asks'] if price not in ask]
            # then, if quantity is not 0, set the new one
            if quantity != 0:
                next_state['asks'].append({'p': price, 'q': quantity})
        # save new state
        self.current_state = next_state
        # aggregate current book
        aggregated_book = self._aggregate_book(next_state)
        # append current book to history
        self.history.append(aggregated_book)
        # logs
        log(f'updated local book to t = {delta['E']} with {delta['u'] - delta['U']} new events from {delta['U']} to {delta['u']} ({len(bids)} new bids, {len(asks)} new asks)')
    
    def _aggregate_book(self, book: dict):
        """
        Returns the order book snapshot aggregated by price levels.\n
        Aggregation in based on self.price_range_for_aggregation.
        """
        aggregated = copy.deepcopy(book)
        aggregated['bids'] = self._aggregate_book_side(book['bids'], type='bids')
        aggregated['asks'] = self._aggregate_book_side(book['asks'], type='asks')
        return aggregated

    def _aggregate_book_side(self, levels: list, type: str):
        """
        Aggregates bids/asks.
        """
        df = pd.DataFrame(levels)
        # create culumn with truncated prices based on price range
        df['price_range'] = (df['p'] // self.price_range_for_aggregation) * self.price_range_for_aggregation
        # group by price range
        aggregated = df.groupby('price_range').agg({'q': 'sum'}).reset_index()
        # rename back to 'p'
        aggregated.rename(columns={'price_range': 'p'}, inplace=True)
        # sort asc/desc based on asks/bids
        aggregated = aggregated.sort_values(by='p', ascending=type=='asks')
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
                log(f'warning: current delta has a diff of {first_update_id - previous_delta_last_update_id + 1} updates from the last ({first_update_id} vs {previous_delta_last_update_id})')
                is_consistent = False
        self.last_delta = delta
        return is_consistent

    def to_csv(self, base_path: str, clear_memory: bool=True):
        """
        Saves book history to csv.
        """
        log(f'saving order book...')
        import os
        import pandas as pd
        from datetime import datetime, timezone
        history = self.get_order_book_history()
        # build dataframes
        bids = pd.DataFrame({
            'time': flatten([[datetime.fromtimestamp(x['t'] // 1000, tz=timezone.utc) for y in x['bids']] for x in history]),
            'price': flatten([[y['p'] for y in x['bids']] for x in history]),
            'quantity': flatten([[y['q'] for y in x['bids']] for x in history]),
        })
        asks = pd.DataFrame({
            'time': flatten([[datetime.fromtimestamp(x['t'] // 1000, tz=timezone.utc) for y in x['asks']] for x in history]),
            'price': flatten([[y['p'] for y in x['asks']] for x in history]),
            'quantity': flatten([[y['q'] for y in x['asks']] for x in history]),
        })
        # create path if it doesn't exist
        if not os.path.exists(base_path):
            os.makedirs(base_path)
        bids_csv = os.path.join(base_path, 'bids.csv')
        asks_csv = os.path.join(base_path, 'asks.csv')
        # save to csv
        bids.to_csv(bids_csv, index=False)
        asks.to_csv(asks_csv, index=False)
        # log
        log(f'successfully saved book to:')
        log(f'  {bids_csv}')
        log(f'  {bids_csv}')