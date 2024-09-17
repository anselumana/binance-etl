import json
import websocket
from utils import log
from order_book import OrderBook
from utils import get_order_book_snapshot
from consts import BINANCE_WEBSOCKET_URL


class OrderBookRecorder():
    """
    Manages websocket connection and the local order book mirror.\n
    Ref: https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly.
    """
    def __init__(self, symbol: str):
        self.symbol: str = symbol
        self.ws: websocket.WebSocketApp = None
        self.book = OrderBook()
        self.is_sync: bool = False
        self.initial_snapshot: dict = None
        self.deltas: list = []
    
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
        self.book.to_csv('./output/')

    def _on_message(self, ws, message):
        """
        WebSocket on message handler.
        """
        delta = json.loads(message)
        if not self.is_sync:
            self.is_sync = self._sync_book(delta)
            return
        self.book.update(delta)

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
        log('')
        log(f'total book snapshots:           {len(self.book.history)}')
        log(f'total bids in all snapshots:    {sum([len(x['bids']) for x in self.book.history])}')
        log(f'total asks in all snapshots:    {sum([len(x['asks']) for x in self.book.history])}')
        log(f'average bids+asks per snapshot: {sum([len(x['bids']) + len(x['asks']) for x in self.book.history]) / (len(self.book.history) or 1):.2f}')

    def _on_open(self, ws):
        """
        WebSocket on open handler.
        """
        log("websocket connected")
    
    def _sync_book(self, delta: dict) -> bool:
        """
        Retrieves initial order book snapshots and syncs websocket deltas to the snapshots.\n
        Ref: https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly.
        """
        log(f'trying to sync order book...')
        self.deltas.append(delta)
        if self.initial_snapshot is None:
            log(f'fetching order book snapshot from the REST API')
            self.initial_snapshot = get_order_book_snapshot(self.symbol, limit=1000)
            if self.initial_snapshot is None:
                log(f'failed to sync')
                return False
            log(f'snapshot fetched (last update id: {self.initial_snapshot['lastUpdateId']})')
        last_update_id = self.initial_snapshot['lastUpdateId']
        valid_deltas = [x for x in self.deltas if x['u'] > last_update_id]
        if len(valid_deltas) == 0:
            log(f'failed to sync: all buffered deltas are older than the snapshot')
            return False
        found_first_event_to_process = False
        for d in valid_deltas:
            if d['U'] <= last_update_id + 1 and d['u'] >= last_update_id + 1:
                found_first_event_to_process = True
                self.book.init(initial_state=self.initial_snapshot)
                self.book.update(d)
            else:
                if found_first_event_to_process:
                    self.book.update(d)
        # clear memory if we've synced successfully
        if found_first_event_to_process:
            self.deltas = []
            log(f'successfully synced order book')
        # if we found the first events to process, we'll also have updated our
        # local book (as you can see in the above for loop), so we're in sync
        return found_first_event_to_process
