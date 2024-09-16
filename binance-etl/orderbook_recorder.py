import json
import websocket
from utils import log
from order_book import OrderBook
from utils import get_order_book_snapshot


class OrderBookRecorder():
    def __init__(self, symbol: str):
        self.rest_url = 'https://api.binance.com/api/v3/depth'
        self.ws_url = f'wss://stream.binance.com:9443/ws/{symbol.lower()}@depth'
        self.symbol = symbol
        self.book = OrderBook()
        self.is_sync = False
        self.initial_snapshot = None
        self.deltas = []
    
    def start(self):
        log(f'starting order book recording for binance:{self.symbol}')
        log(f'connecting to {self.ws_url}')
        ws = websocket.WebSocketApp(self.ws_url,
                                    on_message=self._on_message,
                                    on_error=self._on_error,
                                    on_close=self._on_close,
                                    on_open=self._on_open)
        ws.run_forever()
    
    def _sync_book(self, delta: dict):
        log(f'trying to sync order book...')
        self.deltas.append(delta)
        if self.initial_snapshot is None:
            log(f'fetching order book snapshot from the REST API')
            self.initial_snapshot = get_order_book_snapshot(self.rest_url, self.symbol, limit=1000)
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


    def _on_message(self, ws, message):
        delta = json.loads(message)
        if not self.is_sync:
            self.is_sync = self._sync_book(delta)
            return
        self.book.update(delta)

    def _on_error(self, ws, error):
        log(f"websocket error: {error}")

    def _on_close(self, ws, x, y):
        log("websocket closed")
        log(f'total book snapshots:           {len(self.book.history)}')
        log(f'total bids in all snapshots:    {sum([len(x['bids']) for x in self.book.history])}')
        log(f'total asks in all snapshots:    {sum([len(x['asks']) for x in self.book.history])}')
        log(f'average bids+asks per snapshot: {sum([len(x['bids']) + len(x['asks']) for x in self.book.history]) / (len(self.book.history) or 1):.2f}')

    def _on_open(self, ws):
        log("websocket connected")