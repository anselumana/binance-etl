import websocket
from binance_etl.library.consts import BINANCE_WEBSOCKET_URL
from binance_etl.library.logger import get_logger


logger = get_logger(__name__)

class BinanceWebSocket():
    def __init__(self, on_message, on_error = None, on_open = None, on_close = None):
        self.on_message_handler = on_message
        self.on_error_handler = on_error
        self.on_open_handler = on_open
        self.on_close_handler = on_close
        self.ws: websocket.WebSocketApp = None

    def connect_to_depth_stream(self, symbol: str):
        url = BINANCE_WEBSOCKET_URL.format(symbol.lower())
        logger.info(f'connecting to {url}')
        self.ws = websocket.WebSocketApp(url,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=self._on_open)
        self.ws.run_forever()
    
    def close_connection(self):
        self.ws.close()
    
    def _on_message(self, ws, message):
        """
        WebSocket on message handler.
        """
        self.on_message_handler(message)

    def _on_error(self, ws, error):
        """
        WebSocket on error handler.
        """
        logger.error(f"websocket error: {error}")
        if self.on_error_handler is not None:
            self.on_error_handler()

    def _on_open(self, ws):
        """
        WebSocket on open handler.
        """
        logger.info("websocket connected")
        if self.on_open_handler is not None:
            self.on_open_handler()

    def _on_close(self, ws, x, y):
        """
        WebSocket on close handler.
        """
        logger.info("websocket closed")
        if self.on_close_handler is not None:
            self.on_close_handler()