import time
from typing import Any
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
from binance_etl.library.storage import StorageProvider
from binance_etl.library.logger import get_logger
from binance_etl.library.utils import get_logger_name


class BinanceETL:
    """
    Base implementation for Binance trading events ETL.\n
    Implement core logic in subclasses.
    """
    def __init__(self,
                 market: str,
                 symbol: str,
                 event_type: str,
                 storage: StorageProvider):
        self.market = market
        self.symbol = symbol
        self.event_type = event_type
        self.storage = storage
        # logger
        self.logger = get_logger(get_logger_name(__name__, market, symbol, event_type))
        # binance websocket client
        self.binance_ws_client = SpotWebsocketStreamClient(on_message=self._process_message)
        # state variables
        self.local_timestamp = 0 # arrival timestamp of websocket messages in ms
        # debug stats
        self.total_messages: int = 0

    def start(self):
        """
        Starts ETL job\n
        Implement in subclasses.
        """
        pass

    def stop(self):
        """
        Stops ETL job.
        """
        # close websocket connection
        self.binance_ws_client.stop()
        self._log_debug_stats()
    
    def _process_message(self, _: Any, message: str):
        """
        Websocket message handler.
        """
        # update message arrival timestamp
        self.local_timestamp = int(time.time() * 1_000)
        # deresialize message
        entity = self._deserialize_message(message)
        if entity is None:
            return
        # handle the message
        self._handle_message(entity)
        # update debug stats
        self._update_debug_stats(entity)
    
    def _handle_message(self, entity: dict):
        """
        To implement in subclasses.\n
        Core logic of the ETL.
        """
        raise NotImplementedError()
    
    def _deserialize_message(self, message: str) -> dict:
        """
        To implement in subclasses.\n
        Deserializes the message from the websocket.
        """
        raise NotImplementedError()
    
    def _update_debug_stats(self, entity: dict):
        self.total_messages += 1
        
    def _log_debug_stats(self):
        self.logger.debug('')
        self.logger.debug(f'total messages processed: {self.total_messages}')