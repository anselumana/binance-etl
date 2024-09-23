import json
import time
from typing import Any
import pandas as pd
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
from binance_etl.library.model import ETLBase
from binance_etl.library.storage import StorageProvider
from binance_etl.library.logger import get_logger
from binance_etl.library.utils import logger_name_with_symbol


class SpotTradesETL(ETLBase):
    """
    ETL to record spot market trades.
    """
    def __init__(self,
                 symbol: str,
                 storage: StorageProvider):
        # set params
        self.symbol = symbol
        self.storage = storage
        # logger
        self.logger = get_logger(logger_name_with_symbol(__name__, self.symbol))
        # binance websocket client
        self.binance_ws_client = SpotWebsocketStreamClient(on_message=self._process_message)
        # state variables
        self.local_timestamp = 0 # arrival timestamp of websocket messages in ms
        # debug stats
        self.total_trades: int = 0
    
    def start(self):
        """
        Starts trades recording.
        """
        self.logger.info(f'starting trades ETL for binance:{self.symbol} spot pair')
        # connect to trades stream
        self.binance_ws_client.trade(symbol=self.symbol)
    
    def stop(self):
        """
        Gracefully stops trades recording.
        """
        self.logger.info(f'stopping trades ETL for binance:{self.symbol} spot pair')
        # close websocket connection
        self.binance_ws_client.stop()
        self._log_debug_stats()

    def _process_message(self, _: Any, message: str):
        """
        Trades handler.
        """
        # update message arrival timestamp
        self.local_timestamp = int(time.time() * 1_000)
        # deresialize trade
        trade = self._deserialize_trade_message(message)
        if trade is None:
            return
        self.logger.debug(f'processing trade {trade['id']}')
        # save trades to storage
        self._save_trade(trade)
        # update debug stats
        self._update_debug_stats(trade)
    
    def _deserialize_trade_message(self, message: str) -> dict:
        """
        Deserializes trade message and maps it to our model.\n
        https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#trade-streams
        """
        res = None
        try:
            trade = json.loads(message)
            if trade['e'] == 'trade':
                res = {
                    'timestamp': trade['E'], # ms
                    'local_timestamp': self.local_timestamp,
                    'id': trade['t'],
                    'price': trade['p'],
                    'quantity': trade['q'],
                    'side': 'sell' if trade['m'] else 'buy', # who is the liquidity aggressor (taker)
                }
        except Exception as ex:
            self.logger.warning(f'Unable to deserialize trade message: {ex}\nMessage body:\n{message}')
        return res

    def _save_trade(self, trade: dict):
        df = pd.DataFrame({ key: [value] for key, value in trade.items() })
        self.storage.add_trades(df)
    
    def _update_debug_stats(self, trade: dict):
        self.total_trades += 1

    def _log_debug_stats(self):
        self.logger.debug('')
        self.logger.debug(f'total trades: {self.total_trades}')