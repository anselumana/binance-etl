import json
import pandas as pd
from binance_etl.etls.base import BinanceETL
from binance_etl.library.storage import StorageProvider


class SpotTradesETL(BinanceETL):
    """
    ETL to record spot market trades.
    """
    def __init__(self, symbol: str, storage: StorageProvider):
        super().__init__('spot', symbol, 'trade', storage)
    
    def start(self):
        """
        Starts trades recording.
        """
        self.logger.info(f'starting trades ETL')
        # connect to trades stream
        self.binance_ws_client.trade(symbol=self.symbol)
        super().start()
    
    def stop(self):
        """
        Gracefully stops trades recording.
        """
        self.logger.info(f'stopping trades ETL')
        super().stop()

    def _handle_message(self, message: str):
        """
        Trades handler.
        """
        self.logger.debug(f'processing trade {message['id']} ({message['side']} {message['quantity']} @{message['price']})')
        # save trades to storage
        self._save_trade(message)
    
    def _deserialize_message(self, message: str) -> dict:
        """
        Deserializes trade message.\n
        https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#trade-streams
        """
        res = None
        try:
            trade = json.loads(message)
            # ignore events that are not 'trade' (which is usually only the first message)
            if 'e' in trade.keys() and trade['e'] == 'trade':
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
