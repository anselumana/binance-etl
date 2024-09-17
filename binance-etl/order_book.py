import copy
import pandas as pd
from utils import log, flatten



class OrderBook:
    def __init__(self, price_range_for_aggregation=100):
        self.price_range_for_aggregation = price_range_for_aggregation
        self.history = []
        self.last_state = {}
        self.last_delta = None
        self.first_update_processed = False

    def init(self, initial_state: dict):
        """
        Sets the initial state for the order book.
        The 'initial_state' is shaped as https://api.binance.com/api/v3/depth response.
        """
        self.last_state = {
            't': 0,
            'bids': [{'p': float(bid[0]), 'q': float(bid[1])} for bid in initial_state['bids']],
            'asks': [{'p': float(ask[0]), 'q': float(ask[1])} for ask in initial_state['asks']],
        }
        log(f'initialized local order book with snapshot.last_update_id = {initial_state['lastUpdateId']}')

    def update(self, delta: dict):
        """
        Updates the order book with the given deltas.
        The 'delta' is shaped as wss://stream.binance.com:9443/ws/{symbol}@depth messages.
        """
        if not self._is_consistent(delta):
            raise Exception(f'failed to update order book: received inconsistent delta')
        bids = delta['b']
        asks = delta['a']
        # get and clone last state
        next_state: dict = copy.deepcopy(self.last_state)
        # update time
        next_state['t'] = delta['E']
        # if this is the first update, remove initial snapshot
        # (since it doesn't have time, since the REST api snapshot doesn't have a timestamp)
        if not self.first_update_processed:
            self.history = []
            self.first_update_processed = True
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
        self.last_state = next_state
        # aggregate current book
        aggregated_book = self._aggregate_book(next_state)
        # append current book to history
        self.history.append(aggregated_book)
        # logs
        log(f'updated local book to t = {delta['E']} with {delta['u'] - delta['U']} new events from {delta['U']} to {delta['u']} ({len(bids)} new bids, {len(asks)} new asks)')
    
    def _aggregate_book(self, book: dict):
        aggregated = copy.deepcopy(book)
        aggregated['bids'] = self._aggregate_book_side(book['bids'], type='bids')
        aggregated['asks'] = self._aggregate_book_side(book['asks'], type='asks')
        return aggregated

    def _aggregate_book_side(self, levels: list, type: str):
        # build df
        df = pd.DataFrame(levels)
        # create culumn with truncated prices based on price range
        rng = self.price_range_for_aggregation
        df['price_range'] = (df['p'] // rng) * rng
        # group by price range
        aggregated = df.groupby('price_range').agg({'q': 'sum'}).reset_index()
        # rename back to 'p'
        aggregated.rename(columns={'price_range': 'p'}, inplace=True)
        aggregated = aggregated.sort_values(by='p', ascending=type=='asks')
        return aggregated.to_dict(orient='records')

    
    def _is_consistent(self, delta: dict):
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
        log(f'saving order book...')
        import os
        import pandas as pd
        from datetime import datetime, timezone
        # build dataframes
        bids = pd.DataFrame({
            'time': flatten([[datetime.fromtimestamp(x['t'] // 1000, tz=timezone.utc) for y in x['bids']] for x in self.history]),
            'price': flatten([[y['p'] for y in x['bids']] for x in self.history]),
            'quantity': flatten([[y['q'] for y in x['bids']] for x in self.history]),
        })
        asks = pd.DataFrame({
            'time': flatten([[datetime.fromtimestamp(x['t'] // 1000, tz=timezone.utc) for y in x['asks']] for x in self.history]),
            'price': flatten([[y['p'] for y in x['asks']] for x in self.history]),
            'quantity': flatten([[y['q'] for y in x['asks']] for x in self.history]),
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