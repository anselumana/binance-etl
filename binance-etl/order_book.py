import copy
from utils import log



class OrderBook:
    def __init__(self):
        self.history = []
        self.last_delta = None
        self.first_update_processed = False

    def init(self, initial_state: dict):
        """
        Sets the initial state for the order book.
        The 'initial_state' is shaped as https://api.binance.com/api/v3/depth response.
        """
        self.history.append({
            't': 0,
            'bids': [{'p': bid[0], 'q': bid[1]} for bid in initial_state['bids']],
            'asks': [{'p': ask[0], 'q': ask[1]} for ask in initial_state['asks']],
        })
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
        state = self.history[-1]
        next_state: dict = copy.deepcopy(state)
        # update time
        next_state['t'] = delta['E']
        # if this is the first update, remove initial snapshot
        # (since it doesn't have time, since the REST api snapshot doesn't have a timestamp)
        if not self.first_update_processed:
            self.history = []
            self.first_update_processed = True
        # update bids
        for bid in bids:
            price = bid[0]
            quantity = bid[1]
            # start by removing the price level
            next_state['bids'] = [bid for bid in next_state['bids'] if price not in bid]
            # then, if quantity is not 0, set the new one
            if quantity != 0:
                next_state['bids'].append({'p': price, 'q': quantity})
        # update asks
        for ask in asks:
            price = ask[0]
            quantity = ask[1]
            # start by removing the price level
            next_state['asks'] = [ask for ask in next_state['asks'] if price not in ask]
            # then, if quantity is not 0, set the new one
            if quantity != 0:
                next_state['asks'].append({'p': price, 'q': quantity})
        # sort bids and asks
        next_state['bids'] = sorted(next_state['bids'], key=lambda x: x['p'], reverse=True)
        next_state['asks'] = sorted(next_state['asks'], key=lambda x: x['p'])
        # save new state
        self.history.append(next_state)
        # logs
        log(f'updated local book to t = {delta['E']} with {delta['u'] - delta['U']} new events from {delta['U']} to {delta['u']} ({len(bids)} new bids, {len(asks)} new asks)')
    
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
        pass