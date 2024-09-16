from orderbook_recorder import OrderBookRecorder

if __name__ == '__main__':
    recorder = OrderBookRecorder('BTCUSDT')
    recorder.start()
