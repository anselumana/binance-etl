import sys
import signal
from orderbook_recorder import OrderBookRecorder
from utils import log

recorder = OrderBookRecorder('BTCUSDT')

def handle_shutdown_signal(signal_number, frame):
    log(f"intercepted signal {signal_number}, cleaning up before shutdown...")
    recorder.stop()
    sys.exit(0)  # Exit gracefully

# Register signal handlers
signal.signal(signal.SIGINT, handle_shutdown_signal)   # Handle Ctrl+C
signal.signal(signal.SIGTERM, handle_shutdown_signal)  # Handle kill/termination

if __name__ == '__main__':
    recorder.start()
