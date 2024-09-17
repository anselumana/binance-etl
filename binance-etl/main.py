import os
import sys
import signal
from order_book_manager import OrderBookManager
from utils import log
from dotenv import load_dotenv


load_dotenv()

manager = OrderBookManager('BTCUSDT')

def handle_shutdown_signal(signal_number, frame):
    log(f"intercepted signal {signal_number}, cleaning up before shutdown...")
    manager.stop()
    sys.exit(0)  # Exit gracefully

# Register signal handlers
signal.signal(signal.SIGINT, handle_shutdown_signal)   # Handle Ctrl+C
signal.signal(signal.SIGTERM, handle_shutdown_signal)  # Handle kill/termination

if __name__ == '__main__':
    manager.start()
