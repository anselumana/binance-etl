import sys
import signal
from dotenv import load_dotenv
load_dotenv()
from factory import get_incremental_book_etl
from logger import get_logger


logger = get_logger(__name__)

etl = get_incremental_book_etl()

def handle_shutdown_signal(signal_number, frame):
    logger.info(f"intercepted signal {signal_number}, cleaning up before shutdown...")
    etl.stop()
    logger.info('exiting binance-etl')
    sys.exit(0)  # Exit gracefully

# Register signal handlers
signal.signal(signal.SIGINT, handle_shutdown_signal)   # Handle Ctrl+C
signal.signal(signal.SIGTERM, handle_shutdown_signal)  # Handle kill/termination

if __name__ == '__main__':
    logger.info('starting binance-etl')
    etl.start()
