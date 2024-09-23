import signal
from typing import List
from dotenv import load_dotenv
load_dotenv()
from binance_etl.etls.base import BinanceETL
from binance_etl.library.logger import get_logger
from binance_etl.library.factory import get_etls


logger = get_logger(__name__)

def register_signal_handlers(etls: List[BinanceETL]):
    signal.signal(signal.SIGINT, lambda signal_number, frame: handle_shutdown_signal(signal_number, frame, etls))   # Handle Ctrl+C
    signal.signal(signal.SIGTERM, lambda signal_number, frame: handle_shutdown_signal(signal_number, frame, etls))  # Handle kill/termination

def handle_shutdown_signal(signal_number, frame, etls: List[BinanceETL]):
    logger.info(f"intercepted signal {signal_number}, cleaning up before shutdown...")
    # stop ETLs
    for etl in etls:
        etl.stop()
    logger.info('exiting binance-etl')
    # sys.exit(0)  # Exit gracefully


if __name__ == '__main__':
    logger.info('starting binance-etl')
    # get ETLs
    etls = get_etls()
    # register signal handlers
    register_signal_handlers(etls)
    # start ETLs
    for etl in etls:
        etl.start()
