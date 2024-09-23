import sys
import signal
from typing import List
from dotenv import load_dotenv
load_dotenv()
from binance_etl.model import ETLBase
from binance_etl.symbol_etl import SymbolETL
from binance_etl.utils import load_config
from binance_etl.logger import get_logger
from binance_etl.factory import get_storage_provider


logger = get_logger(__name__)

def handle_shutdown_signal(signal_number, frame, etls: List[ETLBase]):
    logger.info(f"intercepted signal {signal_number}, cleaning up before shutdown...")
    # stop ETLs
    for etl in etls:
        etl.stop()
    logger.info('exiting binance-etl')
    sys.exit(0)  # Exit gracefully

def register_signal_handlers(etls: List[ETLBase]):
    signal.signal(signal.SIGINT, lambda signal_number, frame: handle_shutdown_signal(signal_number, frame, etls))   # Handle Ctrl+C
    signal.signal(signal.SIGTERM, lambda signal_number, frame: handle_shutdown_signal(signal_number, frame, etls))  # Handle kill/termination


if __name__ == '__main__':
    logger.info('starting binance-etl')
    # get config
    config = load_config()
    # create ETLs for each symbol
    etls: List[ETLBase] = []
    for symbol_info in config['symbols']:
        etl = SymbolETL(symbol_info['name'], symbol_info['markets'], symbol_info['events'])
        etls.append(etl)
    # register signal handlers
    register_signal_handlers(etls)
    # start ETLs
    for etl in etls:
        etl.start()
