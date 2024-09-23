import logging
import colorlog

from binance_etl.library.utils import load_config


def get_logger(name):
    """
    Returns a logger with custom config.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    enable_console_handler = True
    enable_file_handler = False

    if not logger.hasHandlers():
        config = load_config()
        logging_config = config['logging']
        mappings = logging.getLevelNamesMapping()

        if bool(logging_config['console']['enabled']):
            # Create a colored console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(mappings[logging_config['console']['level']])
            # Create color formatter for the console with valid color names
            color_formatter = colorlog.ColoredFormatter(
                "%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt='%Y-%m-%d %H:%M:%S',
                log_colors={
                    'DEBUG': 'white',       # Light gray for debug messages
                    'INFO': 'green',        # Green for info messages
                    'WARNING': 'yellow',    # Yellow for warnings
                    'ERROR': 'red',         # Red for errors
                    'CRITICAL': 'bold_red'  # Bold red for critical errors
                }
            )
            # Add the formatters to the handlers
            console_handler.setFormatter(color_formatter)
            # Add handlers to the logger
            logger.addHandler(console_handler)

        if bool(logging_config['file']['enabled']):
            # Create a file handler (no color for files)
            file_handler = logging.FileHandler(logging_config['file']['file_path'])
            file_handler.setLevel(mappings[logging_config['file']['level']])
            # Create normal formatter for the file (no color)
            file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

    return logger
