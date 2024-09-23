import os
import json


def load_config() -> dict:
    path = os.getenv('CONFIG_PATH', 'config.json')
    with open(path) as config:
        return json.loads(config.read())

def get_logger_name(module_name: str,
                    market: str = None,
                    symbol: str = None,
                    event_type: str = None):
    extra_info = [x for x in [market, symbol, event_type] if x]
    return f'{module_name} [{'.'.join(extra_info)}]'
    