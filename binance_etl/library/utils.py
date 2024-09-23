import os
import json


def load_config() -> dict:
    path = os.getenv('CONFIG_PATH', 'config.json')
    with open(path) as config:
        return json.loads(config.read())

def logger_name_with_symbol(name: str, symbol: str):
    return f'{name} [{symbol}]'