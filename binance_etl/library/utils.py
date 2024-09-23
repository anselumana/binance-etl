import json


def flatten(list_of_lists: list):
    return sum(list_of_lists, [])

def is_none_or_empty(s: str) -> bool:
    return s is None or s.strip() == ''

def load_config(path: str = None) -> dict:
    with open(path or './config.json') as config:
        return json.loads(config.read())

def logger_name_with_symbol(name: str, symbol: str):
    return f'{name} [{symbol}]'