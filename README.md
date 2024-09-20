<div align="center">

# Binance ETL

![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Python](https://img.shields.io/badge/python-%3E=3.12-blue)


**E**xtract, **T**ransform and **L**oad **real-time trading events** from Binance exchange.

[Getting started](#getting-started) •
[Data](#data) •
[Export](#export)

</div>

## Getting started

Install dependencies.

```sh
pip install -r requirements.txt
```

Configure the script by creating a `.env` file (see `.env.template` as reference).

```python
# example configuration

# binance symbol to track
SYMBOL='solusdt'
# batch size for each save
STORAGE_BATCH_SIZE=100
# csv storage directory
CSV_STORAGE_DIRECTORY='./output/'
```

Start the ETL:

```sh
python3 ./binance_etl/main.py
```

## Data

We support the collection and storage of the following events.

| Event type | Spot | Futures |
|-|-|-|
| [Order book updates](#order-book-updates) | ✅ | ❌ |
| [Trades](#trades) | ❌ | ❌ |

### Order book updates
Order books updates are fetched in real-time from **binance websockets** and are as frequent as the underlying [data source][binance-spot-depth-updates].
<br>

We store book updates in the following format:
<br>

| Column | Description |
|-|-|
| `timestamp` | Exchange timestamp in milliseconds since epoch |
| `local_timestamp` | Message arrival timestamp in milliseconds since epoch |
| `side` | Side of the order: `bid` or `ask` |
| `price` | Price of the order |
| `quantity` | Quantity of the order |
| `is_snapshot` | `true` if the update is part of the initial snapshot, else `false` |

<br>

Check [binance developer docs][binance-developer-docs-how-to-manage-local-book] to understand how the sync is managed.

### Trades
⌛ *Coming soon...*

## Export
We support the following export destinations:
| Export type | Supported |
|-|-|
| CSV files | ✅ |
| Google BigQuery | ❌ |


[binance-developer-docs-how-to-manage-local-book]: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
[binance-spot-depth-updates]: https://binance-docs.github.io/apidocs/spot/en/#diff-depth-stream
