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

### Install dependencies

```sh
pip install -r requirements.txt
```

### Configure
Configure the ETL by editing `config.json`.

Here you can define the **symbols** you want to track, the types of **events to record** and the desired **storage** destination.

### Start

```sh
python3 -m binance_etl.main
```

## Data

The program supports the collection and storage of the following **trading events**.

| Event type | Spot | Futures |
|-|-|-|
| [Depth updates](#depth-updates) | ✅ | ❌ |
| [Trades](#trades) | ✅ | ❌ |

If you're looking for some **ready to use datasets**, I've exported some sample data to [this kaggle dataset][kaggle-binance-trading-events-dataset], and you can check some started code in [this kaggle notebook][kaggle-binance-trading-events-notebook].

### Depth updates
Depth updates events are **order book updates** streamed from [binance websockets **depth** stream][binance-docs-websocket-depth].
<br>

We store depth updates in the following format:
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

### Trades
Trade events are **market trades** streamed from [binance websockets **trade** stream][binance-docs-websocket-trade].

We store depth updates in the following format:
<br>

| Column | Description |
|-|-|
| `timestamp` | Exchange timestamp in milliseconds since epoch |
| `local_timestamp` | Message arrival timestamp in milliseconds since epoch |
| `id` | Unique identifier of the trade |
| `side` | Can be `buy` or `sell`, and reflects who is the liquidity **taker** |
| `price` | Execution price of the trade |
| `quantity` | Quantity of the trade |

## Export
The program supports the following export destinations:
| Export type | Supported |
|-|-|
| Local CSV files | ✅ |
| Google BigQuery | ❌ *(coming soon)* |



[binance-docs-websocket-depth]: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#diff-depth-stream
[binance-docs-websocket-trade]: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#trade-streams
[kaggle-binance-trading-events-dataset]: https://www.kaggle.com/datasets/circeukan/binance-trading-events
[kaggle-binance-trading-events-notebook]: [kaggle-binance-trading-events-dataset]