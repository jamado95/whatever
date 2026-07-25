Protocol Components

Data layer: ingest data in two ways. Parses .csv kline data frok Binance historical data files, and pings market data Binance API

Processors: data processors consume from data layer and extend data with timestamped snapshot of custom metrics, indicators and technical signals

Risk Management layer: maintains overview of full portofolio balance allocation and makes decisions regarding buy and sell signals based on portfolio-wide metrics. 

Strategy layer: independent strategy modules that receive data from the data ingestion layer analise it and update their internal state based on the each's trategy considerations. Emit buy and sell signals.

Execution layer: thin layer responsible for executing buy and sell orders based on risk management layer signals. Interacts with live account Binance API. Implements a testing a mode where orders are simply logged and not execute live.

Monitor layer: responsible for capturing realtime data about open market positions and sending it to risk management layer.

Engine: connects all layers together. Pipes data between data layer and strategy layer, responsible for injestion timing. Routes buy/sell signals from strategy modules to risk management singleton, and routes market execution orders to execution layer
