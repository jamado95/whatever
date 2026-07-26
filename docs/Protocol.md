# Protocol Components

## Data layer
Ingest data in two ways. Static data ingestion (like .csv kline data from Binance historical data files) and market data APIs, both historical and live data. Meant to abstract data ingestion and normalise data points for further processing inside the pipeline. Meant to be expanded to all kinds of data sources and APIs, like FX, crypto, stocks etc...
- In the future should also support other assets' data like options or futures

## Features
consumes ingested data points and expands them with timestamped snapshot of custom metrics, indicators and technical signals. More complex features can result from composition of different, simpler features. 

## Strategy layer
Independent strategy modules that receive featured data, analise it and produce buy and sell signals. Strategies can maintain internal state if memory between data points is required. This layer is responsible for implementing the actual trading strategy based on available, processed data. 

## Execution layer
Layer responsible for interfacing with live trading / broker API. Like the data layer, it's meant as an abstraction layer for broker APIs, containing within itself the specifities of each broker's API, including request and response shapes and authentication. Executes buy and sell orders based on risk management layer signals. Responsible for managing and reporting on request failures, and third party API availability and overall performance. Includes backoff and retry strategies. Implements a testing a mode where orders are simply logged and not execute live.

## Monitor layer
Responsible for capturing realtime data about open market positions and sending it to risk management layer. Potentially merges with the execution layer as an internal yet independent of that layer. The monitoring layer must likely interface with the same brokers that the execution layer already interfaces with. Plus, given it interacts with the third-party dependencies, it can likely reuse some of the availability and error handling logic already in place for the execution layer.

## Risk Management
Maintains overview of full portofolio balance allocation and can make decisions regarding buy and sell signals based on portfolio-wide metrics. Interfaces with the monitoring layer for live market position updates and can act on these signals independently.

## Engine
Connects all layers together. Pipes data between data layer and strategy layer, responsible for data injestion timing. Routes buy/sell signals from strategy modules to risk management singleton, and routes market execution orders to execution layer. Multiple engines can exist with different setups for testing implementation and development iterations.
