# Architecture Overview 

The core of this project is a live trading engine which is essentially a golang data processing pipeline with event-driven interactions between internal protocol modules.

Processing speed and paralelization are core primitives in this project's development. It must support hundreds (or even thousands) of concurrent data streams internally so as to, for example, develop trading signals based on correlated assets, and manage multi-asset portfolios.

The project also includes a backtesting pipeline. This pipeline is comprised of statistical models and visualisation tools to assess the viability of trading strategies.

## Trading Engine

Entry Point
- /cmd/main.go

Core Architecture
- internal/domains: domain logic (execution, market, monitor, processor, provider, risk, strategy)
- internal/engine: orchestration (backtesting, logging, full runtime)
- internal/pipeline: fanout + pipeline execution
- internal/protocol: domain interfaces
- internal/registry: registers all internal component factories

Design Rules
- Domains contain business logic only
- Dependency injection using a factory registry
  - Backtesting strat param optimisation needs many instances (how to handle TBD)
- Engine wires domains together
- Protocol defines contracts; implementations live in domains
- Pipeline is the primary data/control flow mechanism

Engines
- focus on log/backtesting engines
- meant for live strategy execution

Utilities
- /scripts: helpers for data retrieval and data visualization 
- /data: data exports and downloaded historical market data

Constraints
- Prefer localized changes
- Avoid cross-domain refactors unless requested
- Do not modify pipeline or protocol unless explicitly planned and asked for


## Backtesting

A rough idea of this piece can be found at `docs/Bactesting.md`

This piece is still in planning and under development.
It should live in a separate module, cleanly separated from the trading engine core.


## Exporter

An exported layer is meant to interface between the core trading engine and the backtesting pipeline. Its goal is to feed data and signals produced  by the trading engine to either visualisation scripts or the backtesting pipeline. 

Currently, this exported logic lives in `internal/domains/exporter` and was planned under `docs/plans/data-viz-exporter.md`.

This layer is fully open to expansion, deep design refactoring and further development.