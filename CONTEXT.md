# AI_CONTEXT.md

Project
Go-based trading / execution engine.

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

Scripts
- /scripts: helpers for data retrieval and other utils external to pkg 
- /data: ignore

Constraints
- Prefer localized changes
- Avoid cross-domain refactors unless requested
- Do not modify pipeline or protocol unless explicitly asked
