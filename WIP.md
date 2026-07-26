## WIP (Outdated)

### CHANGELOG (highlights to remember later)
    - introduced monitoring domain responsible for monitoring Fills and producing portfolio/position updates
    - introduced protocol pkg which owns types and interfaces. It defines domains boundaries types and interfaces
    - introduced dependency injection from main for high app configuration flexibility
    - internal/registry for dependency injection now includes `engine`
    - features, strategies, providers have "disabled" flag in config.json

### InProgress:
    - 

### ToDo (tech)
    - adapt 'data_logger' and 'full' engine to new domain/processors layer
    - adapt Strategy interfaces/instances to receive ExtendedMarketData type
    - implement 'fibonnaci' and 'trend' processors
    - backtesting engine (see BACKTESTING.md)