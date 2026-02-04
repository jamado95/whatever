## WIP

CHANGELOG
    - introduced monitoring domain responsible for monitoring Fills and producing portfolio/position updates
    - introduce protocol pkg owning types and interfaces define clean boundaries between domains and fully-contained domain logic
    - moved to dependency injection arch to support full domain instances configuration

InProgress:
    - adapt project structure to golang standards (DONE -> clean up and test)
    - creation of protocol pkg (DONE -> review and test)
    - registry and config review

ToDo (tech)
    - adapt 'data_logger' and 'full' engine to new domain/processors layer
    - adapt Strategy interfaces/instances to receive ExtendedMarketData type
    - implement 'fibonnaci' and 'trend' processors