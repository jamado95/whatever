# Config guide

How to fill in a config file for the engine. One JSON file describes one run: where the data comes
from, which streams to follow, what to compute on them, and which engine runs it.

## Start here

A complete `data_logger_v2` config: BTCUSDT 5m with a rolling 1h derived from it, plus a direct 1d.

```json
{
  "providers": [
    { "type": "binance_csv", "name": "btc_5m", "file": "./data/BTCUSDT/BTCUSDT-5m-2024-01.csv" },
    { "type": "binance_csv", "name": "btc_1d", "file": "./data/BTCUSDT/BTCUSDT-1d-2024-01.csv" }
  ],
  "subscriptions": [
    { "symbol": "BTCUSDT", "timeframe": "5m", "provider": "btc_5m", "derive": ["1h"] },
    { "symbol": "BTCUSDT", "timeframe": "1d", "provider": "btc_1d" }
  ],
  "universes": [
    { "name": "intraday", "rules": [{ "timeframe": ["5m", "1h"] }] }
  ],
  "features": [
    { "type": "rsi", "universe": "intraday", "variants": [{ "period": 14 }] },
    { "type": "ema", "variants": [{ "period": 20 }, { "period": 50 }] }
  ],
  "engine": {
    "type": "data_logger_v2",
    "exporter": { "type": "jsonl", "output_dir": "data/exports" },
    "options": { "limit": 1000, "ticker": { "type": "fixed", "interval": "50ms" } }
  }
}
```

Check a file without running it:

```bash
make validate CONFIG=configs/my_config.json
```

The config is strict:

- Unknown keys and invalid values are errors, never ignored.
- Errors name their path. File errors are reported together first; once they are fixed, construction
  errors are (unmatched stream names among them).
- Blocks with `"disabled": true` are skipped and not checked.

## Blocks

| Block | Required | Holds |
|---|---|---|
| `providers` | yes | Data sources, one instance per subscription |
| `subscriptions` | yes, at least one | The streams to follow |
| `features` | no | Indicators computed on the streams |
| `universes` | no | Named sets of streams and pairs that features bind to |
| `strategies` | no | Trading logic; none in the default build |
| `engine` | yes | Which engine runs, its exporter and options |

## Providers

| Field | Meaning |
|---|---|
| `type` | `binance_csv` or `binance_historical` |
| `name` | The instance name subscriptions reference. Defaults to `type`; unique among enabled providers; must not start with `derived_` |
| `disabled` | `true` skips the provider |

Per type:

| Type | Options |
|---|---|
| `binance_csv` | `file`, required: a Binance kline CSV. Its existence is checked when the run starts, not by `make validate` |
| `binance_historical` | `startTime`, `endTime`: Unix ms, `0` = unbounded; `endTime` after `startTime`. Fetched from the Binance API when the run starts |

Give each subscription its own provider instance: two subscriptions naming one provider fail when the
run starts.

## Subscriptions

| Field | Meaning |
|---|---|
| `symbol` | The market, e.g. `BTCUSDT` |
| `timeframe` | Bar size: `1m`, `5m`, `15m`, `1h`, `1d` |
| `provider` | The `name` of an enabled provider |
| `derive` | Optional, `data_logger_v2` only: rolling windows built from this stream, each coarser than `timeframe` and a multiple of it, e.g. `["1h", "1d"]` from `5m` |

Every stream has an ID, the name universes and strategies use:

- A direct stream's ID is its subscription's, `symbol:timeframe:provider`: `BTCUSDT:5m:btc_5m`.
- A derived stream's is `symbol:timeframe:derived_<source ID>`: `BTCUSDT:1h:derived_BTCUSDT:5m:btc_5m`.

## Features

| Field | Meaning |
|---|---|
| `type` | The feature type, below |
| `disabled` | `true` skips the entry |
| `universe` | Optional: the name of the universe it runs on |
| `variants` | One instance per entry, each its own options. Absent: one instance without options |

| Type | Options | Instance ID |
|---|---|---|
| `sma` | `period`, required | `sma_20` |
| `ema` | `period`, required; `smoothing`, default `2` | `ema_20`; `ema_20_s3` with `smoothing` 3 |
| `rsi` | `period`, required | `rsi_14` |
| `vwap` | `period`, required | `vwap_20` |
| `engulfing_candle` | none | `engulfing_candle` |

Two instances with the same ID are an error: they are configured identically.

## Universes

Validated, but not yet used: no engine reads them until the feature engine lands.

A universe is a `name` and a list of `rules`. Its streams and pairs are the union of what its rules
select. Names are unique; a universe needs at least one rule; an unused universe is valid.

**Filters** choose streams. A list is OR, fields are AND, an absent field does not constrain, and
an empty list or value is an error.

| Filter | Matches | Example |
|---|---|---|
| `symbol` | The symbol; globs (`*`, `?`, `[…]`) | `["*USDT"]` |
| `timeframe` | The bar size; a derived stream's is its window | `["1h"]` |
| `provider` | The provider instance name; globs; a derived stream matches its source's | `["btc_*"]` |
| `kind` | `direct` or `derived` | `["direct"]` |

**Rules** take filters and at most one pairing:

| Rule | Selects |
|---|---|
| Filters only | The streams passing them |
| `references` | Each referenced stream, paired with every stream passing the filters |
| `pairs`: a list | The pairs as written; takes no filter |
| `pairs`: `"*"` | Every pair of the streams passing the filters |

- `references` and `pairs` name streams by their stream IDs, taken as named: filters, `kind`
  included, choose partners only. A name that is no stream's ID is an error.
- Pairs are unordered; a stream is never paired with itself; duplicates collapse.

**Binding.** `universe` on a feature entry runs it on the universe's streams; without one, a
single-stream feature runs on every stream. Pairs serve pair features, none registered yet. A
universe a feature uses must select at least one stream.

### Examples

Five subscriptions, one provider instance each:

```json
{ "symbol": "BTCUSDT", "timeframe": "5m", "provider": "btc_5m", "derive": ["1h"] },
{ "symbol": "BTCUSDT", "timeframe": "1h", "provider": "btc_1h" },
{ "symbol": "ETHUSDT", "timeframe": "5m", "provider": "eth_5m", "derive": ["1h"] },
{ "symbol": "SOLUSDT", "timeframe": "1h", "provider": "sol_1h" },
{ "symbol": "ETHBTC",  "timeframe": "1h", "provider": "ethbtc_1h" }
```

give seven streams:

| | Stream ID | Kind |
|---|---|---|
| A | `BTCUSDT:5m:btc_5m` | direct |
| B | `BTCUSDT:1h:derived_BTCUSDT:5m:btc_5m` | derived from A |
| C | `BTCUSDT:1h:btc_1h` | direct |
| D | `ETHUSDT:5m:eth_5m` | direct |
| E | `ETHUSDT:1h:derived_ETHUSDT:5m:eth_5m` | derived from D |
| F | `SOLUSDT:1h:sol_1h` | direct |
| G | `ETHBTC:1h:ethbtc_1h` | direct |

One rule each:

| Rule | Streams | Pairs | Shows |
|---|---|---|---|
| `{}` | A–G | — | no filter selects every stream |
| `{"symbol": ["*USDT"]}` | A–F | — | a glob |
| `{"timeframe": ["1h"]}` | B C E F G | — | derived streams match their window |
| `{"timeframe": ["1h"], "kind": ["direct"]}` | C F G | — | `kind` drops derived streams |
| `{"symbol": ["BTCUSDT", "ETHUSDT"], "timeframe": ["1h"]}` | B C E | — | OR within a field, AND across |
| `{"provider": ["*_5m"]}` | A B D E | — | derived streams match their source's provider |
| `{"references": ["BTCUSDT:1h:btc_1h"], "timeframe": ["1h"]}` | B C E F G | CB CE CF CG | a reference against every partner, never itself |
| `{"references": ["BTCUSDT:5m:btc_5m"], "symbol": ["*USDT"], "timeframe": ["1h"]}` | A B C E F | AB AC AE AF | the reference is taken as named: filters choose partners |
| `{"references": ["BTCUSDT:1h:btc_1h", "SOLUSDT:1h:sol_1h"], "symbol": ["ETH*"], "timeframe": ["1h"]}` | C E F G | CE CG FE FG | references are not paired with each other; `ETH*` matches `ETHBTC` |
| `{"references": ["BTCUSDT:1h:derived_BTCUSDT:5m:btc_5m"], "timeframe": ["1h"], "kind": ["direct"]}` | B C F G | BC BF BG | a derived reference by its ID; `kind` chooses partners only |
| `{"references": ["BTCUSDT:1h:btc_5m"]}` | error | | no stream has that ID: a derived stream is named by its own |
| `{"pairs": [["BTCUSDT:1h:btc_1h", "ETHUSDT:1h:derived_ETHUSDT:5m:eth_5m"], ["SOLUSDT:1h:sol_1h", "ETHBTC:1h:ethbtc_1h"]]}` | C E F G | CE FG | explicit pairs, as written |
| `{"pairs": [["BTCUSDT:1h:btc_1h", "SOLUSDT:1h:sol_1h"]], "kind": ["direct"]}` | error | | explicit pairs take no filter |
| `{"pairs": "*", "symbol": ["*USDT"], "timeframe": ["1h"]}` | B C E F | BC BE BF CE CF EF | every pair, same-symbol included |
| `{"pairs": "*", "symbol": ["*USDT"], "timeframe": ["1h"], "kind": ["direct"]}` | C F | CF | `kind` keeps one stream per symbol here |
| `{"pairs": "*", "symbol": ["BTCUSDT"]}` | A B C | AB AC BC | pairs across timeframes |

A universe of three rules:

```json
{ "name": "majors", "rules": [
  { "pairs": "*", "symbol": ["*USDT"], "timeframe": ["1h"], "kind": ["direct"] },
  { "pairs": [["SOLUSDT:1h:sol_1h", "BTCUSDT:1h:btc_1h"]] },
  { "symbol": ["ETHBTC"] }
] }
```

resolves to streams C F G and the single pair CF: rules union, a pair written either way is one pair,
and a rule without a pairing adds streams only.

Bound to features:

```json
"universes": [{ "name": "usdt", "rules": [{ "symbol": ["*USDT"] }] }],
"features": [
  { "type": "rsi", "universe": "usdt", "variants": [{ "period": 14 }] },
  { "type": "ema", "variants": [{ "period": 20 }] }
]
```

`rsi_14` runs on A–F, `ema_20` on A–G.

## Strategies

| Field | Meaning |
|---|---|
| `type` | The strategy type |
| `disabled` | `true` skips the strategy |
| `subscriptions` | The stream IDs it trades, each a declared subscription |

Any other key is the strategy's own option. No strategy is registered in the default build.

## Engine

| Field | Meaning |
|---|---|
| `type` | `data_logger_v2`, or `data_logger`, which rejects `derive` |
| `strategies` | Strategy types to run, each enabled under `strategies` |
| `exporter` | Optional: `type` `jsonl`, `output_dir` (required), `disabled` |
| `options.limit` | Frames to run, `0` = unlimited |
| `options.ticker` | Pacing: `{ "type": "fixed", "interval": "50ms" }` waits between frames; `realtime`, or no `ticker`, runs as fast as the data allows |

## Common errors

| Mistake | Error |
|---|---|
| A misspelt key | `failed to parse config: json: unknown field "refrences"` |
| An invalid timeframe | `failed to parse config: invalid timeframe "1hr", want one of [1m 5m 15m 1h 1d]` |
| An invalid `kind` | `failed to parse config: invalid stream kind "window", want one of [direct derived]` |
| `pairs` neither `"*"` nor a list | `failed to parse config: pairs must be "*" or a list of pairs, got "all"` |
| A subscription without a provider | `subscriptions[1]: missing required 'provider' field` |
| The same subscription twice | `subscriptions[2]: duplicate subscription "BTCUSDT:1d:btc_1d"` |
| A derived window not coarser | `subscriptions[1] (BTCUSDT:1d:btc_1d): derive "1h" must be coarser than "1d"` |
| Two providers with one name | `providers[1]: duplicate provider name "btc_5m"` |
| A provider named `derived_…` | `providers[1]: name "derived_1d" must not start with "derived_"` |
| A feature naming an unknown universe | `features[0] (rsi): unknown universe "usdt"` |
| An empty list in a rule | `universes[0] (btc): rules[0].symbol: empty list` |
| `references` and `pairs` in one rule | `universes[0] (btc): rules[1]: references and pairs are exclusive` |
| A filter on explicit pairs | `universes[0] (btc): rules[1]: explicit pairs take no filter` |
| A name that is no stream's ID | `universe "btc": rules[1].references[0]: no stream has ID "BTCUSDT:1h:btc_5m"` |
| A used universe selecting nothing | `universe "btc": selects no streams` |
| A subscription naming an unknown provider | `subscriptions[1] (BTCUSDT:1d:ghost) references provider "ghost", which was not instantiated (disabled or missing)` |
| An unregistered feature type | `feature "macd" not registered` |
| A misspelt feature option | `feature "rsi" variant 0: json: unknown field "periodd"` |
| Two identical variants | `engine "data_logger_v2": duplicate feature "rsi_14": two variants are configured identically` |
| `fixed` pacing without an interval | `engine "data_logger_v2": ticker: 'interval' is required and must be positive when type is "fixed"` |
| `derive` on `data_logger` | `engine "data_logger": data_logger: subscription "BTCUSDT:5m:btc_5m": derived subscriptions are not supported` |

Two mistakes pass `make validate` and fail when the run starts:

| Mistake | Error |
|---|---|
| A missing CSV file | `provider of BTCUSDT:1d:btc_1d: init: file does not exist: ./data/nope.csv` |
| Two subscriptions on one provider | `data: subscriptions "BTCUSDT:1d:btc_5m" and "BTCUSDT:5m:btc_5m" share one provider instance` |
