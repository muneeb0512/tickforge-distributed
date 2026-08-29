# Milestone 1 — Real Market Data + WebSocket Ingestion

Design reference for the ingestion path. For WebSocket/TLS/framing
fundamentals and the full reasoning behind each decision below, see the
Milestone 1 discussion in the project conversation history; this document
is the durable, condensed version.

## 1. Provider: Binance public trade stream

Researched against Binance's own current documentation (not memory) before
choosing. Verified facts, as of this milestone:

| | |
|---|---|
| Endpoint | `wss://stream.binance.com:9443/stream?streams=<symbol>@trade/...` |
| Auth | None required for market-data streams (order/account streams are a separate, authenticated API this project doesn't use) |
| Max connection lifetime | 24 hours, then the server disconnects (a `serverShutdown` warning precedes it) |
| Keepalive | Server pings every 20s; must pong within 60s or it disconnects |
| Rate limits | 5 incoming messages/sec (commands - irrelevant here, we send none), 300 connection attempts / 5 min / IP, 1024 streams / connection |

**Runner-up considered:** Finnhub's real-time US equities WebSocket (free
API key, keeps a stock-market narrative consistent with Project 1's
AAPL/MSFT symbols). Not chosen because it only streams during US market
hours (~9:30am-4pm ET, weekdays) - silent the rest of the time - and has
no documented guaranteed disconnect to reliably exercise reconnect logic
against. Binance's 24/7 market and documented 24h forced disconnect serve
this project's own stated learning objectives better. Trade-off accepted:
instruments are crypto pairs (`BTCUSDT`, `ETHUSDT`), not equities.

**Licensing/reliability:** public market data, standard non-commercial/
educational use; this project makes no investment-advice or trading claims
of any kind (see top-level README).

## 2. Field mapping — the provider boundary

`parseBinanceTradeMessage()` (`src/binance_trade_parser.cpp`) is the *only*
function in the codebase that knows these names:

| Binance field | Meaning | `MarketEvent` field | Notes |
|---|---|---|---|
| `data.s` | symbol | `symbol` | must be non-empty |
| `data.p` | price, **as a string** | `price` (`double`) | parsed via `std::from_chars`; rejected if non-positive or unparseable. Sent as a string specifically so precision loss from string→double happens at one explicit point, not inside a generic JSON number parser |
| `data.T` | trade time, ms since epoch | `event_time` (`system_clock`) | wall-clock: "when the trade happened" |
| `data.t` | trade ID | `provider_sequence` | optional - absent is valid, not an error |
| (n/a) | - | `ingest_time` (`steady_clock`) | set by `LiveSource`, not the parser - "when we received it," not derivable from the payload |
| (n/a) | - | `source` | set by `LiveSource` to `"live:binance"` |

Anything else in Binance's payload (`q` quantity, `m`/`M` maker flags) is
read by nobody - `MarketEvent` doesn't carry fields nothing downstream
needs yet, per the project's own "don't invent fields" rule.

## 3. `LiveSource` design

**Library:** Boost.Beast (WebSocket) + Boost.Asio (TCP) + OpenSSL (TLS),
used **synchronously** (blocking calls), matching Project 1's
`TcpServer` design choice of blocking I/O over an event loop - one thread,
one connection, clarity over juggling many connections at once (which this
project doesn't need).

**Reconnection is transparent to the caller.** `next()` never returns
`false` because of a disconnect - only because `stop()` was called. A
disconnect (24h forced close, ping/pong timeout, network interruption -
indistinguishable to our code, and all three occurred during this
milestone's own development) makes `next()` reconnect internally, with
exponential backoff (1s → 2s → 4s → ... capped at 30s), before resuming
the read loop. This is a first pass: it retries forever with no circuit
breaker and no distinction between "down 5 seconds" and "down 5 hours" -
Milestone 5's job.

**Shutdown, cross-thread.** `stop()` must be callable from a thread other
than the one blocked inside `next()`'s `ws.read()` call. It force-closes
the underlying TCP socket, which makes the blocked read fail immediately -
the same technique Project 1's `TcpServer::stop()` used via `shutdown(2)`
(handoff §12/§14), here via Beast's socket `.close()`. A mutex
(`ws_mutex_`) guards only the *replacement* of the connection object
(during connect/reconnect), never the blocking read call itself - holding
a lock across a blocking call would make `stop()` unable to interrupt it,
defeating the purpose.

## 4. `ReplaySource` and the capture format

`EventRecorder` writes, `ReplaySource` reads: one JSON object per line,
TickForge's own schema (`market_event_json.hpp`), never Binance's. This
is what lets replay fixtures and tests stay stable even if a provider's
wire format changes. `ingest_time` is never captured or restored - it's a
monotonic reading with no meaning outside the process that took it (see
the header comment); a replayed event gets a fresh `steady_clock::now()`
stamped at replay time instead.

Both `LiveSource` and `ReplaySource` apply the same discipline to bad
input: reject the one bad message/line, count it, keep going. Never crash
the loop over one corrupt record.

## 5. Running it

```bash
# Live, default symbols (btcusdt, ethusdt):
./build/src/tickforge_ingest_demo

# Live, custom symbols, also recording a fixture:
TICKFORGE_SYMBOLS=btcusdt,solusdt ./build/src/tickforge_ingest_demo --record replay/sample.jsonl

# Deterministic replay, no network:
./build/src/tickforge_ingest_demo --replay replay/sample.jsonl
```

`Ctrl+C` (SIGINT) triggers the same graceful-shutdown path Project 1 used:
an atomic flag set in the signal handler, noticed by a ~100ms poll loop on
the main thread, which then calls `stop()`.

`replay/btcusdt_ethusdt_sample.jsonl` (329 lines, checked in) is a real
20-second capture against live Binance, taken while building this
milestone - not synthetic data. Replaying it reproduces the identical
event sequence deterministically; verified by diffing the printed output
of a live run against the replay of its own capture.

**A real bug was caught only by actually running this**, not by any unit
test - the same category of lesson Project 1's handoff records twice
(§21): the demo's main thread only exits its shutdown-poll loop on a
signal (SIGINT/SIGTERM). `LiveSource::next()` only ever returns `false`
after `stop()`, so that's fine in live mode - but `ReplaySource::next()`
also returns `false` on ordinary EOF, with no signal involved. The first
`--replay` run hung forever after printing "Ingestion loop finished,"
waiting on a Ctrl+C that was never coming. Fixed by having the ingestion
thread set the shutdown flag itself when its loop ends for *any* reason,
not only reacting to external signals.

## 6. Known limitations of this milestone

- Single connection, one thread - no fan-out across many symbols beyond
  what one combined-stream connection carries.
- No structured metrics export - counters exist (`messagesReceived()`,
  `messagesMalformed()`, `reconnectCount()`) but only reach stdout.
- No circuit breaker / no health signal for "provider has been down for a
  long time" - reconnect retries forever with the same backoff ceiling.
- No connect timeout on the initial TCP/TLS handshake - relies on OS
  defaults, which can be slow to fail. Worth revisiting if it proves
  annoying in practice.
- Replay has no pacing option (always full-speed) - deliberately, to keep
  the mechanism as simple as it can be for what this project needs so far.
