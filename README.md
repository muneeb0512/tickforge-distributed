# TickForge Distributed

A real-time market-data platform built as the second project in a
progressive C++ systems-learning portfolio, following **TickForge Core**
(`tickforge-cpp` - see [`PROJECT1_HANDOFF.md`](PROJECT1_HANDOFF.md) for its
full architecture, benchmarks, and lessons). Project 1 was a
single-process, single-machine concurrent backend. This project takes that
same proven internal pipeline and adds a distributed layer on top of it: a
real external market-data feed, Kafka as an event backbone, Redis for
shared hot state, and PostgreSQL for durable history.

> **This is an educational infrastructure project.** It is not a trading
> system, does not execute orders, is not production-ready, and nothing it
> outputs should be used to make an investment decision.

## Status

**Milestone 1 — Real market data + WebSocket ingestion.** A real,
persistent WebSocket connection to Binance's public trade stream, parsed,
validated, and normalized into `MarketEvent`, plus a deterministic
replay/capture path for testing without a live connection. No Kafka, no
Redis, no PostgreSQL yet - see [`docs/architecture.md`](docs/architecture.md)
§12 for the full milestone roadmap and
[`docs/milestone-1-ingestion.md`](docs/milestone-1-ingestion.md) for this
milestone's design in detail (provider research, field mapping, reconnect
design).

## Architecture

Full explanation, diagrams, and the reasoning behind every major decision
live in [`docs/architecture.md`](docs/architecture.md). Short version - the
target end state:

```
Real WebSocket -> C++ ingestion (parse/validate/normalize) -> MarketEvent
       -> BoundedQueue<MarketEvent> -> Kafka producer -> Kafka (partitioned by symbol)
       -> [hot path] C++ processor -> Redis -> TCP query service
       -> [cold path] historical consumer -> PostgreSQL
```

`docs/architecture.md` explains, for every piece above: what problem it
solves that Project 1's architecture couldn't, what the alternatives were,
and what trade-offs and failure modes it introduces.

## Building

Developed and built via WSL2 Ubuntu, against this same repository through
the `/mnt/d` bridge (no separate WSL-native checkout).

```bash
wsl -d Ubuntu
cd "/mnt/d/coding projects/tickforge-distributed"
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Requires a C++20 compiler and CMake ≥ 3.20. `TICKFORGE_BUILD_TESTS`
(default `ON`) fetches GoogleTest `v1.18.0` via `FetchContent` on first
configure, matching Project 1's testing setup.

As of Milestone 1, also requires (WSL2 Ubuntu, via apt):

```bash
sudo apt-get install -y libboost-dev libssl-dev nlohmann-json3-dev
```

`libboost-dev` provides Boost.Asio/Beast (WebSocket + TLS client, header-only
usage - no Boost libraries are linked); `libssl-dev` is OpenSSL, for the TLS
handshake; `nlohmann-json3-dev` is the JSON library used at the provider
boundary. None of this needs an internet connection to build or run the
test suite - only the live WebSocket path (see below) does.

## Running the ingestion demo

```bash
# Live, default symbols (btcusdt, ethusdt) - needs internet:
./build/src/tickforge_ingest_demo

# Live, custom symbols, capturing a replay fixture as it runs:
TICKFORGE_SYMBOLS=btcusdt,solusdt ./build/src/tickforge_ingest_demo --record replay/sample.jsonl

# Deterministic replay from a captured fixture - no network required:
./build/src/tickforge_ingest_demo --replay replay/sample.jsonl
```

`Ctrl+C` triggers graceful shutdown (same signal-handling pattern as
Project 1's server). See [`replay/README.md`](replay/README.md) for the
capture file format.

## Configuration

Copy [`.env.example`](.env.example) to `.env` and fill in values as later
milestones introduce the things they configure (`.env` is gitignored - it
is never committed). As of Milestone 1, `TICKFORGE_SYMBOLS` is the only
variable actually read (comma-separated Binance stream symbols, lowercase;
defaults to `btcusdt,ethusdt` if unset) - no API key is needed for this
provider's public market-data streams.

## Repository layout

See [`docs/architecture.md`](docs/architecture.md) §11 for the current tree
and what each future directory (`services/`, `benchmarks/`, `integration/`,
`.github/workflows/`) is reserved for and when it appears.
