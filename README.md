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

**Milestone 2 — Bounded queue + Kafka.** Real Binance trades now flow
WebSocket → bounded queue → Kafka producer → Kafka (3 partitions,
partitioned by instrument) → Kafka consumer, as two independent processes
(`services/ingestion`, `services/consumer`) sharing nothing but the topic.
No Redis, no PostgreSQL yet - see
[`docs/architecture.md`](docs/architecture.md) §12 for the full milestone
roadmap, [`docs/milestone-1-ingestion.md`](docs/milestone-1-ingestion.md)
for the WebSocket ingestion design, and
[`docs/milestone-2-queue-kafka.md`](docs/milestone-2-queue-kafka.md) for
the queue/Kafka design and real measured results (throughput, backpressure,
a real Kafka-outage test).

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

As of Milestone 2, also requires (WSL2 Ubuntu, via apt):

```bash
sudo apt-get install -y libboost-dev libssl-dev nlohmann-json3-dev librdkafka-dev
```

`libboost-dev` provides Boost.Asio/Beast (WebSocket + TLS client, header-only
usage - no Boost libraries are linked); `libssl-dev` is OpenSSL, for the TLS
handshake; `nlohmann-json3-dev` is the JSON library used at the provider
boundary; `librdkafka-dev` is the Kafka client (found via `pkg-config`, not
a CMake config package). None of this needs an internet connection to build
or run the `ctest` suite - only the live WebSocket path and anything
touching a real Kafka broker (see below) does.

## Running it

**Ingestion only, no Kafka** (Milestone 1's demo - still valid, unchanged):

```bash
./build/src/tickforge_ingest_demo                              # live
./build/src/tickforge_ingest_demo --record replay/sample.jsonl # live + capture
./build/src/tickforge_ingest_demo --replay replay/sample.jsonl # deterministic, no network
```

**Full distributed path** (Milestone 2 - needs Docker):

```bash
# Bring up Kafka (single-node KRaft broker + topic creation, idempotent):
docker compose up -d

# Terminal 1 - consumer:
KAFKA_CONSUMER_GROUP=demo ./build/services/consumer/tickforge_consumer_service

# Terminal 2 - ingestion (live Binance -> queue -> Kafka):
./build/services/ingestion/tickforge_ingestion_service

# Or, to see real backpressure: full-speed replay against a small queue
./build/services/ingestion/tickforge_ingestion_service \
    --replay replay/btcusdt_ethusdt_sample.jsonl --queue-capacity 8

# Real throughput numbers (queue / Kafka producer / Kafka consumer):
./build/benchmarks/tickforge_queue_kafka_benchmark
```

`Ctrl+C` triggers graceful shutdown everywhere (same signal-handling
pattern as Project 1's server). See
[`docs/milestone-2-queue-kafka.md`](docs/milestone-2-queue-kafka.md) §9 for
more detail and §8 for what these actually produced when run for real.

## Configuration

Copy [`.env.example`](.env.example) to `.env` and fill in values as later
milestones introduce the things they configure (`.env` is gitignored - it
is never committed). Currently read: `TICKFORGE_SYMBOLS` (comma-separated
Binance stream symbols, lowercase; defaults to `btcusdt,ethusdt` - no API
key needed for this provider), `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`,
`KAFKA_CONSUMER_GROUP` (all default to values matching `docker-compose.yml`).

## Repository layout

See [`docs/architecture.md`](docs/architecture.md) §11 for the current tree
and what each future directory (`integration/`, `.github/workflows/`) is
reserved for and when it appears.
