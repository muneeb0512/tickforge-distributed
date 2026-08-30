# Milestone 2 — Bounded Queue + Kafka

Design reference for the distributed publishing path. For the full
concept-by-concept teaching (backpressure, Kafka fundamentals, delivery
semantics), see the Milestone 2 discussion in the project conversation
history; this document is the durable, condensed version.

## 1. Architecture

```
MarketDataSource (Live or Replay)          [Milestone 1]
        │  next() -> MarketEvent
        ▼
   ingest thread                            IngestionPipeline
        │  push()
        ▼
BoundedQueue<MarketEvent>   ◄── in-process, milliseconds, no persistence
        │  pop()
        ▼
   publish thread
        │  EventPublisher::publish()
        ▼
KafkaEventProducer (real) / FakeEventPublisher (tests)
        │  rd_kafka_producev(), keyed by symbol
        ▼
┌───────────────────────────────────────────────┐
│  Kafka topic "market-events", 3 partitions      │  ◄── cross-process, hours/days, persistent
└───────────────────────────────────────────────┘
        │                              │                      │
        ▼ partition 0                  ▼ partition 1          ▼ partition 2
   KafkaEventConsumer (services/consumer) - separate OS process, shares
   nothing with ingestion but the topic
```

`EventPublisher` is the same dependency-inversion pattern as
`MarketDataSource` (Milestone 0/1), applied to the opposite boundary:
`IngestionPipeline` depends on the abstraction, never concretely on Kafka,
so its threading/queue/shutdown logic is unit-tested with an in-memory
`FakeEventPublisher` - no live Kafka broker required for `ctest`.

## 2. Why the queue still exists (condensed)

Kafka decouples *processes*, for hours; the queue decouples *threads*, for
milliseconds. Removing the queue wouldn't remove the need to keep the
WS-read thread from ever waiting on a Kafka call - it would just move that
coupling back into one thread doing two jobs, exactly what Milestone 1
already ruled out. Full reasoning: project conversation, Milestone 2
teaching section.

## 3. `IngestionPipeline`: mirrors Project 1's pipeline exactly, on purpose

Same shape as `MarketDataPipeline` (tickforge-cpp handoff §6): N producer
thread(s) → `BoundedQueue<T>` → 1 consumer thread. Same
`requestStop()`/`stop()` split, for the same reason: `LiveSource::next()`
never ends on its own, so `stop()` alone (which waits for the ingest
thread to finish before proceeding) would hang forever - exactly Project
1's documented deadlock (handoff §14). `requestStop()` calls
`source_->stop()`, which is what actually bounds that wait.
`tests/ingestion_pipeline_test.cpp`'s
`RequestStopThenStopCompletesPromptlyForAnUnboundedSource` exercises this
directly, against a `FakeInfiniteSource` test double built for exactly
this purpose.

Shutdown ordering in `stop()`: join the ingest thread *first* (bounded,
because `requestStop()` was already called, or the source ended on its
own), **then** shut the queue down, **then** join the publish thread. That
ordering is what lets the publish thread drain every already-queued event
through to the publisher before the process exits - nothing accepted from
the network before shutdown is dropped during shutdown either.

## 4. Kafka: what was actually built

- **Broker:** `apache/kafka:4.3.1`, single-node, KRaft mode (Kafka 4.x has
  no ZooKeeper mode at all), via Docker Compose. Config is Apache's own
  official single-node example, trimmed.
- **Topic:** `market-events`, 3 partitions, replication factor 1 (single
  broker - no redundancy; a real cluster would use ≥3 for durability).
  Created by a one-shot `kafka-topic-init` container that waits for the
  broker's healthcheck, then runs `kafka-topics.sh --create --if-not-exists`.
- **Producer (`KafkaEventProducer`):** wraps librdkafka's C API directly
  (not a C++ wrapper library - see the decision log). Message value is
  the same JSON produced by `marketEventToJson` (Milestone 1's capture
  format) - no new serialization was introduced, per the project's own
  "prefer JSON, defer binary serialization" rule. Message key is
  `event.symbol` - the entire mechanism behind "partition by instrument."
  `publish()`'s return value reflects only a local, immediate failure
  (e.g. librdkafka's outgoing queue full); actual broker delivery success
  or failure arrives later, asynchronously, via the delivery-report
  callback (`deliverySuccessCount()`/`deliveryFailureCount()`).
- **Consumer (`KafkaEventConsumer`):** high-level, consumer-group-balanced
  API (`rd_kafka_subscribe` + `rd_kafka_consumer_poll`).
  `enable.auto.commit` left at librdkafka's default (true) - deliberately,
  since that default is what makes this consumer at-least-once rather
  than exactly-once (see §6). `auto.offset.reset=earliest` by default, so
  a fresh `group.id` replays the whole topic - a deliberate choice for a
  repeatable demo, not necessarily what a long-running production
  consumer would want.
- **Why no custom Kafka reconnect logic**, unlike Milestone 1's `LiveSource`:
  librdkafka already owns broker connection resilience internally. Beast
  gave us nothing equivalent for the WebSocket, which is why Milestone 1
  had to build backoff/reconnect by hand; building a second one on top of
  librdkafka here would just be two systems contesting the same
  connection state.

## 5. Partitioning and ordering - stated precisely

Kafka guarantees order **within one partition**, and makes **no**
guarantee across partitions. Keying every message by `symbol` means every
`BTCUSDT` event lands in the same partition, in production order; it says
nothing about `BTCUSDT` versus `ETHUSDT`'s relative order, which can
interleave arbitrarily. **This project does not claim global ordering
across the topic.** The `services/consumer` demo logs `[partition P
offset O]` per message specifically so this is verifiable by reading
output, not just asserted in a comment.

## 6. Delivery semantics - what's real, what isn't

At-least-once, not exactly-once, because nothing in this implementation
provides exactly-once. Two independent duplicate sources exist:

- **Producer side:** a lost/timed-out acknowledgment can cause librdkafka
  to resend a message the broker already stored.
- **Consumer side:** `enable.auto.commit`'s timer-based offset commits
  mean a consumer crash between "processed a message" and "the next
  auto-commit" causes that message to be re-delivered on restart.

Neither is worked around in this milestone - idempotent downstream
processing (using `provider_sequence` as identity) is Milestone 3's job,
once there's a state store where "processing twice" would otherwise cause
a visible problem.

## 7. Failure handling implemented this milestone (basic only)

- **Kafka unavailable at producer/consumer startup:** doesn't throw or
  crash - `rd_kafka_new()` succeeds even if the broker isn't reachable
  yet; librdkafka retries connecting internally.
- **Producer errors:** a failed `publish()` is counted
  (`IngestionPipeline::Stats::publish_failures`) and logged; the pipeline
  keeps running. No retry/backoff of individual failed publishes yet.
- **Consumer shutdown:** `stop()` sets an atomic flag; `next()`'s loop
  notices it between bounded `rd_kafka_consumer_poll()` calls - no
  force-close trick needed, unlike `LiveSource` (Beast's blocking read had
  no built-in timeout to hang the check off of).
- **Graceful shutdown:** see §3 - explicitly ordered to drain the queue
  before exiting.

Explicitly **not** built yet (Milestone 5): retry/backoff for individual
failed publishes, a circuit breaker, Kafka consumer lag monitoring, health
checks, structured metrics export beyond stdout counters.

## 8. Results - measured, not invented

All numbers below are from real runs against the live Binance stream and/or
the real Docker Kafka broker on this machine, on 2026-08-29/30. Single
runs, one machine - the same caveat Project 1's own benchmarks carried
(handoff §17/§22): absolute numbers won't transfer to another machine, the
*shape* of the results is what's meaningful.

**End-to-end, live Binance -> queue -> Kafka -> consumer** (20s,
`btcusdt,ethusdt`): 180 real trades received, published locally, and
confirmed consumed - `received=180 published=180 publish_failures=0`,
consumer reported `Total consumed: 180`. Queue high-water mark peaked at
59/1024 - nowhere near saturation at live Binance's actual rate (§ below
explains why that's expected, not a non-result).

**Partitioning by instrument - real, observed, not just asserted.**
First run (`btcusdt`, `ethusdt` only): all 180 events landed in **partition
0** for both symbols. This is not a bug - with only 2 distinct keys hashed
across 3 partitions, landing in the same bucket is an entirely ordinary
outcome of hashing, not evidence partitioning "isn't working." Second run,
adding a third symbol (`solusdt`): of 208 events consumed from the
topic's start, **200 landed in partition 0** (`btcusdt`/`ethusdt`) and
**8 landed in partition 2** (`solusdt`) - confirmed real separation once a
key hashed differently. Consistent across repeated runs: the same symbol
always lands in the same partition, exactly as designed; *which* partition
is an implementation detail of the hash function, not something this
project chooses or should be expected to predict.

**Backpressure under a higher input rate.** Full-speed replay of the
329-event captured fixture against a deliberately small queue
(`--queue-capacity 8`): `queue_high_water_mark=8/8` - the queue reached
**exact** full capacity, meaning the ingest thread genuinely blocked on
`push()` at least once, waiting for the publish thread to catch up.
`published(local)=329, publish_failures(local)=0` - backpressure blocked
production, it did not drop anything. This is what "what happens if the
downstream producer cannot keep up" looks like in practice: the queue
fills, the ingest side stalls, nothing is lost.

**Throughput** (`benchmarks/queue_kafka_benchmark`, real captured fixture
data replayed 20x = 6,580 events - repeated real data, not synthetic):

| Stage | Events | Time | Throughput |
|---|---|---|---|
| Queue only (no Kafka) | 6,580 | 0.0024s | ~2,761,928 events/sec |
| Kafka producer (real broker) | 6,580 | 0.393s | ~16,743 events/sec |
| Kafka consumer (real broker) | 6,580 | 0.571s | ~11,532 events/sec |

The ~165x gap between raw queue throughput and Kafka producer throughput
is the concrete, measured version of §2's qualitative point: the
in-process handoff is nowhere near the bottleneck. Kafka's real network
round trips are - by roughly three orders of magnitude - which is exactly
why the queue's job (decouple two threads at very different speeds) is
worth doing, and exactly why nobody should expect the queue itself to ever
be what limits this pipeline.

**Kafka unavailable - the local-vs-broker distinction, proven, not just
documented.** Stopped the broker (`docker compose stop kafka`), then ran
ingestion against the 329-event fixture:

```
Final stats: received=329 published(local)=329 publish_failures(local)=0
             delivered(broker)=0 delivery_failed(broker)=0
             queue_high_water_mark=32/32
```

Every one of the 329 local `producev()` calls succeeded - librdkafka
happily buffered them internally - while **zero** were ever confirmed
delivered, because the broker was never reachable. Exactly the gap §7's
design predicted: `publish()` succeeding is not the same claim as "Kafka
has it." No crash, no exception, exit code 0. librdkafka logged its own
retries and, on shutdown, an explicit warning: *"Producer terminating
with 329 messages (39793 bytes) still in queue or transit: use flush() to
wait for outstanding message delivery."* One honest, real cost observed:
graceful shutdown took noticeably longer than usual, because
`flush(10s)` genuinely waits up to its full timeout trying to deliver
before giving up on an unreachable broker - a real trade-off between "try
hard not to lose data" and "shut down fast" that this milestone accepts
as-is (Milestone 5 territory to improve). Restarting the broker and
re-running the same fixture immediately afterward: `delivered(broker)=5`
for all 5 events - confirmed recovery, same code path, no restart of our
own process needed.

## 9. Running it

```bash
# Bring up Kafka (KRaft broker + topic-init, idempotent):
docker compose up -d

# Terminal 1 - consumer (subscribes first, so nothing produced is missed
# by a live watch, though auto.offset.reset=earliest means late-starting
# is fine too):
KAFKA_CONSUMER_GROUP=demo ./build/services/consumer/tickforge_consumer_service

# Terminal 2 - ingestion, live Binance -> queue -> Kafka:
./build/services/ingestion/tickforge_ingestion_service

# Or replay (no network) at full speed, small queue capacity, to make
# backpressure visible:
./build/services/ingestion/tickforge_ingestion_service \
    --replay replay/btcusdt_ethusdt_sample.jsonl --queue-capacity 8

# Benchmarks (needs Kafka running):
./build/benchmarks/tickforge_queue_kafka_benchmark
```

## 10. Known limitations of this milestone

- Single Kafka broker, replication factor 1 - no durability against a
  broker crash (a real cluster would use ≥3 brokers, replication factor
  ≥3). Acceptable for a local dev/demo environment; would be a real gap
  in anything resembling production.
- No retry/backoff for individual failed publishes.
- No consumer lag monitoring - `services/consumer` reports what it
  consumed, not how far behind it is.
- No idempotency enforcement anywhere yet - duplicates from either source
  described in §6 are possible and currently unhandled, because nothing
  downstream yet cares (no state store exists until Milestone 3).
- Benchmark numbers reflect one machine, one run, repeated real data - not
  statistically repeated trials (same caveat Project 1's own benchmarks
  carried, handoff §17/§22).
- **No persistent volume for the Kafka broker's data.** `KAFKA_LOG_DIRS`
  lives in the container's writable layer, not a mounted volume - every
  container recreation (as opposed to a plain restart) starts with an
  empty topic. Discovered directly during this milestone: an abrupt
  machine power-off mid-session forced a `wsl --shutdown` recovery, which
  caused Docker to recreate the broker container, silently wiping the
  topic created earlier in the same session. Acceptable for a local
  dev/demo environment where topics are cheap to recreate
  (`docker compose up -d` re-runs `kafka-topic-init` idempotently); would
  need a named volume before this could be trusted with anything meant to
  persist.

## 11. Decision log

- Selected librdkafka's C API directly over a C++ wrapper (cppkafka,
  modern-cpp-kafka) - apt-installable with no `FetchContent` source
  build, and it's what nearly every language's Kafka client actually
  wraps, so the API being learned here transfers directly.
- Selected `apache/kafka` (official image) over Confluent's
  `cp-kafka` - no Confluent-specific extensions, and KRaft-native since
  the image's earliest published versions.
- 3 partitions (not 1) specifically so partitioning-by-key has a visible
  effect to demonstrate - with 1 partition, "partition by instrument"
  would be trivially true and unobservable. Confirmed with real traffic:
  see §8.
- Introduced `services/` this milestone (not at Milestone 0) - the first
  moment two genuinely independent, separately-run OS processes exist,
  sharing nothing but Kafka. `src/tickforge_ingest_demo` (Milestone 1,
  no Kafka) stays where it is - still a valid, distinct, lower-level dev
  tool, not superseded by the new services.
- Added `KafkaEventProducer::deliverySuccessCount()`/`deliveryFailureCount()`
  visibility to `services/ingestion`'s printed stats mid-milestone, after
  the Kafka-unavailable failure test (§8) made clear that
  `IngestionPipeline::Stats::publish_failures` alone can't answer "did
  Kafka actually get this data" - only the async delivery-report counts
  can. `EventPublisher` itself stays generic on purpose; `main.cpp` is
  the one place that deliberately reaches past the abstraction to read
  Kafka-specific state, because it's the one place that legitimately
  knows it's talking to Kafka.
