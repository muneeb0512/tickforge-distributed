# replay/

Captured `MarketEvent` fixtures, one JSON object per line, in TickForge's
own format (`include/tickforge/market_event_json.hpp`) - never a provider's
raw wire format. Produced by running the ingestion demo against the live
source with `--record`:

```bash
./build/src/tickforge_ingest_demo --record replay/btcusdt_sample.jsonl
# let it run for a bit, then Ctrl+C
```

And replayed deterministically with:

```bash
./build/src/tickforge_ingest_demo --replay replay/btcusdt_sample.jsonl
```

Each line looks like:

```json
{"symbol":"BTCUSDT","price":65432.1,"event_time_ms":1735500000119,"provider_sequence":98765432,"source":"live:binance"}
```

`ingest_time` is never stored - it's a monotonic (`steady_clock`) reading
that only means something within the process that recorded it. Replaying a
file stamps a fresh `ingest_time` at replay time instead; see
`market_event_json.hpp` for why.
