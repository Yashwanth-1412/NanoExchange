# NanoExchange Performance Runner

`run_perf_test.py` starts the exchange, connects as a real OUCH TCP client, generates orders, drains OUCH responses, and stops the exchange with `SIGINT` so the asynchronous logger flushes.

From the NanoExchange root:

```bash
python3 scripts/run_perf_test.py --orders 100000 --warmup 5000
```

The default run uses:

- Engine: `build/NanoExchange`
- OUCH gateway: `127.0.0.1:10000`
- Incremental feed: `127.0.0.1:20001`
- Snapshot TCP: `127.0.0.1:20002`
- Interface: `lo`
- Unpinned runner arguments (`-1`) so the host remains usable

To run the existing vector order-book implementation without changing the map
production binary:

```bash
python3 scripts/run_perf_test.py \
  --engine build/NanoExchangeVector \
  --orders 100000 --warmup 10000 \
  --core-me 12 --core-gateway 2 --core-publisher 4 \
  --core-streamer 6 --core-logger 8 --runner-core 10
```

For a controlled rate:

```bash
python3 scripts/run_perf_test.py --orders 100000 --warmup 10000 --rate 100000
```

For dedicated exchange cores, pass the same core layout used by `NanoExchange`:

```bash
python3 scripts/run_perf_test.py \
  --orders 1000000 --warmup 50000 \
  --core-me 1 --core-gateway 2 --core-publisher 4 \
  --core-streamer 6 --core-logger 8
```

With `isolcpus=2,3 nohz_full=2,3 rcu_nocbs=2,3` on this 16-CPU host, use CPU 2 only for the matching engine. CPU 3 is its SMT sibling and should remain unused. Keep the other components on different physical cores and pin the generator separately:

```bash
python3 scripts/run_perf_test.py \
  --orders 100000 --warmup 10000 --rate 1000 \
  --core-me 2 --core-gateway 4 --core-publisher 6 \
  --core-streamer 8 --core-logger 10 --runner-core 12
```

The runner applies `--runner-core` after starting NanoExchange, so the engine process inherits its full CPU mask and its internal threads apply their own requested pinning.

Each run creates a timestamped directory under `perf-runs/` containing:

- `engine.stdout.log`
- `nanoexchange.log`

The engine log contains the `RDTSC` and `TTT` records emitted by the instrumentation. The generator sends a mixture of new, cancel, and replace orders. Cancel and replace orders use tokens registered by earlier enter orders.

The runner reports the generation rate separately from the response-drain settle time. The engine is stopped with `SIGINT`, allowing `nanoexchange.log` to flush before the run exits.

## Package A Run

Package the completed 100k-operation run into a compact ZIP for Colab:

```bash
python3 scripts/package_perf_run.py \
  --run perf-runs/20260802-162352 \
  --warmup 10000 \
  --output perf-runs/nanoexchange-20260802-162352.zip
```

The ZIP contains parsed gzip CSV files, summary JSON, engine output, and the compressed raw log.

## Visualize In Colab

Upload the ZIP and `scripts/colab_visualize.py` to Colab, then run:

```bash
python3 colab_visualize.py --input nanoexchange-20260802-162352.zip --output visuals
```

Or omit `--input` to use the Colab upload dialog:

```bash
python3 colab_visualize.py --output visuals
```

The script creates percentile bars, distributions, CDFs, and request-index timelines for internal and cross-component latency.
