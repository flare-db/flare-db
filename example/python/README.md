# Python examples

Python counterparts of the Java pipelines under `example/`, submitted to
FlareDB through the Python SDK harness. They exist so the same pipelines can be
run from a second SDK to validate that the runner produces the same results
across SDKs.

| Python example | Java equivalent | Exercises |
|---|---|---|
| [`wordcount.py`](./wordcount.py) | `example/wordcount` | `FlatMap`, `Filter`, `Count.perElement` |
| [`co_groupby_key.py`](./co_groupby_key.py) | `example/co-groupby-key` | `CoGroupByKey` (runner-native `GroupByKey` fan-in, tagged coders) |
| [`flatten.py`](./flatten.py) | `example/flatten` | `Flatten` fan-in + `GroupByKey` |
| [`fixed_window.py`](./fixed_window.py) | `example/fixed-window` | `FixedWindows`, event-time timestamps, windowed `GroupByKey` |
| [`sliding_window.py`](./sliding_window.py) | `example/sliding-window` | `SlidingWindows` (overlapping windows) |
| [`timers.py`](./timers.py) | `example/timers-example` | `BagState`, processing-time + event-time timers, timer-only re-runs, self-verification |

The FlareIO Java examples (`example/flare-io-read`, `example/flare-io-write`)
are intentionally not ported here.

## Setup

From the repository root:

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install apache-beam
pip install -e runner-sdk/python/flarerunner
```

Start a local FlareDB instance (see the repository root `README.md`), then run
any example:

```sh
python3 example/python/wordcount.py
python3 example/python/co_groupby_key.py
python3 example/python/flatten.py
python3 example/python/fixed_window.py
python3 example/python/sliding_window.py
python3 example/python/timers.py
```

All examples target the local job service at `127.0.0.1:8099`; override it with
`--job_endpoint=host:port`. `fixed_window.py`, `sliding_window.py` and
`timers.py` read `test-data/scores.csv` relative to the repository root.

## Notes

- Fixed-window, sliding-window and timers read the input file on the driver and
  feed it through `beam.Create`, matching the existing `wordcount.py` (which
  embeds `test-data/thirukkural.txt` rather than reading it). This keeps the
  examples focused on the transform and windowing/timer behavior under test.
- `timers.py` writes its `@on_timer` results to `build/timers-example-flush.txt`
  (and `.event`) and asserts them against counts computed from the input,
  failing the run on any mismatch.
