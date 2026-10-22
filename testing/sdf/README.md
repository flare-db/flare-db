# SDF integration pipelines

Self-verifying Python pipelines for the Splittable DoFn split/residual loop.
They run against a local FlareDB instance and are meant to be run from the
repository root.

## Prerequisites

- FlareDB running on `127.0.0.1:8099` (for example `./flareup-dev.sh --debug`).
- A Python environment with `apache-beam` and the local FlareDB runner
  (`pip install -e runner-sdk/python/flarerunner`).

## Pipelines

| File | What it exercises | What it verifies |
|---|---|---|
| `channel_split.py` | Several independent job elements in one bundle, slow enough to force a split. The SDK can hand back whole unstarted elements as channel splits. | Every job claims blocks `0..BLOCKS-1` exactly once. |
| `watermark_holds.py` | Multiple timestamped SDF elements, one fixed window, and a downstream `Count.PerKey`. Whole-element residuals must hold the stage output watermark. | The window count equals the number of elements (no early, partial pane). |
| `edge_cases.py` | Empty restriction, single-position restriction, boundary split, a tracker that declines every split, and a slow single position. | Every element's positions are claimed exactly once and the job completes. |

Run them individually:

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -e runner-sdk/python/flarerunner   # this also installs apache-beam==2.76.0
```

```sh
python3 testing/sdf/channel_split.py
python3 testing/sdf/watermark_holds.py
python3 testing/sdf/edge_cases.py
```

Claim/result files are written under `build/sdf/`.

## What these pipelines cannot cover

Malformed runner-side split responses are not reachable from a valid Python
DoFn/SDK harness:

- an empty residual element,
- a regressed or out-of-range `first_residual_element`,
- multiple channel splits for one input PCollection.

Those cases belong in Rust mock/unit tests around
`flaredb/src/engine/sdf.rs` (`poll_progress_and_maybe_split`,
`channel_split_range`, `residual_items`).
