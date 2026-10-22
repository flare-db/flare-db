"""Watermark-hold test for SDF channel residuals feeding a windowed aggregation.

The SDF emits one value per input element, but only from the bundle that owns
the tail of that element's original restriction, so a split's primary and its
residuals together emit exactly once. Several slow elements are submitted in one
bundle, forcing the runner to split and hand whole unstarted elements back as
channel residuals.

Those residuals must hold the SDF stage's output watermark. If they do not, the
downstream fixed-window `Count.PerKey` can fire before all residual work
completes and report a partial count. All input timestamps fall in a single
fixed window, so the expected count is the number of elements.

This pipeline exercises the runner-derived hold for whole-element channel
residuals (`channel_item_holds`). Beam 2.76's Python SDK has no
output-watermark hook on the restriction tracker, so intra-element
residual-root holds (`residual_item_holds`) are not exercised here.

Run from the repository root with FlareDB running:

    python3 testing/sdf/watermark_holds.py
"""

import logging
import time
from pathlib import Path

import apache_beam as beam
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import TimestampedValue

from flaredb_runner.flare_runner import FlareRunner

from _common import build_dir, pipeline_options, reset_file

LOG = logging.getLogger(__name__)

KEY = "city"
ELEMENTS = 4
BLOCKS = 12
BLOCK_SECONDS = 0.5
WINDOW_SECONDS = 3600
BASE_TIMESTAMP_SECONDS = 1000  # all elements land in window [0, 3600)
OUTPUT = build_dir() / "watermark-holds-counts.txt"

# Distinct scores make a partial `Mean.PerKey` obvious if this is later
# extended to verify values instead of just counts.
SCORES = [1, 2, 4, 8]


class BlockRangeProvider(beam.RestrictionProvider):
    """One offset range per element; runtime splits stay runner-driven."""

    def initial_restriction(self, element):
        _, blocks, _, _ = element
        return OffsetRange(0, blocks)

    def create_tracker(self, restriction):
        return OffsetRestrictionTracker(restriction)

    def restriction_size(self, element, restriction):
        return restriction.size()


class SlowScore(beam.DoFn):
    """Claims blocks slowly and emits one `(key, score)` per input element."""

    def process(
        self,
        element,
        timestamp=beam.DoFn.TimestampParam,
        tracker=beam.DoFn.RestrictionParam(BlockRangeProvider()),
    ):
        key, blocks, score, _ = element
        restriction = tracker.current_restriction()
        stop = restriction.stop
        LOG.info(
            "element %s timestamp=%s segment [%d, %d) started",
            key,
            timestamp,
            restriction.start,
            stop,
        )

        offset = restriction.start
        while tracker.try_claim(offset):
            current = tracker.current_restriction()
            if current.stop < stop:
                LOG.info(
                    "element %s split: blocks [%d, %d) handed back",
                    key,
                    current.stop,
                    stop,
                )
                stop = current.stop

            time.sleep(BLOCK_SECONDS)
            offset += 1

        # Only the bundle owning the tail of the original restriction emits.
        if tracker.current_restriction().stop == blocks:
            LOG.info("element %s emitting score at timestamp=%s", key, timestamp)
            yield key, score


class RecordCounts(beam.DoFn):
    def __init__(self, path):
        self.path = path

    def process(self, element, window=beam.DoFn.WindowParam):
        key, count = element
        with open(self.path, "a", encoding="utf-8") as handle:
            handle.write(f"{key}:{count}\n")
        LOG.info("window %s: %s count=%d", window, key, count)


def verify():
    path = Path(OUTPUT)
    if not path.exists():
        raise AssertionError("watermark-holds FAILED: no windowed output produced")

    counts = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        key, value = line.rsplit(":", 1)
        if key == KEY:
            counts.append(int(value))

    if not counts:
        raise AssertionError(f"watermark-holds FAILED: no counts for key {KEY!r}")

    if max(counts) != ELEMENTS:
        raise AssertionError(
            "watermark-holds FAILED: the window fired before all residual work "
            f"completed (expected count {ELEMENTS}, saw {counts})."
        )

    LOG.info(
        "watermark-holds: key %s count=%d over %d pane(s): YES",
        KEY,
        max(counts),
        len(counts),
    )


def run(argv=None):
    reset_file(OUTPUT)

    elements = [
        (KEY, BLOCKS, score, BASE_TIMESTAMP_SECONDS + index)
        for index, score in enumerate(SCORES)
    ]

    with beam.Pipeline(
        runner=FlareRunner(), options=pipeline_options(argv)
    ) as pipeline:
        (
            pipeline
            | "Create scores" >> beam.Create(elements, reshuffle=False)
            | "Assign event time"
            >> beam.Map(lambda e: TimestampedValue(e, e[3]))
            | "Slow SDF" >> beam.ParDo(SlowScore())
            | "One fixed window" >> beam.WindowInto(FixedWindows(WINDOW_SECONDS))
            | "Count per key" >> beam.combiners.Count.PerKey()
            | "Record counts" >> beam.ParDo(RecordCounts(str(OUTPUT)))
        )

    verify()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
