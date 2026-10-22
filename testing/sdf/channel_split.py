"""Whole-element channel-split test for Splittable DoFns.

`example/python/dynamic_split.py` has a single input element, so every split
produces only intra-element `residual_roots`. This pipeline submits several
independent job elements in one bundle: when FlareDB splits the running bundle,
the SDK can hand back the whole unstarted elements as channel splits
(`first_residual_element > 0`), which the runner re-feeds as new bundles.

It verifies that every job's blocks are claimed exactly once, regardless of how
the runner divided the work.

Run from the repository root with FlareDB running:

    python3 testing/sdf/channel_split.py
"""

import logging
import time
from pathlib import Path

import apache_beam as beam
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker

from flaredb_runner.flare_runner import FlareRunner

from _common import build_dir, pipeline_options, read_ints

LOG = logging.getLogger(__name__)

JOBS = 4
BLOCKS = 10
BLOCK_SECONDS = 0.4
CLAIMS_DIR = build_dir() / "channel-split-claims"


class BlockRangeProvider(beam.RestrictionProvider):
    """One offset range per job element; `split()` is intentionally not
    overridden so the initial restriction stays a single range and any split
    observed at runtime is runner-initiated.
    """

    def initial_restriction(self, element):
        _, blocks = element
        return OffsetRange(0, blocks)

    def create_tracker(self, restriction):
        return OffsetRestrictionTracker(restriction)

    def restriction_size(self, element, restriction):
        return restriction.size()


class SlowBlocks(beam.DoFn):
    """Claims each block slowly, recording every claim per job."""

    def __init__(self, claims_dir):
        self.claims_dir = claims_dir

    def process(
        self, element, tracker=beam.DoFn.RestrictionParam(BlockRangeProvider())
    ):
        job, _ = element
        path = Path(self.claims_dir) / f"job-{job}.txt"
        restriction = tracker.current_restriction()
        stop = restriction.stop
        LOG.info(
            "job %s segment [%d, %d) started", job, restriction.start, stop
        )

        offset = restriction.start
        while tracker.try_claim(offset):
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(f"{offset}\n")

            current = tracker.current_restriction()
            if current.stop < stop:
                LOG.info(
                    "job %s split: blocks [%d, %d) handed back",
                    job,
                    current.stop,
                    stop,
                )
                stop = current.stop

            time.sleep(BLOCK_SECONDS)
            yield job, offset
            offset += 1

        LOG.info(
            "job %s segment [%d, %d) finished",
            job,
            restriction.start,
            tracker.current_restriction().stop,
        )


def verify():
    problems = []
    for job in range(JOBS):
        path = Path(CLAIMS_DIR) / f"job-{job}.txt"
        actual = read_ints(path)
        expected = list(range(BLOCKS))
        if sorted(actual) != expected:
            problems.append(f"job {job}: expected {expected}, got {sorted(actual)}")

    if problems:
        raise AssertionError("channel-split FAILED:\n" + "\n".join(problems))

    LOG.info(
        "channel-split: every job claimed blocks 0..%d exactly once: YES",
        BLOCKS - 1,
    )


def run(argv=None):
    if CLAIMS_DIR.exists():
        for path in CLAIMS_DIR.glob("*.txt"):
            path.unlink()
    CLAIMS_DIR.mkdir(parents=True, exist_ok=True)

    with beam.Pipeline(
        runner=FlareRunner(), options=pipeline_options(argv)
    ) as pipeline:
        (
            pipeline
            | "Create jobs"
            >> beam.Create(
                [(job, BLOCKS) for job in range(JOBS)], reshuffle=False
            )
            | "Process blocks" >> beam.ParDo(SlowBlocks(str(CLAIMS_DIR)))
            | "Log results" >> beam.Map(lambda kv: LOG.info("result=%s", kv))
        )

    verify()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
