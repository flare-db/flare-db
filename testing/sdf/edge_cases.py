"""SDK-reachable edge cases for SDF splitting.

This pipeline exercises edge cases that a normal Python DoFn can actually
produce:

- an empty initial restriction (`[0, 0)`),
- a single-position restriction (nothing to split),
- a restriction that can split at a boundary,
- a tracker that declines every split (`try_split` returns `None`), including a
  long-running one that keeps the runner polling and asking to split,
- a slow single-position restriction.

It verifies that every element's positions are claimed exactly once and that the
job completes rather than hanging or duplicating work.

Note: malformed runner-side split responses — an empty residual element, a
regressed/out-of-range `first_residual_element`, or multiple channel splits —
cannot be produced by a valid DoFn/SDK harness. Those paths belong in Rust
mock-harness unit tests, not in a Python pipeline.

Run from the repository root with FlareDB running:

    python3 testing/sdf/edge_cases.py
"""

import logging
import time
from pathlib import Path

import apache_beam as beam
from apache_beam.io.restriction_trackers import RestrictionProgress
from apache_beam.io.restriction_trackers import RestrictionTracker

from flaredb_runner.flare_runner import FlareRunner

from _common import build_dir, pipeline_options, read_ints

LOG = logging.getLogger(__name__)

CLAIMS_DIR = build_dir() / "edge-case-claims"

# (job, blocks, refuse_split, sleep_seconds)
ELEMENTS = [
    (0, 0, False, 0.0),  # empty restriction
    (1, 1, False, 0.0),  # single position
    (2, 2, False, 0.3),  # short restriction, split at a boundary
    (3, 2, True, 0.3),  # split declined
    (4, 8, True, 0.5),  # long restriction, split declined repeatedly
    (5, 1, False, 0.5),  # slow single position
]

EXPECTED = {
    0: [],
    1: [0],
    2: [0, 1],
    3: [0, 1],
    4: list(range(8)),
    5: [0],
}


class EdgeRangeTracker(RestrictionTracker):
    """Offset-style tracker whose restriction can decline splits."""

    def __init__(self, restriction):
        self._start, self._stop, self._refuse_split = restriction
        self._current = self._start

    def current_restriction(self):
        return (self._current, self._stop, self._refuse_split)

    def try_claim(self, position):
        if self._current <= position < self._stop:
            self._current = position + 1
            return True
        return False

    def try_split(self, fraction_of_remainder):
        if self._refuse_split:
            return None
        remaining = self._stop - self._current
        if remaining <= 0:
            return None
        split = self._current + int(remaining * fraction_of_remainder)
        if split <= self._current or split >= self._stop:
            return None
        residual_stop = self._stop
        # The SDK expects the tracker to follow the primary restriction after a
        # split; the residual becomes a new tracker starting at `split`.
        self._stop = split
        return (
            (self._current, split, self._refuse_split),
            (split, residual_stop, self._refuse_split),
        )

    def check_done(self):
        return self._current >= self._stop

    def current_progress(self):
        # Sized restrictions are asked for their progress on every
        # `ProcessBundleProgress`; the base class raises `NotImplementedError`,
        # which makes the runner disable splitting for the bundle. Mirror
        # `OffsetRestrictionTracker.current_progress` so the split path is
        # actually exercised.
        if self._stop <= self._start:
            fraction = 1.0  # empty restriction: already done
        elif self._current <= self._start:
            fraction = 0.0
        else:
            fraction = (self._current - self._start) / (self._stop - self._start)
        return RestrictionProgress(fraction=min(1.0, fraction))

    def is_bounded(self):
        return True


class EdgeRangeProvider(beam.RestrictionProvider):
    def initial_restriction(self, element):
        _, blocks, refuse_split, _ = element
        return (0, blocks, refuse_split)

    def create_tracker(self, restriction):
        return EdgeRangeTracker(restriction)

    def restriction_size(self, element, restriction):
        start, stop, _ = restriction
        return max(0, stop - start)

    def split(self, element, restriction):
        return [restriction]


class EdgeBlocks(beam.DoFn):
    def __init__(self, claims_dir):
        self.claims_dir = claims_dir

    def process(
        self, element, tracker=beam.DoFn.RestrictionParam(EdgeRangeProvider())
    ):
        job, _, _, sleep_seconds = element
        path = Path(self.claims_dir) / f"job-{job}.txt"
        # This tracker's restriction is a plain ``(start, stop, refuse_split)``
        # tuple, not an ``OffsetRange``.
        restriction = tracker.current_restriction()
        stop = restriction[1]
        LOG.info(
            "job %s segment [%d, %d) started", job, restriction[0], stop
        )

        offset = restriction[0]
        while tracker.try_claim(offset):
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(f"{offset}\n")

            current = tracker.current_restriction()
            if current[1] < stop:
                LOG.info(
                    "job %s split: [%d, %d) handed back",
                    job,
                    current[1],
                    stop,
                )
                stop = current[1]

            if sleep_seconds:
                time.sleep(sleep_seconds)
            yield job, offset
            offset += 1


def verify():
    problems = []
    for job, expected in EXPECTED.items():
        path = Path(CLAIMS_DIR) / f"job-{job}.txt"
        actual = read_ints(path)
        if sorted(actual) != expected:
            problems.append(f"job {job}: expected {expected}, got {sorted(actual)}")

    if problems:
        raise AssertionError("edge-cases FAILED:\n" + "\n".join(problems))

    LOG.info("edge-cases: all restrictions claimed exactly once: YES")


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
            | "Create edge cases" >> beam.Create(ELEMENTS, reshuffle=False)
            | "Process edge cases" >> beam.ParDo(EdgeBlocks(str(CLAIMS_DIR)))
            | "Log results" >> beam.Map(lambda kv: LOG.info("result=%s", kv))
        )

    verify()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
