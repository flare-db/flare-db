"""Dynamic-split example for a Splittable DoFn (SDF).

One input element describes a job of BLOCKS blocks (offsets 0..BLOCKS-1) and
each block takes BLOCK_SECONDS to process. The DoFn never overrides split(), so
the initial restriction stays one big range: the only way the work can be
divided is for the runner to ask the SDK to split a *running* bundle, which is
what makes this a dynamic split.

While a bundle runs, FlareDB polls the SDK for progress. The SplitManager grants
a split only when worker capacity is idle, the work queue is empty, the bundle
has enough estimated work remaining, and its cooldown/depth limits allow it.
The SDK keeps part of the remaining restriction and hands the rest back as a
residual, which the runner schedules as a new bundle.

Claim and split events are written under `build/`. After the pipeline finishes,
the example checks that every block ran exactly once and that the requested
number of dynamic splits was observed.

Run from the repository root with a FlareDB instance already started:

    FLAREDB_WORKERS=4 \
    FLAREDB_MIN_WORKERS=4 \
    FLAREDB_SDF_OVERDECOMPOSE=1 \
    FLAREDB_SDF_MIN_SPLIT_SECS=1 \
    FLAREDB_SDF_PROGRESS_POLL_SECS=0.25 \
    ./flareup-dev.sh --debug --log-level info

    python3 example/python/dynamic_split.py

Environment overrides:

    DYNAMIC_SPLIT_BLOCKS=40
    DYNAMIC_SPLIT_BLOCK_SECONDS=0.25
    DYNAMIC_SPLIT_ELEMENTS=1
    DYNAMIC_SPLIT_EXPECT_SPLITS=1

See runner-sdk/python/flarerunner for how to install the flaredb dependency,
and the repository root README.md for how to start FlareDB.
"""

import logging
import os
import time
from pathlib import Path

import apache_beam as beam
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.options.pipeline_options import PipelineOptions

from flaredb_runner.flare_runner import FlareRunner

LOG = logging.getLogger(__name__)

# Workload shape. The default creates one long SDF restriction and expects
# dynamic splitting when at least one worker remains idle. Set
# DYNAMIC_SPLIT_EXPECT_SPLITS=0 for a capacity-saturated control run.
BLOCKS = int(os.environ.get("DYNAMIC_SPLIT_BLOCKS", "24"))
BLOCK_SECONDS = float(os.environ.get("DYNAMIC_SPLIT_BLOCK_SECONDS", "0.5"))
ELEMENT_COUNT = int(os.environ.get("DYNAMIC_SPLIT_ELEMENTS", "1"))
EXPECT_DYNAMIC_SPLITS = int(os.environ.get("DYNAMIC_SPLIT_EXPECT_SPLITS", "1"))


def append_event(path, value):
  """Appends one complete event line for post-run validation."""
  directory = os.path.dirname(path)
  if directory:
    os.makedirs(directory, exist_ok=True)
  with open(path, "a", encoding="utf-8") as handle:
    handle.write(value + "\n")


def append_claim(path, name, block):
  """Appends one claimed block to `path`, creating the parent directory."""
  append_event(path, "%s:%d" % (name, block))


class BlockRangeProvider(beam.RestrictionProvider):
  """Maps one job element to the offset range of its blocks.

  split() is intentionally not overridden: the initial restriction stays a
  single range, so any split observed at runtime is a dynamic split requested
  by the runner.
  """

  def initial_restriction(self, element):
    _, num_blocks = element
    return OffsetRange(0, num_blocks)

  def create_tracker(self, restriction):
    return OffsetRestrictionTracker(restriction)

  def restriction_size(self, element, restriction):
    return restriction.size()


class SlowBlockFn(beam.DoFn):
  """Processes each block slowly, logging claims and any dynamic split."""

  def __init__(self, claims_file, segments_file, splits_file):
    self.claims_file = claims_file
    self.segments_file = segments_file
    self.splits_file = splits_file

  def process(
      self,
      element,
      tracker=beam.DoFn.RestrictionParam(BlockRangeProvider())):
    name, _ = element
    restriction = tracker.current_restriction()
    segment = "%d-%d" % (restriction.start, restriction.stop)
    append_event(self.segments_file, "%s:%d:%d" % (name, restriction.start, restriction.stop))
    LOG.info(
        "segment %s started: blocks [%d, %d)",
        segment,
        restriction.start,
        restriction.stop)

    stop = restriction.stop
    offset = restriction.start
    # Canonical SDF loop: claim an offset, then do its work. try_claim()
    # returns False once the offset falls outside the (possibly shrunk)
    # restriction.
    while tracker.try_claim(offset):
      append_claim(self.claims_file, name, offset)
      LOG.info("segment %s claimed block %d", segment, offset)

      # A dynamic split shrinks the current restriction; the tail becomes a
      # residual that the runner processes in a new bundle.
      current = tracker.current_restriction()
      if current.stop < stop:
        LOG.info(
            "segment %s split: blocks [%d, %d) handed back as a residual",
            segment,
            current.stop,
            stop)
        append_event(self.splits_file, "%s:%d:%d" % (name, current.stop, stop))
        stop = current.stop

      time.sleep(BLOCK_SECONDS)
      yield "%s:block-%d" % (name, offset)
      offset += 1

    LOG.info(
        "segment %s finished: processed blocks [%d, %d)",
        segment,
        restriction.start,
        tracker.current_restriction().stop)


def log_result(element):
  """Logs one processed block."""
  LOG.info("result=%s", element)


def read_events(path):
  """Reads non-empty event lines recorded by the pipeline."""
  if not os.path.exists(path):
    return []
  with open(path, "r", encoding="utf-8") as handle:
    return [line.strip() for line in handle if line.strip()]


def verify_claims(claims, split_events):
  """Checks exact block coverage and the configured dynamic split count."""
  expected = [
      "job-%d:%d" % (element, block)
      for element in range(ELEMENT_COUNT)
      for block in range(BLOCKS)
  ]
  ok = sorted(claims) == sorted(expected)
  LOG.info(
      "Every block processed exactly once for %d element(s), blocks 0..%d: %s",
      ELEMENT_COUNT,
      BLOCKS - 1,
      "YES" if ok else "NO  <-- BUG")
  if not ok:
    raise AssertionError(
        "dynamic-split-example FAILED: claims do not match the input.\n"
        "  expected_count=%d actual_count=%d" % (len(expected), len(claims)))

  split_ok = (
      EXPECT_DYNAMIC_SPLITS < 0
      or len(split_events) >= EXPECT_DYNAMIC_SPLITS)
  LOG.info(
      "Dynamic split validation: observed=%d expected_min=%d: %s",
      len(split_events),
      EXPECT_DYNAMIC_SPLITS,
      "YES" if split_ok else "NO  <-- BUG")
  if not split_ok:
    raise AssertionError(
        "dynamic-split-example FAILED: expected at least %d dynamic split(s), "
        "observed %d" % (EXPECT_DYNAMIC_SPLITS, len(split_events)))


def run(argv=None):
  """Builds and submits the dynamic-split pipeline to FlareDB."""

  pipeline_options = PipelineOptions(
      argv or [],
      job_endpoint="127.0.0.1:8099",
  )

  build_dir = Path("build").resolve()
  claims_file = str(build_dir / "dynamic-split-claims.txt")
  segments_file = str(build_dir / "dynamic-split-segments.txt")
  splits_file = str(build_dir / "dynamic-split-splits.txt")

  for path in (claims_file, segments_file, splits_file):
    if os.path.exists(path):
      os.remove(path)

  with beam.Pipeline(
      runner=FlareRunner(),
      options=pipeline_options,
  ) as p:
    (
        p
        | "Create" >> beam.Create(
            [("job-%d" % index, BLOCKS) for index in range(ELEMENT_COUNT)])
        | "SlowBlocks" >> beam.ParDo(
            SlowBlockFn(claims_file, segments_file, splits_file))
        | "LogResults" >> beam.Map(log_result)
    )

  claims = read_events(claims_file)
  segments = read_events(segments_file)
  split_events = read_events(splits_file)
  LOG.info(
      "Dynamic split run summary: elements=%d blocks=%d block_seconds=%s "
      "segments=%d splits=%d",
      ELEMENT_COUNT,
      BLOCKS,
      BLOCK_SECONDS,
      len(segments),
      len(split_events))
  verify_claims(claims, split_events)


if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  run()
