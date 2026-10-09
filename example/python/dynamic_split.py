"""Dynamic-split example for a Splittable DoFn (SDF).

One input element describes a job of BLOCKS blocks (offsets 0..BLOCKS-1) and
each block takes BLOCK_SECONDS to process. The DoFn never overrides split(), so
the initial restriction stays one big range: the only way the work can be
divided is for the runner to ask the SDK to split a *running* bundle, which is
what makes this a dynamic split.

While a bundle runs, FlareDB polls its progress on a short interval. When two
consecutive polls show no new input index and no new output elements, the
bundle is treated as stalled and FlareDB asks the SDK to split it. The SDK
keeps part of the remaining blocks and hands the rest back as a residual, which
the runner schedules as a new bundle. Making a block slower than the poll
interval is what causes the split.

Each claimed block is logged, so a run shows the work divided across several
bundles. After the pipeline finishes, the claimed blocks are read back and
checked against 0..BLOCKS-1 to confirm every block ran exactly once, no matter
how often the work was split.

Run from the repository root with a FlareDB instance already started:

    python3 example/python/dynamic_split_new.py

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

# Number of blocks in the single job, and how long each block takes. Keep
# BLOCK_SECONDS above the runner's progress-poll interval so the bundle looks
# stalled and is split.
BLOCKS = 24
BLOCK_SECONDS = 0.5


def append_claim(path, block):
  """Appends one claimed block to `path`, creating the parent directory."""
  directory = os.path.dirname(path)
  if directory:
    os.makedirs(directory, exist_ok=True)
  with open(path, "a", encoding="utf-8") as handle:
    handle.write("%d\n" % block)


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

  def __init__(self, claims_file):
    self.claims_file = claims_file

  def process(
      self,
      element,
      tracker=beam.DoFn.RestrictionParam(BlockRangeProvider())):
    name, _ = element
    restriction = tracker.current_restriction()
    segment = "%d-%d" % (restriction.start, restriction.stop)
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
      append_claim(self.claims_file, offset)
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


def read_claims(path):
  """Reads the claimed blocks recorded by the pipeline."""
  if not os.path.exists(path):
    return []
  with open(path, "r", encoding="utf-8") as handle:
    return [int(line) for line in handle if line.strip()]


def verify_claims(claims):
  """Checks that every block 0..BLOCKS-1 was claimed exactly once."""
  ok = sorted(claims) == list(range(BLOCKS))
  LOG.info(
      "Every block 0..%d processed exactly once: %s",
      BLOCKS - 1,
      "YES" if ok else "NO  <-- BUG")
  if not ok:
    raise AssertionError(
        "dynamic-split-example FAILED: processed blocks do not match the "
        "input.\n  expected=%s\n  actual=%s"
        % (list(range(BLOCKS)), sorted(claims)))


def run(argv=None):
  """Builds and submits the dynamic-split pipeline to FlareDB."""

  pipeline_options = PipelineOptions(
      argv or [],
      job_endpoint="127.0.0.1:8099",
  )

  claims_file = str(Path("build").resolve() / "dynamic-split-claims.txt")

  if os.path.exists(claims_file):
    os.remove(claims_file)

  with beam.Pipeline(
      runner=FlareRunner(),
      options=pipeline_options,
  ) as p:
    (
        p
        | "Create" >> beam.Create([("job", BLOCKS)])
        | "SlowBlocks" >> beam.ParDo(SlowBlockFn(claims_file))
        | "LogResults" >> beam.Map(log_result)
    )

  verify_claims(read_claims(claims_file))


if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  run()
