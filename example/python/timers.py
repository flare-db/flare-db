"""Timers example, mirroring example/timers-example (Java).

Counts elements per key with bag state and timers: a processing-time timer
flushes each key's count, and an event-time timer fires twice. The results are
checked against the input.
"""

import logging
import os
import time
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer
from apache_beam.utils.timestamp import Timestamp

from flaredb_runner.flare_runner import FlareRunner

LOG = logging.getLogger(__name__)

# Seconds after the last element for a key before the processing-time timer
# fires (mirrors the Java example's `offset(2s)`).
FLUSH_SECONDS = 2


def append_line(path, line):
  """Appends a line to `path`, creating the parent directory if needed."""
  directory = os.path.dirname(path)
  if directory:
    os.makedirs(directory, exist_ok=True)
  with open(path, "a", encoding="utf-8") as handle:
    handle.write(line + "\n")


class ParseName(beam.DoFn):
  """Parses a scores.csv line into a (name, name) keyed record."""

  def process(self, line):
    line = line.strip()

    # Skip the header and blank lines.
    if not line or line.startswith("id,"):
      return

    fields = line.split(",")
    if len(fields) < 2:
      return

    # Key by name so state and the timers are per-name.
    yield (fields[1], fields[1])


class BufferAndFireOnTimer(beam.DoFn):
  """Terminal stateful stage with a processing-time and an event-time timer."""

  BUFFER = BagStateSpec("buffer", beam.coders.StrUtf8Coder())
  EVENT_ARMED = BagStateSpec("eventArmed", beam.coders.StrUtf8Coder())
  FLUSH = TimerSpec("flush", TimeDomain.REAL_TIME)
  EVENT_FLUSH = TimerSpec("eventFlush", TimeDomain.WATERMARK)

  def __init__(self, output_file, event_file):
    self.output_file = output_file
    self.event_file = event_file

  def process(self,
              element,
              buffer=beam.DoFn.StateParam(BUFFER),
              flush=beam.DoFn.TimerParam(FLUSH),
              event_flush=beam.DoFn.TimerParam(EVENT_FLUSH)):
    key, value = element
    buffer.add(value)

    # Re-arm the processing-time timer from now; the last element for a key
    # wins, so the flush happens once the key goes quiet. Python's
    # processing-time domain is REAL_TIME, and its timer set is absolute, so
    # compute now + FLUSH_SECONDS.
    flush.set(Timestamp(time.time() + FLUSH_SECONDS))

    # Arm an event-time timer at the epoch. With a bounded source the input
    # watermark only reaches it when the source reports +inf, so this fires at
    # the bounded end rather than on the wall clock.
    event_flush.set(Timestamp(0))

    LOG.info("buffered key=%s value=%s", key, value)

  @on_timer(FLUSH)
  def on_flush(self,
               key=beam.DoFn.KeyParam,
               buffer=beam.DoFn.StateParam(BUFFER)):
    count = len(list(buffer.read()))
    LOG.info(
        "processing-time timer fired: key=%s bufferedElements=%s", key, count)

    append_line(self.output_file, "%s,%s" % (key, count))
    buffer.clear()

  @on_timer(EVENT_FLUSH)
  def on_event_flush(self,
                     key=beam.DoFn.KeyParam,
                     event_armed=beam.DoFn.StateParam(EVENT_ARMED),
                     event_flush=beam.DoFn.TimerParam(EVENT_FLUSH)):
    LOG.info("event-time timer fired: key=%s", key)
    append_line(self.event_file, "%s,event" % key)

    # Re-arm exactly once. The watermark is already +inf here, so the re-armed
    # timer is immediately due again and must be delivered (drained), not
    # stranded.
    if not list(event_armed.read()):
      event_armed.add("rearmed")
      event_flush.set(Timestamp(0))


def read_lines(path):
  """Reads all lines of `path`, returning an empty list when it is absent."""
  if not os.path.exists(path):
    return []
  with open(path, "r", encoding="utf-8") as handle:
    return [line.rstrip("\n") for line in handle]


def expected_counts(input_file):
  """Per-key element counts read directly from the input CSV."""
  counts = {}
  for line in read_lines(input_file):
    if not line or line.startswith("id,"):
      continue
    fields = line.split(",")
    if len(fields) < 2:
      continue
    counts[fields[1]] = counts.get(fields[1], 0) + 1
  return counts


def read_flush_results(output_file):
  """Per-key counts recorded by the pipeline.

  A key appearing more than once means the timer fired twice, which is a bug
  even when the final count happens to match.
  """
  counts = {}
  for line in read_lines(output_file):
    if not line.strip():
      continue
    key, _, count = line.rpartition(",")
    if not key:
      continue
    if key in counts:
      raise AssertionError(
          "timers-example FAILED: key '%s' fired more than once" % key)
    counts[key] = int(count)
  return counts


def count_event_firings(event_file):
  """Per-key number of event-time firings recorded by the pipeline."""
  firings = {}
  for line in read_lines(event_file):
    if not line.strip():
      continue
    key, _, _ = line.rpartition(",")
    if not key:
      continue
    firings[key] = firings.get(key, 0) + 1
  return firings


def verify(input_file, output_file, event_file):
  """Compares recorded results against the input, failing on any mismatch."""
  expected = expected_counts(input_file)
  actual = read_flush_results(output_file)
  expected_total = sum(expected.values())
  actual_total = sum(actual.values())

  LOG.info(
      "timers-example: expected keys=%d total=%d; flushed keys=%d total=%d",
      len(expected), expected_total, len(actual), actual_total)

  if expected != actual:
    raise AssertionError(
        "timers-example FAILED: flushed per-key counts do not match the input."
        "\n  expected=%s\n  actual=%s" % (expected, actual))

  # The event-time timer fires at the bounded end (+inf) and re-arms once, so
  # every key must be recorded exactly twice: the drain after +inf is what
  # proves a re-armed event-time timer is not stranded.
  event_firings = count_event_firings(event_file)
  for key in expected:
    firings = event_firings.get(key, 0)
    if firings != 2:
      raise AssertionError(
          "timers-example FAILED: event-time timer for key '%s' fired %d "
          "time(s), expected 2 (one plus one re-arm after +inf). firings=%s"
          % (key, firings, event_firings))

  if set(event_firings) != set(expected):
    raise AssertionError(
        "timers-example FAILED: event-time keys do not match the input. "
        "event=%s expected=%s" % (set(event_firings), set(expected)))

  LOG.info(
      "timers-example PASSED: %d keys each fired once (processing time, %d "
      "elements) and %d keys each fired twice (event time)",
      len(actual), actual_total, len(event_firings))


def run(argv=None):
  """Builds and submits the timers pipeline, then verifies the results."""

  pipeline_options = PipelineOptions(
      argv or [],
      job_endpoint="127.0.0.1:8099",
  )
  input_file = str(
      Path(__file__).resolve().parents[2] / "test-data" / "scores.csv")

  output_file = str(Path("build").resolve() / "timers-example-flush.txt")
  event_file = output_file + ".event"

  # Start from a clean slate so a stale file cannot make a broken run look OK.
  for path in (output_file, event_file):
    if os.path.exists(path):
      os.remove(path)
    directory = os.path.dirname(path)
    if directory:
      os.makedirs(directory, exist_ok=True)

  with beam.Pipeline(
      runner=FlareRunner(),
      options=pipeline_options,
  ) as p:
    (
        p
        | "ReadCSV" >> beam.Create(read_lines(input_file))
        | "ParseCSV" >> beam.ParDo(ParseName())
        # Terminal stateful stage with a processing-time timer and an
        # event-time timer.
        | "BufferAndFireOnTimer"
        >> beam.ParDo(BufferAndFireOnTimer(output_file, event_file))
    )

  # FlareRunner.run blocks until the job completes, so the output files are
  # fully written when this returns.
  verify(input_file, output_file, event_file)


if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  run()
