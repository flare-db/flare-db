"""Fixed-window example, mirroring example/fixed-window (Java).

Each row of test-data/scores.csv is parsed into a keyed record with a
deterministic event timestamp (id - 1 seconds), windowed into 60-second fixed
windows, and grouped with GroupByKey. Every key's values within a window are
logged together with the element timestamp and window, so the windowing
boundaries can be compared against the Java example.

Run from the repository root with a FlareDB instance already started:

    python3 example/python/fixed_window.py

See runner-sdk/python/flarerunner for how to install the flaredb dependency,
and the repository root README.md for how to start FlareDB.
"""

import logging
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils.timestamp import Timestamp

from flaredb_runner.flare_runner import FlareRunner

LOG = logging.getLogger(__name__)

WINDOW_SIZE_SECONDS = 60


class ParseScoresCsv(beam.DoFn):
  """Parses a scores.csv line into a keyed record with an event timestamp."""

  def process(self, line):
    line = line.strip()

    # Skip the header and blank lines.
    if not line or line.startswith("id,"):
      return

    fields = line.split(",")
    if len(fields) < 5:
      return

    try:
      record_id = int(fields[0])
      score = int(fields[4])
    except ValueError:
      return

    # Deterministic event timestamp, as in the Java example.
    yield beam.window.TimestampedValue(
        (fields[1], score), Timestamp(record_id - 1))


class PrintGroupedResults(beam.DoFn):
  """Logs a grouped (key, values) element with its timestamp and window."""

  def process(self,
              element,
              timestamp=beam.DoFn.TimestampParam,
              window=beam.DoFn.WindowParam):
    key, values = element
    LOG.info(
        "key=%s values=%s timestamp=%s window=%s",
        key,
        list(values),
        timestamp,
        window)


def read_lines(path):
  """Reads the input file on the driver; the lines are fed to beam.Create."""
  with open(path, "r", encoding="utf-8") as handle:
    return [line.rstrip("\n") for line in handle]


def run(argv=None):
  """Builds and submits the fixed-window pipeline to FlareDB."""

  pipeline_options = PipelineOptions(
      argv or [],
      job_endpoint="127.0.0.1:8099",
  )
  input_file = str(
      Path(__file__).resolve().parents[2] / "test-data" / "scores.csv")

  with beam.Pipeline(
      runner=FlareRunner(),
      options=pipeline_options,
  ) as p:
    (
        p
        | "ReadCSV" >> beam.Create(read_lines(input_file))
        | "ParseCSV" >> beam.ParDo(ParseScoresCsv())
        | "FixedWindow"
        >> beam.WindowInto(beam.window.FixedWindows(WINDOW_SIZE_SECONDS))
        | "GroupByKey" >> beam.GroupByKey()
        | "PrintGroupedResults" >> beam.ParDo(PrintGroupedResults())
    )


if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  run()
