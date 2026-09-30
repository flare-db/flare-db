"""Scores filter example pipeline for FlareDB.

Reads ``test-data/scores.csv``, parses each row, keeps the rows whose score
meets a threshold, and logs the matching rows.

Python port of the CSV parsing in
example/flare-io-write/src/main/java/com/flaredb/example/flareio/WritePipeline.java:
it skips the header, blank lines, and malformed rows, and parses the ``id``,
``age``, and ``score`` columns as integers.

Run from the repository root with a FlareDB instance already started:

    pip install -e runner-sdk/python/flarerunner
    python example/python/scores.py

See the repository root README.md for how to start FlareDB.
"""

import csv
import logging
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from flaredb_runner.flare_runner import FlareRunner

LOG = logging.getLogger(__name__)

# test-data/scores.csv lives at the repository root; this file is at
# example/python/scores.py, so climb two directories to reach it.
REPO_ROOT = Path(__file__).resolve().parents[2]
INPUT_CSV = REPO_ROOT / "test-data" / "scores.csv"

# Only rows scoring at or above this are considered passing.
PASS_THRESHOLD = 60

# Number of columns in scores.csv: id,name,city,age,score.
EXPECTED_FIELDS = 5


def is_data_line(line):
  """True for a non-blank line that is not the CSV header row."""
  return bool(line.strip()) and not line.startswith("id,")


def parse_score(line):
  """Parses one CSV line into ``(id, name, city, age, score)``.

  Returns ``None`` for malformed rows so the pipeline can drop them with a
  ``Filter``, matching the warning-and-skip behaviour of the Java example.
  """
  fields = next(csv.reader([line]))
  if len(fields) != EXPECTED_FIELDS:
    LOG.warning("Skipping malformed line (expected %d fields, got %d): %s",
                EXPECTED_FIELDS, len(fields), line)
    return None
  try:
    return (int(fields[0]), fields[1], fields[2], int(fields[3]), int(fields[4]))
  except ValueError:
    LOG.warning("Skipping line with a non-numeric field: %s", line)
    return None


def is_passing(row):
  """True when the row's score meets the threshold."""
  return row[4] >= PASS_THRESHOLD


class LogResult(beam.DoFn):
  """Logs each formatted passing score."""

  def process(self, element):
    LOG.info("Element: %s", element)


def run(argv=None):
  """Builds and submits the scores filter pipeline to FlareDB."""

  pipeline_options = PipelineOptions(
      argv or [],
      job_endpoint="127.0.0.1:8099",
  )

  with beam.Pipeline(
      runner=FlareRunner(),
      options=pipeline_options,
  ) as p:
    (
        p
        | "Read scores CSV" >> beam.io.ReadFromText(str(INPUT_CSV))
        | "Skip header and blank lines" >> beam.Filter(is_data_line)
        | "Parse rows" >> beam.Map(parse_score)
        | "Drop malformed rows" >> beam.Filter(lambda row: row is not None)
        | "Keep passing scores" >> beam.Filter(is_passing)
        | "Format results"
        >> beam.Map(lambda row: f"{row[1]} ({row[2]}): {row[4]}")
        | "Log results" >> beam.ParDo(LogResult())
    )


if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  run()
