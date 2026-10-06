"""CoGroupByKey example, mirroring example/co-groupby-key (Java).

Two keyed PCollections are co-grouped by key, each key's values from both sides
are logged. CoGroupByKey is a composite of GroupByKey, so this exercises the
runner-native GroupByKey fan-in path (including coder handling for the tagged
inputs) through the Python SDK harness.

Run from the repository root with a FlareDB instance already started:

    Create a python virtual env:

    python3 -m venv .venv
    source .venv/bin/activate

    and install the beam and flaredb dependencies:

    pip install apache-beam
    pip install -e runner-sdk/python/flarerunner

    run the example:
    python3 example/python/co_groupby_key.py

See the repository root README.md for how to start FlareDB.
"""

import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from flaredb_runner.flare_runner import FlareRunner

LOG = logging.getLogger(__name__)

LEFT = [("a", 1), ("a", 2), ("b", 3)]
RIGHT = [("a", "x"), ("a", "y"), ("c", "z")]


def log_grouped(element):
  """Logs one co-grouped key with its values from each tagged input."""
  key, groups = element
  LOG.info(
      "%s left=%s right=%s",
      key,
      sorted(groups["left"]),
      sorted(groups["right"]))


def run(argv=None):
  """Builds and submits the CoGroupByKey pipeline to FlareDB."""

  pipeline_options = PipelineOptions(
      argv or [],
      job_endpoint="127.0.0.1:8099",
  )

  with beam.Pipeline(
      runner=FlareRunner(),
      options=pipeline_options,
  ) as p:
    left = p | "CreateLeft" >> beam.Create(LEFT)
    right = p | "CreateRight" >> beam.Create(RIGHT)

    (
        {"left": left, "right": right}
        | "CoGroupByKey" >> beam.CoGroupByKey()
        | "Print" >> beam.Map(log_grouped)
    )


if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  run()
