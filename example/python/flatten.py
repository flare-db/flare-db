"""Flatten example, mirroring example/flatten (Java).

Three keyed PCollections are merged with Flatten and then grouped with
GroupByKey. This exercises the runner-native Flatten fan-in and the downstream
GroupByKey over the merged PCollection.

Run from the repository root with a FlareDB instance already started:

    python3 example/python/flatten.py

See runner-sdk/python/flarerunner for how to install the flaredb dependency,
and the repository root README.md for how to start FlareDB.
"""

import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from flaredb_runner.flare_runner import FlareRunner

LOG = logging.getLogger(__name__)

STORE_SALES = [("apple", 10), ("banana", 20)]
ONLINE_SALES = [("apple", 15), ("banana", 25)]
PARTNER_SALES = [("apple", 5), ("banana", 10)]


def log_grouped(element):
  """Logs a grouped product with all of its sale amounts."""
  product, sales = element
  LOG.info("product=%s sales=%s", product, list(sales))


def run(argv=None):
  """Builds and submits the Flatten pipeline to FlareDB."""

  pipeline_options = PipelineOptions(
      argv or [],
      job_endpoint="127.0.0.1:8099",
  )

  with beam.Pipeline(
      runner=FlareRunner(),
      options=pipeline_options,
  ) as p:
    store = p | "StoreSales" >> beam.Create(STORE_SALES)
    online = p | "OnlineSales" >> beam.Create(ONLINE_SALES)
    partner = p | "PartnerSales" >> beam.Create(PARTNER_SALES)

    (
        (store, online, partner)
        | "FlattenSales" >> beam.Flatten()
        | "GroupByProduct" >> beam.GroupByKey()
        | "Print" >> beam.Map(log_grouped)
    )


if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  run()
