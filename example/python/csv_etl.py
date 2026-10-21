"""CSV ETL example pipeline for FlareDB.

This example reads local scores data, removes invalid records, calculates the
average score for each city, and logs the transformed results.
"""

import csv
import logging
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import PortableOptions

from flaredb_runner.flare_runner import FlareRunner

LOG = logging.getLogger(__name__)

INPUT_FILE = Path("test-data") / "scores2.csv"


def parse_valid_score_records(line):
    """Parse a CSV record and emit valid (city, score) pairs."""
    row = next(csv.reader([line]))

    try:
        city = row[2].strip()
        age = int(row[3])
        score = float(row[4])
    except (IndexError, ValueError):
        return

    if city and 0 <= age <= 120 and 0 <= score <= 100:
        yield city, score


def format_city_average(city_and_average):
    """Format one calculated city average for logging."""
    city, average_score = city_and_average
    return f"{city}: average score = {average_score:.2f}"


class LogResult(beam.DoFn):
    """Log each final ETL result."""

    def process(self, element):
        LOG.info("ETL result: %s", element)


def run(argv=None):
    """Build and submit the CSV ETL pipeline to FlareDB."""
    pipeline_options = PipelineOptions(argv or [])
    portable_options = pipeline_options.view_as(PortableOptions)
    portable_options.job_endpoint = "127.0.0.1:8099"

    with beam.Pipeline(
        runner=FlareRunner(),
        options=pipeline_options,
    ) as pipeline:
        (
            pipeline
            | "Read local scores CSV" >> beam.io.ReadFromText(str(INPUT_FILE))
            | "Skip header row" >> beam.Filter(lambda line: not line.startswith("id,"))
            | "Parse and validate rows" >> beam.FlatMap(parse_valid_score_records)
            | "Calculate average score per city" >> beam.combiners.Mean.PerKey()
            | "Format city averages" >> beam.Map(format_city_average)
            | "Log ETL results" >> beam.ParDo(LogResult())
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
