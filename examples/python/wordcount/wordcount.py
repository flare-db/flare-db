"""WordCount example pipeline for FlareDB.

Python port of the Java reference at
example/wordcount/src/main/java/com/flaredb/example/WordCount.java: the same
five transforms (read, split, filter, count, format) followed by logging
each result, submitted to a FlareDB Job Service via FlareRunner.

Run (from the repository root, with a FlareDB instance already started via
``flare up``):

    pip install -e runner-sdk/python/flarerunner
    python examples/python/wordcount/wordcount.py

See runner-sdk/python/flarerunner/README.md for how to start FlareDB.
"""

import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

from flaredb_runner.flare_runner import FlareRunner

LOG = logging.getLogger(__name__)


class LogResult(beam.DoFn):
  """Logs each formatted count, mirroring the Java example's LOG.info ParDo."""

  def process(self, element):
    LOG.info('Element: %s', element)


def run(argv=None):
  """Builds and submits the WordCount pipeline to a FlareDB Job Service."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      default='test-data/thirukkural.txt',
      help='Path to the input text file to count words from.')
  parser.add_argument(
      '--job_endpoint',
      default='127.0.0.1:8099',
      help='Address of the FlareDB Job Service to submit the pipeline to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args, job_endpoint=known_args.job_endpoint)

  with beam.Pipeline(runner=FlareRunner(), options=pipeline_options) as p:
    (
        p
        | 'ReadLines' >> ReadFromText(known_args.input)
        | 'Split lines into words' >> beam.FlatMap(lambda line: line.split(' '))
        | 'Remove empty words' >> beam.Filter(lambda word: word != '')
        | 'Count occurrences' >> beam.combiners.Count.PerElement()
        | 'Convert counts to strings' >> beam.MapTuple(lambda word, count: f'{word}: {count}')
        | 'Log results' >> beam.ParDo(LogResult())
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
