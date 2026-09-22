"""WordCount example pipeline for FlareDB.

Python port of the Java reference at
example/wordcount/src/main/java/com/flaredb/example/WordCount.java.

The pipeline creates input text, splits them into words, removes
empty words, counts occurrences, formats the results, and logs each result.

Run from the repository root with a FlareDB instance already started:

    pip install -e runner-sdk/python/flarerunner
    python examples/python/wordcount/wordcount.py

See the repository root README.md for how to start FlareDB.
"""

import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from flaredb_runner.flare_runner import FlareRunner

LOG = logging.getLogger(__name__)


INPUT_LINES = [
    "Wisdom is a weapon that guards against all woes a fort no foe can break",
    "Wisdom checks the straying senses expels evil and impels goodness",
    "To grasp the truth from everywhere from everyone is wisdom fair",
    "Speaking thoughts in clarity and reading subtle sense in others is wisdom",
    "The wise befriend the world they bloom nor gloom equal in mind",
    "As moves the world so move the wise in tune with changing times and ways",
    "The wise foresee what is to come the unwise lack in that wisdom",
    "Fear the frightful and act wisely not to fear the frightful is folly",
    "No frightful evil shocks the wise Who guard themselves against surprise",
    "Who have wisdom they are all full Whatevr they own, misfits are nil",
]


class LogResult(beam.DoFn):
  """Logs each formatted word count."""

  def process(self, element):
    LOG.info("Element: %s", element)


def run(argv=None):
  """Builds and submits the WordCount pipeline to FlareDB."""

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
        | "Create" >> beam.Create(INPUT_LINES)
        | "Split lines into words" >> beam.FlatMap(str.split)
        | "Remove empty words" >> beam.Filter(bool)
        | "Count occurrences" >> beam.combiners.Count.PerElement()
        | "Convert counts to strings"
        >> beam.MapTuple(lambda word, count: f"{word}: {count}")
        | "Log results" >> beam.ParDo(LogResult())
    )


if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  run()
