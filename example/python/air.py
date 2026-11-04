import argparse
import csv
import io
import math
from datetime import datetime
from zoneinfo import ZoneInfo

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import Timestamp

from flaredb_runner.flare_runner import FlareRunner


IST = ZoneInfo("Asia/Kolkata")

HEADER = (
    "station_id,parameter_name,unit,window_start,"
    "measurement_count,average,minimum,maximum,sources"
)


def parse_measurement(line):
    """Return a keyed measurement and its event timestamp."""
    try:
        row = next(csv.reader([line]))

        if len(row) != 6:
            return None

        station_id, parameter_name, unit, collected_at, value, source = row

        dt = datetime.strptime(
            collected_at.strip(), "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=IST)

        value = float(value)

        if not math.isfinite(value):
            return None

        key = (
            station_id.strip(),
            parameter_name.strip(),
            unit.strip(),
        )

        timestamp = Timestamp.of(dt.timestamp())

        return (
            key,
            (value, source.strip()),
            timestamp,
        )

    except (ValueError, TypeError, StopIteration):
        return None


def assign_event_timestamp(record):
    """Restore the key/value pair and assign its event timestamp."""
    key, value, timestamp = record

    return beam.window.TimestampedValue(
        (key, value),
        timestamp,
    )


class HourlyStatsCombineFn(beam.CombineFn):
    """Combine each group into compact statistics before the final GBK."""

    def create_accumulator(self):
        return 0, 0.0, float("inf"), float("-inf"), frozenset()

    def add_input(self, accumulator, measurement):
        count, total, minimum, maximum, sources = accumulator
        value, source = measurement
        return (
            count + 1,
            total + value,
            min(minimum, value),
            max(maximum, value),
            sources | {source},
        )

    def merge_accumulators(self, accumulators):
        count = 0
        total = 0.0
        minimum = float("inf")
        maximum = float("-inf")
        sources = set()
        for other_count, other_total, other_min, other_max, other_sources in accumulators:
            count += other_count
            total += other_total
            minimum = min(minimum, other_min)
            maximum = max(maximum, other_max)
            sources.update(other_sources)
        return count, total, minimum, maximum, frozenset(sources)

    def extract_output(self, accumulator):
        return accumulator


def format_hourly_result(element, window=beam.DoFn.WindowParam):
    """Format already-combined statistics for a station and pollutant."""
    (station_id, parameter_name, unit), stats = element
    count, total, minimum, maximum, sources = stats

    if count == 0:
        return None

    window_start = datetime.fromtimestamp(
        float(window.start), tz=IST
    ).strftime("%Y-%m-%d %H:%M:%S")

    row = [
        station_id,
        parameter_name,
        unit,
        window_start,
        count,
        f"{total / count:.4f}",
        f"{minimum:.4f}",
        f"{maximum:.4f}",
        ",".join(sorted(sources)),
    ]

    output = io.StringIO()
    csv.writer(output).writerow(row)
    return output.getvalue().rstrip("\r\n")


def run(argv=None):
    parser = argparse.ArgumentParser(
        description="Hourly air-quality analytics on FlareDB"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the input CSV",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output file prefix",
    )
    parser.add_argument(
        "--window_seconds",
        type=int,
        default=3600,
        help="Window duration in seconds (default: 3600)",
    )

    args, beam_args = parser.parse_known_args(argv)

    if args.window_seconds <= 0:
        parser.error("--window_seconds must be positive")

    options = PipelineOptions(
        beam_args,
        job_endpoint="127.0.0.1:8099",
        job_name="air-quality-analytics",
    )

    with beam.Pipeline(
        runner=FlareRunner(),
        options=options,
    ) as pipeline:
        lines = (
            pipeline
            | "Read CSV" >> beam.io.ReadFromText(
                args.input,
                skip_header_lines=1,
            )
        )

        measurements = (
            lines
            | "Parse measurements" >> beam.Map(parse_measurement)
            | "Drop invalid records"
            >> beam.Filter(lambda record: record is not None)
            | "Assign event timestamps"
            >> beam.Map(assign_event_timestamp)
        )

        hourly_groups = (
            measurements
            | "Window measurements"
            >> beam.WindowInto(FixedWindows(args.window_seconds))
            | "Combine hourly statistics"
            >> beam.CombinePerKey(HourlyStatsCombineFn())
        )

        hourly_results = (
            hourly_groups
            | "Calculate hourly statistics"
            >> beam.Map(format_hourly_result)
            | "Drop empty results"
            >> beam.Filter(lambda row: row is not None)
        )

        (
            hourly_results
            | "Write hourly CSV"
            >> beam.io.WriteToText(
                args.output,
                file_name_suffix=".csv",
                header=HEADER,
            )
        )


if __name__ == "__main__":
    run()
