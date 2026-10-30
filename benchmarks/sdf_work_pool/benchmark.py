"""Fixed-workload benchmark for FlareDB's SDF worker pool.

The serial pipeline and the dynamically split pipeline perform the same number
of fixed-duration blocks. The script verifies exactly-once block claims and
reports median elapsed time and observed concurrency.
"""

import json
import statistics
import time
from pathlib import Path

import apache_beam as beam
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.options.pipeline_options import PipelineOptions

from flaredb_runner.flare_runner import FlareRunner


BLOCKS = 24
BLOCK_SECONDS = 0.25
REPETITIONS = 2
JOB_ENDPOINT = "127.0.0.1:8099"
BUILD_DIR = Path("build")


def append_json_line(path, value):
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(value) + "\n")


def append_claim(path, block):
    with open(path, "a", encoding="utf-8") as handle:
        handle.write("%d\n" % block)


def timed_sleep(path, block):
    started = time.monotonic()
    time.sleep(BLOCK_SECONDS)
    finished = time.monotonic()
    append_json_line(path, {"start": started, "finish": finished})
    append_claim(claims_path_for(path), block)


def claims_path_for(timeline_path):
    return str(timeline_path) + ".claims"


class SlowMap(beam.DoFn):
    def __init__(self, timeline_path):
        self.timeline_path = timeline_path

    def process(self, block):
        timed_sleep(self.timeline_path, block)
        yield block


class BlockRangeProvider(beam.RestrictionProvider):
    def initial_restriction(self, element):
        return OffsetRange(0, element)

    def create_tracker(self, restriction):
        return OffsetRestrictionTracker(restriction)

    def restriction_size(self, element, restriction):
        return restriction.size()


class SlowRestriction(beam.DoFn):
    def __init__(self, timeline_path):
        self.timeline_path = timeline_path

    def process(
        self,
        element,
        tracker=beam.DoFn.RestrictionParam(BlockRangeProvider()),
    ):
        restriction = tracker.current_restriction()
        offset = restriction.start
        while tracker.try_claim(offset):
            timed_sleep(self.timeline_path, offset)
            yield offset
            offset += 1


def run_pipeline(kind):
    timeline_path = BUILD_DIR / ("%s-timeline.jsonl" % kind)
    claims_path = Path(claims_path_for(timeline_path))
    for path in (timeline_path, claims_path):
        path.unlink(missing_ok=True)

    options = PipelineOptions(
        job_endpoint=JOB_ENDPOINT,
        streaming=False,
    )
    started = time.monotonic()
    with beam.Pipeline(runner=FlareRunner(), options=options) as pipeline:
        if kind == "serial":
            _ = (
                pipeline
                | "CreateBlocks" >> beam.Create(range(BLOCKS))
                | "SlowMap" >> beam.ParDo(SlowMap(str(timeline_path)))
            )
        else:
            _ = (
                pipeline
                | "CreateRestriction" >> beam.Create([BLOCKS])
                | "SlowRestriction" >> beam.ParDo(SlowRestriction(str(timeline_path)))
            )
    elapsed = time.monotonic() - started

    claims = [
        int(line)
        for line in claims_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if sorted(claims) != list(range(BLOCKS)):
        raise AssertionError(
            "%s processed blocks incorrectly: expected=%s actual=%s"
            % (kind, list(range(BLOCKS)), sorted(claims))
        )

    intervals = [
        json.loads(line)
        for line in timeline_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    events = []
    for interval in intervals:
        events.append((interval["start"], 1))
        events.append((interval["finish"], -1))
    active = 0
    max_concurrency = 0
    for _, delta in sorted(events):
        active += delta
        max_concurrency = max(max_concurrency, active)

    return {
        "kind": kind,
        "elapsed_seconds": elapsed,
        "blocks": BLOCKS,
        "block_seconds": BLOCK_SECONDS,
        "max_concurrency": max_concurrency,
        "throughput_blocks_per_second": BLOCKS / elapsed,
    }


def main():
    BUNDLE = BUILD_DIR
    BUNDLE.mkdir(parents=True, exist_ok=True)

    results = {"serial": [], "sdf": []}
    for _ in range(REPETITIONS):
        for kind in ("serial", "sdf"):
            result = run_pipeline(kind)
            results[kind].append(result)
            print(json.dumps(result, sort_keys=True))

    summary = {}
    for kind, runs in results.items():
        summary[kind] = {
            "median_elapsed_seconds": statistics.median(
                run["elapsed_seconds"] for run in runs
            ),
            "median_throughput_blocks_per_second": statistics.median(
                run["throughput_blocks_per_second"] for run in runs
            ),
            "max_observed_concurrency": max(
                run["max_concurrency"] for run in runs
            ),
        }
    summary["speedup"] = (
        summary["serial"]["median_elapsed_seconds"]
        / summary["sdf"]["median_elapsed_seconds"]
    )

    output = BUILD_DIR / "sdf-work-pool-benchmark.json"
    output.write_text(
        json.dumps({"runs": results, "summary": summary}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    print("wrote", output)


if __name__ == "__main__":
    main()
