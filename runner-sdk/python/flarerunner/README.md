FlareDB runs Apache Beam pipelines as jobs. Build your pipeline with the Beam Python SDK as usual, then set `FlareRunner` as the runner to submit it to a running FlareDB instance.

## Prerequisites

- A running FlareDB instance. See the [Quickstart](/quickstart).
- Python 3.9+.
- An Apache Beam pipeline. The examples in this guide use Apache Beam 2.76.0.

## 1. Install the runner dependency

Create and activate a virtual environment, then install the `flaredb-runner` package from the repository. It includes flare and apache-beam dependency.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install flaredb-runner
```

## 2. Configure the pipeline

Set `FlareRunner` as the runner and point it at the FlareDB instance:

```python wordcount.py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from flaredb_runner.flare_runner import FlareRunner

pipeline_options = PipelineOptions(
    job_endpoint="127.0.0.1:8099",
)

with beam.Pipeline(runner=FlareRunner(), options=pipeline_options) as p:
    ...
```

Unlike Java, there is nothing to stage manually: FlareDB ships your pipeline code to the workers automatically. See the full pipeline in the [WordCount example](https://github.com/flare-db/flare-db/tree/main/example/python/wordcount.py).

## 3. Run the pipeline

With a FlareDB instance running, submit the pipeline:

```bash
python3 wordcount.py
```

## Pipeline options

Pass options as keyword arguments to `PipelineOptions`:

| Option | Usage | Description |
| --- | --- | --- |
| Runner | `runner=FlareRunner()` | Pipeline runner. |
| Job endpoint | `job_endpoint="host:port"` | URL of the FlareDB job service. Required. |
| Job name | `job_name="my-job"` | Name of the submitted job. |

These options can also be provided as command-line arguments:

```bash
python3 wordcount.py --job_endpoint=127.0.0.1:8099 --job_name=my-job
```

## Pipeline logs

A `JOB-ID` is generated automatically for each submitted job. Use it to view logs for a specific job, or stream the most recent logs when you do not provide an ID. Usage:

```bash
  flare logs                  # Stream logs for the most recent job
  flare logs <JOB_ID>         # Stream logs for a specific job
```

The `JOB-ID` is logged during job submission via the runner SDK.
