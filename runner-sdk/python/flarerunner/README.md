# flaredb-runner (Python)

Apache Beam portable runner for submitting Python pipelines to FlareDB. This
is the Python counterpart to `runner-sdk/java/flarerunner`: FlareDB speaks
Beam's portable Job API and Fn API directly, so pipeline translation,
artifact staging, and worker execution are already implemented generically
by `apache_beam`. `FlareRunner` just gives that machinery a FlareDB-specific
name.

## Install

```sh
pip install -e runner-sdk/python/flarerunner
```

## Run

1. Start a local FlareDB dev instance (from the repo root):

   ```sh
   ./flareup-dev.sh --debug
   ```

2. Submit a pipeline against it:

   ```sh
   python -m apache_beam.examples.wordcount \
       --input=<path> \
       --output=<path> \
       --runner=flaredb_runner.flare_runner.FlareRunner \
       --job_endpoint=localhost:8099
   ```

`FlareRunner` does not start a job server for you (unlike `FlinkRunner` or
`PrismRunner`) — start FlareDB separately, then point `--job_endpoint` at it.

## Test

```sh
cd runner-sdk/python/flarerunner
python -m unittest discover -s tests -p "*_test.py"
```
