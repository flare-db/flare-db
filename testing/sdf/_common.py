"""Shared helpers for the SDF integration pipelines under testing/sdf/.

Run the pipelines from the repository root with a FlareDB instance listening
on 127.0.0.1:8099.
"""

from pathlib import Path

from apache_beam.options.pipeline_options import PipelineOptions

JOB_ENDPOINT = "127.0.0.1:8099"


def pipeline_options(argv=None):
    """Pipeline options pointing at the local FlareDB instance."""
    return PipelineOptions(argv or [], job_endpoint=JOB_ENDPOINT)


def build_dir():
    """Directory for claim/result files, under the repository root."""
    path = Path("build").resolve() / "sdf"
    path.mkdir(parents=True, exist_ok=True)
    return path


def reset_file(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()


def read_ints(path):
    path = Path(path)
    if not path.exists():
        return []
    return [
        int(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
