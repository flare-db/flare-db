"""Apache Beam Runner implementation for executing pipelines on a FlareDB.

Mirrors the Java runner-sdk (``com.flaredb.runner.FlareRunner``): FlareDB
speaks Beam's portable Job API and Fn API directly, so submission
(translate pipeline -> prepare -> stage artifacts -> run) and worker
execution (the Python SDK harness) are already implemented generically by
``apache_beam``. This module only needs to give that generic machinery a
FlareDB-specific name pipeline authors can pass to ``--runner``.
"""

from apache_beam.runners.portability.portable_runner import PortableRunner


class FlareRunner(PortableRunner):
  """A runner for submitting portable Beam pipelines to a FlareDB Job Service.

  Usage::

      python -m my_pipeline \\
          --runner=flaredb_runner.flare_runner.FlareRunner \\
          --job_endpoint=localhost:8099

  ``job_endpoint`` must point at a running FlareDB Job Service (see
  ``flareup-dev.sh`` at the repository root). Unlike ``FlinkRunner`` or
  ``PrismRunner``, ``FlareRunner`` does not start a local job server on
  your behalf -- start FlareDB separately, then submit against it.
  """

  # Inherits run_portable_pipeline (translate/prepare/stage/run) and
  # default_environment from PortableRunner unchanged.
