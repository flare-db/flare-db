"""Apache Beam Runner implementation for executing pipelines on a FlareDB."""

import sys

from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.runners.portability.portable_runner import PortableRunner


class FlareRunner(PortableRunner):
  """A runner for submitting portable Beam pipelines to a FlareDB Job Service.

  FlareDB exposes an Job Service based on Beam's Job API over gRPC
  and can be set via ``--job_endpoint``
  """

  def default_environment(self, options):
    """Advertises the environment FlareDB workers actually run in.

    FlareDB executes SDK user code in-process: the Job Service launches an SDK
    harness subprocess that dials back into the same endpoint.
    """

    #The PROCESS environment's ``command`` names the interpreter the Job Service
    #should start the harness with. We set it to ``sys.executable`` -- the very
    #interpreter running this pipeline -- because that interpreter is known to
    #have ``apache_beam`` importable. Without it the service falls back to
    #whatever ``python3`` resolves to on its own ``PATH``, which is commonly a
    #different environment and fails with ``ModuleNotFoundError: apache_beam``.
    portable_options = options.view_as(PortableOptions)
    if not portable_options.environment_type:
      portable_options.environment_type = 'PROCESS'
    if (portable_options.environment_type == 'PROCESS' and
        not portable_options.lookup_environment_option('process_command')):
      portable_options.add_environment_option(
          'process_command=' + sys.executable)
    return super().default_environment(options)
