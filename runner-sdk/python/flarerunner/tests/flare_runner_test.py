"""Tests pinning down how FlareRunner integrates with Beam's runner
machinery, mirroring the intent of FlarePipelineOptionsTest.java: verify the
public contract pipeline authors rely on, not Beam's own internals."""

import unittest

from apache_beam.runners.portability.portable_runner import PortableRunner
from apache_beam.runners.runner import create_runner

from flaredb_runner.flare_runner import FlareRunner


class FlareRunnerTest(unittest.TestCase):

  def test_is_a_portable_runner(self):
    self.assertTrue(issubclass(FlareRunner, PortableRunner))

  def test_does_not_override_run_portable_pipeline(self):
    # FlareDB speaks Beam's Job API directly, so the generic
    # translate/prepare/stage/run flow must stay untouched.
    self.assertIs(
        FlareRunner.run_portable_pipeline,
        PortableRunner.run_portable_pipeline,
    )

  def test_resolvable_via_fully_qualified_runner_name(self):
    runner = create_runner('flaredb_runner.flare_runner.FlareRunner')
    self.assertIsInstance(runner, FlareRunner)


if __name__ == '__main__':
  unittest.main()
