"""Tests pinning down how FlareRunner integrates with Beam's runner
machinery, mirroring the intent of FlarePipelineOptionsTest.java: verify the
public contract pipeline authors rely on, not Beam's own internals."""

import sys
import unittest
from types import SimpleNamespace
from unittest import mock

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import runner
from apache_beam.runners.runner import create_runner

from flaredb_runner.flare_runner import FlareRunner
from flaredb_runner.flare_runner import JobServiceHandle


class _FakeJobService(object):
  """Minimal stand-in for Beam's JobService stub (prepare/stage/run only)."""

  def __init__(self):
    self.prepare_response = SimpleNamespace(
        preparation_id='jobid-1234',
        artifact_staging_endpoint=SimpleNamespace(url='127.0.0.1:8099'),
        staging_session_token='token')

  def DescribePipelineOptions(self, request, timeout=None):
    return SimpleNamespace(options=[])

  def Prepare(self, request, timeout=None):
    return self.prepare_response

  def GetStateStream(self, request, timeout=None):
    return iter([SimpleNamespace(state=beam_job_api_pb2.JobState.RUNNING)])

  def GetMessageStream(self, request, timeout=None):
    return iter([])

  def Run(self, request, timeout=None):
    return SimpleNamespace(job_id='jobid-1234')


class FlareRunnerTest(unittest.TestCase):

  def test_is_a_pipeline_runner(self):
    self.assertTrue(issubclass(FlareRunner, runner.PipelineRunner))

  def test_resolvable_via_fully_qualified_runner_name(self):
    runner_ = create_runner('flaredb_runner.flare_runner.FlareRunner')
    self.assertIsInstance(runner_, FlareRunner)

  def test_default_environment_uses_running_interpreter(self):
    # FlareDB starts the SDK harness with the very interpreter running this pipeline
    options = PipelineOptions([], job_endpoint='x:1')
    FlareRunner().default_environment(options)
    portable_options = options.view_as(PortableOptions)
    self.assertEqual(portable_options.environment_type, 'PROCESS')
    self.assertEqual(
        portable_options.lookup_environment_option('process_command'),
        sys.executable)

  def test_uses_configured_job_name(self):
    options = PipelineOptions(['--job_name=my-job'], job_endpoint='x:1')
    handle = FlareRunner().create_job_service_handle(object(), options)
    self.assertEqual(handle._job_name, 'my-job')

  def test_generates_a_job_name_when_unset(self):
    options = PipelineOptions([], job_endpoint='x:1')
    handle = FlareRunner().create_job_service_handle(object(), options)
    self.assertTrue(handle._job_name)
    self.assertNotEqual(handle._job_name, 'None')

  def test_submission_logs_each_handshake_step(self):
    options = PipelineOptions(['--job_name=my-job'], job_endpoint='x:1')
    handle = JobServiceHandle(_FakeJobService(), options)
    proto_pipeline = beam_runner_api_pb2.Pipeline()

    with mock.patch(
        'flaredb_runner.flare_runner.artifact_service'
        '.offer_artifacts'), self.assertLogs(
            'flaredb_runner.flare_runner', level='INFO') as logs:
      handle.submit(proto_pipeline)

    messages = [record.getMessage() for record in logs.records]
    self.assertEqual(
        messages,
        [
            'PrepareJobResponse received for jobName=my-job',
            'Staging artifacts to 127.0.0.1:8099',
            'Artifact staging completed',
            'Created run job request: JOB-ID jobid-1234',
            'RunJobResponse received jobName=my-job (JOB-ID jobid-1234)',
            'Job execution completed',
        ])

  def test_prepares_with_resolved_job_name(self):
    options = PipelineOptions(['--job_name=my-job'], job_endpoint='x:1')
    job_service = _FakeJobService()
    handle = JobServiceHandle(job_service, options)

    with mock.patch.object(
        job_service, 'Prepare',
        return_value=job_service.prepare_response) as prepare:
      handle.prepare(beam_runner_api_pb2.Pipeline())

    self.assertEqual(prepare.call_args.args[0].job_name, 'my-job')


if __name__ == '__main__':
  unittest.main()
