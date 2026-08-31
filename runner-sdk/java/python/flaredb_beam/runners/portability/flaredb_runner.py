from __future__ import annotations

import logging
import os
import typing
from apache_beam.options import pipeline_options
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_runner
from apache_beam.transforms import environments
from apache_beam.utils import shared
from apache_beam.utils import subprocess_server

_LOGGER = logging.getLogger(__name__)

class FlareDBRunner(portable_runner.PortableRunner):
    """Native Python Runner SDK targeting FlareDB clusters and local instances."""
    
    shared_handle = shared.Shared()

    def default_environment(
        self, 
        options: pipeline_options.PipelineOptions
    ) -> environments.Environment:
        portable_options = options.view_as(pipeline_options.PortableOptions)
        
        if not portable_options.environment_type and not portable_options.output_executable_path:
            portable_options.environment_type = 'LOOPBACK'
            
        return super().default_environment(options)

    def default_job_server(self, options: pipeline_options.PipelineOptions):
        get_job_server = lambda: job_server.StopOnExitJobServer(FlareDBJobServer(options))
        return FlareDBRunner.shared_handle.acquire(get_job_server)


class FlareDBJobServer(job_server.SubprocessJobServer):
    """Orchestrates the background FlareDB server instance binary lifecycle."""

    def __init__(self, options: pipeline_options.PipelineOptions):
        super().__init__()
        job_options = options.view_as(pipeline_options.JobServerOptions)
        self._job_port = job_options.job_port
        self._binary = getattr(
            options.view_as(pipeline_options.DebugOptions), 
            'flaredb_binary_path', 
            'flaredb'
        )

    def subprocess_cmd_and_endpoint(self) -> tuple[list[str], str]:
        job_port, = subprocess_server.pick_port(self._job_port)
        
        subprocess_cmd = [
            self._binary,
            '--job_port', str(job_port),
            '--storage_engine', 'tonbo',
            '--enable_element_store=true'
        ]
        
        _LOGGER.info("Launching FlareDB portable job coordinator: %s", " ".join(subprocess_cmd))
        return subprocess_cmd, f"localhost:{job_port}"
