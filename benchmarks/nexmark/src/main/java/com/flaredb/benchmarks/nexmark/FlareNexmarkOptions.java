package com.flaredb.benchmarks.nexmark;

import com.flaredb.runner.FlarePipelineOptions;
import org.apache.beam.sdk.nexmark.NexmarkOptions;

/**
 * Combined pipeline options for executing Nexmark benchmark queries on FlareDB.
 */
public interface FlareNexmarkOptions extends FlarePipelineOptions, NexmarkOptions {
}
