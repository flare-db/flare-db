package com.flaredb.benchmarks.nexmark;

import java.io.File;
import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkPerf;
import org.apache.beam.sdk.nexmark.NexmarkQueryName;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.nexmark.queries.NexmarkQuery;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flaredb.benchmarks.nexmark.queries.BatchQueryRegistry;
import com.flaredb.runner.FlareRunner;

/**
 * Launcher for running Nexmark benchmark queries against FlareDB runner.
 */
public class FlareNexmarkLauncher {
  private static final Logger LOG = LoggerFactory.getLogger(FlareNexmarkLauncher.class);

  private final FlareNexmarkOptions options;
  private final NexmarkConfiguration configuration;

  public FlareNexmarkLauncher(FlareNexmarkOptions options, NexmarkConfiguration configuration) {
    this.options = options;
    this.configuration = configuration;
  }

  public NexmarkPerf run() throws IOException {
    // Configure default runner to FlareRunner if not specified or set to DirectRunner
    if (options.getRunner() == null 
        || options.getRunner().getName().equals("org.apache.beam.sdk.PipelineRunner")
        || options.getRunner().getName().equals("org.apache.beam.runners.direct.DirectRunner")) {
      options.setRunner(FlareRunner.class);
    }

    // Default job endpoint to 127.0.0.1:8099 if empty
    if (options.getJobEndpoint() == null || options.getJobEndpoint().isEmpty()) {
      options.setJobEndpoint("127.0.0.1:8099");
    }

    // Auto-detect shadow jar if uberJar path is missing
    if (options.getUberJar() == null || options.getUberJar().isEmpty()) {
      File shadowJar = new File("benchmarks/nexmark/build/libs/nexmark-0.1.0-all.jar");
      if (!shadowJar.exists()) {
        shadowJar = new File("build/libs/nexmark-0.1.0-all.jar");
      }
      if (shadowJar.exists()) {
        options.setUberJar(shadowJar.getAbsolutePath());
      }
    }

    NexmarkQueryName queryNameEnum = configuration.query;
    NexmarkQuery<?> query = BatchQueryRegistry.createBatchQueries(configuration).get(queryNameEnum);
    if (query == null) {
      LOG.warn("Query {} is not supported in batch global window suite, skipping.", queryNameEnum);
      return null;
    }

    String queryName = query.getName();
    LOG.info("Configuring pipeline for Nexmark Query: {} ({})", queryName, configuration.toShortString());

    Pipeline p = Pipeline.create(options);
    NexmarkUtils.setupPipeline(configuration.coderStrategy, p);

    // Generate batch event source in default global window
    PCollection<Event> source = p.apply(queryName + ".ReadEvents", NexmarkUtils.batchEventsSource(configuration));

    if (query.getTransform().needsSideInput()) {
      query.getTransform().setSideInput(NexmarkUtils.prepareSideInput(p, configuration));
    }

    if (options.getLogEvents()) {
      source = source.apply(queryName + ".Events.Log", NexmarkUtils.log(queryName + ".Events"));
    }

    // Execute query transform
    @SuppressWarnings("unchecked")
    PCollection<TimestampedValue<KnownSize>> results =
        (PCollection<TimestampedValue<KnownSize>>) source.apply(query);

    // Output formatting & optional logging
    if (options.getLogResults()) {
      results
          .apply(queryName + ".Format", NexmarkUtils.format(queryName))
          .apply(queryName + ".Results.Log", NexmarkUtils.log(queryName + ".Results"));
    }

    // Measure job execution time
    long startTime = System.currentTimeMillis();
    LOG.info("Submitting query {} job to FlareDB at {}", queryName, options.getJobEndpoint());

    p.run();

    long endTime = System.currentTimeMillis();
    double runtimeSec = Math.max(0.001, (endTime - startTime) / 1000.0);

    NexmarkPerf perf = new NexmarkPerf();
    perf.runtimeSec = runtimeSec;
    perf.numEvents = configuration.numEvents;
    perf.eventsPerSec = configuration.numEvents / runtimeSec;
    perf.numResults = 0; // Estimated or reported

    LOG.info("Completed query {} in {}s (Events/sec: {})", queryName, String.format("%.2f", runtimeSec), String.format("%.1f", perf.eventsPerSec));

    return perf;
  }
}
