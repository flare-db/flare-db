package com.flaredb.benchmarks.nexmark;

import com.flaredb.benchmarks.nexmark.queries.BatchQueryRegistry;
import com.flaredb.runner.FlarePipelineJob;
import com.flaredb.runner.FlareRunner;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkPerf;
import org.apache.beam.sdk.nexmark.NexmarkQueryName;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.nexmark.queries.NexmarkQuery;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Launcher for running Nexmark benchmark queries against FlareDB runner. */
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
      File shadowJar = findShadowJar();
      if (shadowJar != null) {
        options.setUberJar(shadowJar.getAbsolutePath());
        LOG.info("Auto-detected uber JAR: {}", shadowJar.getAbsolutePath());
      } else {
        LOG.warn(
            "No uber JAR found. Build one with './gradlew :nexmark:shadowJar' or pass "
                + "--uberJar=<path> explicitly.");
      }
    }

    NexmarkQueryName queryNameEnum = configuration.query;
    NexmarkQuery<?> query = BatchQueryRegistry.createBatchQueries(configuration).get(queryNameEnum);
    if (query == null) {
      LOG.warn("Query {} is not supported in batch global window suite, skipping.", queryNameEnum);
      return null;
    }

    String queryName = query.getName();
    LOG.info(
        "Configuring pipeline for Nexmark Query: {} ({})",
        queryName,
        configuration.toShortString());

    Pipeline p = Pipeline.create(options);
    NexmarkUtils.setupPipeline(configuration.coderStrategy, p);

    // Generate batch event source in default global window
    PCollection<Event> source =
        p.apply(queryName + ".ReadEvents", NexmarkUtils.batchEventsSource(configuration));

    if (query.getTransform().needsSideInput()) {
      query.getTransform().setSideInput(NexmarkUtils.prepareSideInput(p, configuration));
    }

    if (options.getLogEvents()) {
      source = source.apply(queryName + ".Events.Log", NexmarkUtils.log(queryName + ".Events"));
    }

    @SuppressWarnings("unchecked")
    PCollection<TimestampedValue<KnownSize>> results =
        (PCollection<TimestampedValue<KnownSize>>) (PCollection<?>) source.apply(query);

    // Output formatting, optional logging, and a terminal counter so every
    // emitted result contributes to the benchmark's Results metric.
    if (options.getLogResults()) {
      results
          .apply(queryName + ".Format", NexmarkUtils.format(queryName))
          .apply(queryName + ".Results.Log", NexmarkUtils.log(queryName + ".Results"))
          .apply(queryName + ".Results.Count", NexmarkUtils.devNull(queryName));
    } else {
      results.apply(queryName + ".Results.Count", NexmarkUtils.devNull(queryName));
    }

    // Measure job execution time
    long startTime = System.currentTimeMillis();
    LOG.info("Submitting query {} job to FlareDB at {}", queryName, options.getJobEndpoint());

    PipelineResult pipelineResult = p.run();
    NexmarkPerf perf;
    try {
      long endTime = System.currentTimeMillis();
      perf = buildPerformance(query, pipelineResult, startTime, endTime);
    } finally {
      closePipelineResult(pipelineResult);
    }

    LOG.info(
        "Completed query {} in {}s (Events/sec: {}, Results/sec: {})",
        queryName,
        String.format("%.2f", perf.runtimeSec),
        String.format("%.1f", perf.eventsPerSec),
        String.format("%.1f", perf.resultsPerSec));
    LOG.info("NexmarkPerf {}", perf);

    return perf;
  }

  static void closePipelineResult(PipelineResult pipelineResult) throws IOException {
    if (pipelineResult instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException("failed to close pipeline result", e);
      }
    }
  }

  private NexmarkPerf buildPerformance(
      NexmarkQuery<?> query, PipelineResult pipelineResult, long startTime, long endTime)
      throws IOException {
    NexmarkPerf perf = new NexmarkPerf();
    perf.jobId = pipelineResult instanceof FlarePipelineJob flareJob ? flareJob.getJobId() : null;
    perf.errors = List.of();

    long eventCount = -1;
    long eventBytes = -1;
    long eventStart = -1;
    long eventEnd = -1;
    if (query.eventMonitor != null) {
      MetricsReader eventMetrics = new MetricsReader(pipelineResult, query.eventMonitor.name);
      eventCount = eventMetrics.getCounterMetric(query.eventMonitor.prefix + ".elements");
      eventBytes = eventMetrics.getCounterMetric(query.eventMonitor.prefix + ".bytes");
      eventStart = eventMetrics.getStartTimeMetric(query.eventMonitor.prefix + ".processingTime");
      eventEnd = eventMetrics.getEndTimeMetric(query.eventMonitor.prefix + ".processingTime");
    }

    long resultCount = -1;
    long resultBytes = -1;
    long resultStart = -1;
    long resultEnd = -1;
    long timestampStart = -1;
    long timestampEnd = -1;
    if (query.resultMonitor != null) {
      MetricsReader resultMetrics = new MetricsReader(pipelineResult, query.resultMonitor.name);
      resultCount = resultMetrics.getCounterMetric(query.resultMonitor.prefix + ".elements");
      resultBytes = resultMetrics.getCounterMetric(query.resultMonitor.prefix + ".bytes");
      resultStart =
          resultMetrics.getStartTimeMetric(query.resultMonitor.prefix + ".processingTime");
      resultEnd = resultMetrics.getEndTimeMetric(query.resultMonitor.prefix + ".processingTime");
      timestampStart =
          resultMetrics.getStartTimeMetric(query.resultMonitor.prefix + ".eventTimestamp");
      timestampEnd = resultMetrics.getEndTimeMetric(query.resultMonitor.prefix + ".eventTimestamp");
    }

    if (resultCount < 0) {
      resultCount = countResults(pipelineResult, query.getName());
    }

    long effectiveEnd = -1;
    if (eventEnd >= 0 && resultEnd >= 0) {
      effectiveEnd = Math.max(eventEnd, resultEnd);
    } else if (resultEnd >= 0) {
      effectiveEnd = resultEnd;
    } else if (eventEnd >= 0) {
      effectiveEnd = eventEnd;
    }

    double wallClockSec = Math.max(0.001, (endTime - startTime) / 1000.0);
    perf.runtimeSec =
        effectiveEnd >= 0 && eventStart >= 0 && effectiveEnd >= eventStart
            ? (effectiveEnd - eventStart) / 1000.0
            : wallClockSec;
    perf.numEvents = eventCount >= 0 ? eventCount : configuration.numEvents;
    perf.numResults = Math.max(0, resultCount);

    if (perf.runtimeSec > 0.0) {
      perf.eventsPerSec = perf.numEvents / perf.runtimeSec;
      perf.resultsPerSec = perf.numResults / perf.runtimeSec;
      if (eventBytes >= 0) {
        perf.eventBytesPerSec = eventBytes / perf.runtimeSec;
      }
      if (resultBytes >= 0) {
        perf.resultBytesPerSec = resultBytes / perf.runtimeSec;
      }
    }

    if (eventStart >= 0) {
      perf.startupDelaySec = (eventStart - startTime) / 1000.0;
    }
    if (resultStart >= 0 && eventStart >= 0 && resultStart >= eventStart) {
      perf.processingDelaySec = (resultStart - eventStart) / 1000.0;
    }
    if (timestampStart >= 0 && timestampEnd >= 0 && perf.runtimeSec > 0.0) {
      double timeDilation = ((timestampEnd - timestampStart) / 1000.0) / perf.runtimeSec;
      perf = NexmarkUtils.MAPPER.updateValue(perf, Map.of("timeDilation", timeDilation));
    }
    if (resultEnd >= 0) {
      perf.shutdownDelaySec = Math.max(0.0, (endTime - resultEnd) / 1000.0);
    }

    NexmarkPerf.ProgressSnapshot snapshot =
        NexmarkUtils.MAPPER.convertValue(
            Map.of(
                "secSinceStart", wallClockSec,
                "runtimeSec", perf.runtimeSec,
                "numEvents", perf.numEvents,
                "numResults", perf.numResults),
            NexmarkPerf.ProgressSnapshot.class);
    perf.snapshots = List.of(snapshot);

    return perf;
  }

  private static long countResults(PipelineResult pipelineResult, String queryName) {
    MetricQueryResults metrics =
        pipelineResult
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(queryName, "discarded"))
                    .build());

    long results = 0;
    for (MetricResult<Long> counter : metrics.getCounters()) {
      results += counter.getAttempted();
    }
    return results;
  }

  /** Locates the Nexmark shadow (uber) JAR produced by the {@code shadowJar} task. */
  private static File findShadowJar() {
    String[] candidateDirs = {"build/libs", "benchmarks/nexmark/build/libs"};
    for (String dir : candidateDirs) {
      File[] matches =
          new File(dir)
              .listFiles((d, name) -> name.startsWith("nexmark-") && name.endsWith("-all.jar"));
      if (matches != null && matches.length > 0) {
        Arrays.sort(matches, Comparator.comparing(File::getName));
        return matches[matches.length - 1];
      }
    }
    return null;
  }
}
