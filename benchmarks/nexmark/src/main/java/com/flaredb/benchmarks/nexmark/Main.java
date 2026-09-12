package com.flaredb.benchmarks.nexmark;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkPerf;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entrypoint for launching Nexmark benchmarks against FlareDB.
 */
public class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);
  private static final String LINE =
      "==========================================================================================";

  public static void main(String[] args) throws IOException {
    PipelineOptionsFactory.register(FlareNexmarkOptions.class);
    FlareNexmarkOptions options = PipelineOptionsFactory.fromArgs(args)
        .as(FlareNexmarkOptions.class);

    if (options.getJobEndpoint() == null || options.getJobEndpoint().isEmpty()) {
      options.setJobEndpoint("127.0.0.1:8099");
    }
    if (options.getRunner() == null 
        || options.getRunner().getName().equals("org.apache.beam.sdk.PipelineRunner")
        || options.getRunner().getName().equals("org.apache.beam.runners.direct.DirectRunner")) {
      options.setRunner(com.flaredb.runner.FlareRunner.class);
    }

    LOG.info("Starting FlareDB Nexmark Benchmark Suite: {}", options.getSuite());

    Set<NexmarkConfiguration> configurations = options.getSuite().getConfigurations(options);
    Map<NexmarkConfiguration, NexmarkPerf> actual = new LinkedHashMap<>();

    for (NexmarkConfiguration configuration : configurations) {
      // Force batch mode as specified for batch queries with default global window alone
      options.setStreaming(false);

      FlareNexmarkLauncher launcher = new FlareNexmarkLauncher(options, configuration);
      try {
        NexmarkPerf perf = launcher.run();
        if (perf != null) {
          actual.put(configuration, perf);
        }
      } catch (Exception e) {
        LOG.error("Failed executing configuration {}", configuration.toShortString(), e);
      }
    }

    printSummary(configurations, actual);
  }

  private static void printSummary(
      Set<NexmarkConfiguration> configurations,
      Map<NexmarkConfiguration, NexmarkPerf> actual) {

    System.out.println();
    System.out.println(LINE);
    System.out.println("FlareDB Nexmark Benchmark Execution Results");
    System.out.println(LINE);
    System.out.println(
        String.format(
            "  %4s  %12s  %16s  %12s",
            "Conf",
            "Runtime(sec)",
            "Events(/sec)",
            "Results"));

    int conf = 0;
    for (NexmarkConfiguration configuration : configurations) {
      String line = String.format("  %04d  ", conf++);
      NexmarkPerf perf = actual.get(configuration);
      if (perf == null) {
        line += "*** not run / skipped ***";
      } else {
        line += String.format("%12.1f  %16.1f  %12d", perf.runtimeSec, perf.eventsPerSec, perf.numResults);
      }
      System.out.println(line);
    }
    System.out.println(LINE);
    System.out.println();
  }
}
