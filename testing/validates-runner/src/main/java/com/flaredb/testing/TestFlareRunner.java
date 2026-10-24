package com.flaredb.testing;

import com.flaredb.runner.FlarePipelineJob;
import com.flaredb.runner.FlarePipelineOptions;
import com.flaredb.runner.FlareRunner;
import java.io.File;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFlareRunner extends PipelineRunner<PipelineResult> {
  private static final Logger LOG = LoggerFactory.getLogger(TestFlareRunner.class);

  private final FlareRunner delegate;
  private final FlarePipelineOptions options;

  private TestFlareRunner(FlareRunner delegate, FlarePipelineOptions options) {
    this.delegate = delegate;
    this.options = options;
  }

  public static TestFlareRunner fromOptions(PipelineOptions options) {
    FlarePipelineOptions flareOptions = options.as(FlarePipelineOptions.class);
    if (flareOptions.getJobEndpoint() == null || flareOptions.getJobEndpoint().isEmpty()) {
      flareOptions.setJobEndpoint("127.0.0.1:8099");
    }
    if (flareOptions.getUberJar() == null || flareOptions.getUberJar().isEmpty()) {
      File shadowJar = findShadowJar();
      if (shadowJar != null) {
        flareOptions.setUberJar(shadowJar.getAbsolutePath());
      }
    }
    FlareRunner delegate = FlareRunner.fromOptions(flareOptions);
    return new TestFlareRunner(delegate, flareOptions);
  }

  private static File findShadowJar() {
    File[] candidates =
        new File[] {
          new File("build/libs/validates-runner-0.3.2-all.jar"),
          new File("testing/validates-runner/build/libs/validates-runner-0.3.2-all.jar")
        };
    for (File candidate : candidates) {
      if (candidate.exists()) {
        return candidate;
      }
    }

    // Try finding the jar dynamically in candidate directories
    File[] dirs =
        new File[] {new File("build/libs"), new File("testing/validates-runner/build/libs")};
    for (File dir : dirs) {
      if (dir.isDirectory()) {
        File[] matches =
            dir.listFiles(
                (d, name) -> name.startsWith("validates-runner-") && name.endsWith("-all.jar"));
        if (matches != null && matches.length > 0) {
          return matches[0];
        }
      }
    }
    return null;
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    LOG.info("Running pipeline on TestFlareRunner with uberJar={}", options.getUberJar());
    FlarePipelineJob job = delegate.run(pipeline);
    PipelineResult.State state = job.getState();
    Assert.assertEquals("Pipeline execution was not DONE", PipelineResult.State.DONE, state);
    TestPipeline.verifyPAssertsSucceeded(pipeline, job);
    return job;
  }

  public static class Registrar implements PipelineRunnerRegistrar {
    @Override
    public Iterable<Class<? extends PipelineRunner<?>>> getPipelineRunners() {
      return ImmutableList.of(TestFlareRunner.class);
    }
  }
}
