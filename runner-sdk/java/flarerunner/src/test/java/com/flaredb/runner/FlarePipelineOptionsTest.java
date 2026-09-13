package com.flaredb.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

public class FlarePipelineOptionsTest {

  @Test
  public void uberJarDefaultsToNull() {
    FlarePipelineOptions options = PipelineOptionsFactory.as(FlarePipelineOptions.class);

    assertNull(options.getUberJar());
  }

  @Test
  public void uberJarRoundTripsThroughGetterAndSetter() {
    FlarePipelineOptions options = PipelineOptionsFactory.as(FlarePipelineOptions.class);

    options.setUberJar("/path/to/app-all.jar");

    assertEquals("/path/to/app-all.jar", options.getUberJar());
  }

  @Test
  public void filesToStageRoundTripsThroughGetterAndSetter() {
    FlarePipelineOptions options = PipelineOptionsFactory.as(FlarePipelineOptions.class);
    List<String> files = Arrays.asList("a.jar", "b.jar");

    options.setFilesToStage(files);

    assertEquals(files, options.getFilesToStage());
  }

  @Test
  public void optionsAreCreatedFromCommandLineArgs() {
    FlarePipelineOptions options =
        PipelineOptionsFactory.fromArgs("--uberJar=/tmp/app.jar", "--jobEndpoint=localhost:8099")
            .as(FlarePipelineOptions.class);

    assertEquals("/tmp/app.jar", options.getUberJar());
    assertEquals("localhost:8099", options.getJobEndpoint());
  }
}
