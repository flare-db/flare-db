package com.flaredb.testing;

import com.flaredb.runner.FlarePipelineOptions;
import com.flaredb.runner.FlareRunner;
import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.beam.sdk.options.Description;

/**
 * Options for the {@link TestStreamExample}. The pipeline's input is a scripted {@code TestStream},
 * so the only configurable path is where the recorded panes are written.
 */
public interface TestStreamPipelineOptions extends FlarePipelineOptions {

  /**
   * Returns the file the observed panes are written to. Defaults to {@code
   * build/test-stream-panes.txt} (absolute) when not set.
   */
  @Description("Path to the file the observed panes are written to")
  String getOutputFile();

  /** Sets the file the observed panes are written to. */
  void setOutputFile(String path);

  /**
   * Applies the standard FlareDB example defaults: run on {@link FlareRunner}, target the local
   * FlareDB job service, auto-detect the shadow (uber) JAR produced by this module's {@code
   * shadowJar} task, and default the output file to an absolute path under {@code build/}.
   *
   * @return the same options, for fluent use
   */
  static TestStreamPipelineOptions applyFlareDefaults(TestStreamPipelineOptions options) {
    options.setRunner(FlareRunner.class);
    options.setJobEndpoint("127.0.0.1:8099");
    if (options.getUberJar() == null || options.getUberJar().isEmpty()) {
      File shadowJar = findShadowJar();
      if (shadowJar != null) {
        options.setUberJar(shadowJar.getAbsolutePath());
      }
    }
    if (options.getOutputFile() == null || options.getOutputFile().isEmpty()) {
      // Absolute so main() and the SDK harness agree on the location regardless
      // of their working directories.
      options.setOutputFile(new File("build/test-stream-panes.txt").getAbsolutePath());
    }
    return options;
  }

  /** Locates the shadow (uber) JAR produced by this module's {@code shadowJar} task. */
  private static File findShadowJar() {
    String[] candidateDirs = {"build/libs", "testing/test-stream/build/libs"};
    for (String dir : candidateDirs) {
      File[] matches =
          new File(dir)
              .listFiles((d, name) -> name.startsWith("test-stream-") && name.endsWith("-all.jar"));
      if (matches != null && matches.length > 0) {
        Arrays.sort(matches, Comparator.comparing(File::getName));
        return matches[matches.length - 1];
      }
    }
    return null;
  }
}
