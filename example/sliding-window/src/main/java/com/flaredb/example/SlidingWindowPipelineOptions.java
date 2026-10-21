package com.flaredb.example;

import com.flaredb.runner.FlarePipelineOptions;
import com.flaredb.runner.FlareRunner;
import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.beam.sdk.options.Description;

public interface SlidingWindowPipelineOptions extends FlarePipelineOptions {

  /**
   * Returns the text file to count words in. Defaults to the bundled sample ({@code
   * test-data/scores.csv}) when not set; override with {@code --inputFile=/path/to/file}.
   */
  @Description("Path to the text file to count words in")
  String getInputFile();

  /** Sets the text file to count words in. */
  void setInputFile(String path);

  /**
   * Applies the standard FlareDB example defaults: run on {@link FlareRunner}, target the local
   * FlareDB job service, and auto-detect the shadow (uber) JAR produced by this module's {@code
   * shadowJar} task, and auto-detect the bundled sample input file. Explicitly configured {@code
   * --uberJar} and {@code --inputFile} values take precedence.
   *
   * @return the same options, for fluent use
   */
  static SlidingWindowPipelineOptions applyFlareDefaults(SlidingWindowPipelineOptions options) {
    options.setRunner(FlareRunner.class);
    options.setJobEndpoint("127.0.0.1:8099");
    if (options.getUberJar() == null || options.getUberJar().isEmpty()) {
      File shadowJar = findShadowJar();
      if (shadowJar != null) {
        options.setUberJar(shadowJar.getAbsolutePath());
      }
    }
    if (options.getInputFile() == null || options.getInputFile().isEmpty()) {
      File inputFile = findSampleInput();
      if (inputFile != null) {
        options.setInputFile(inputFile.getAbsolutePath());
      }
    }
    return options;
  }

  /** Locates the shadow (uber) JAR produced by this module's {@code shadowJar} task. */
  private static File findShadowJar() {
    String[] candidateDirs = {"build/libs", "example/silding-window/build/libs"};
    for (String dir : candidateDirs) {
      File[] matches =
          new File(dir)
              .listFiles(
                  (d, name) -> name.startsWith("sliding-window-") && name.endsWith("-all.jar"));
      if (matches != null && matches.length > 0) {
        Arrays.sort(matches, Comparator.comparing(File::getName));
        return matches[matches.length - 1];
      }
    }
    return null;
  }

  /**
   * Locates the bundled sample input file, probing paths relative to both the module directory
   * (when run via {@code :fixed-window:run}) and the repository root.
   */
  private static File findSampleInput() {
    String[] candidates = {"test-data/scores.csv", "../../test-data/scores.csv"};
    for (String candidate : candidates) {
      File file = new File(candidate);
      if (file.isFile()) {
        return file;
      }
    }
    return null;
  }
}
