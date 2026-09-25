package com.flaredb.example.flareio;

import com.flaredb.io.FlareDbIO;
import com.flaredb.runner.FlarePipelineOptions;
import com.flaredb.runner.FlareRunner;
import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface ReadPipelineOptions extends FlarePipelineOptions {

  /**
   * Applies the standard FlareDB example defaults: run on {@link FlareRunner}, target the local
   * FlareDB job service, and auto-detect the shadow (uber) JAR produced by this module's {@code
   * shadowJar} task. An explicitly configured {@code --uberJar} takes precedence.
   *
   * @return the same options, for fluent use
   */
  static ReadPipelineOptions applyFlareDefaults(ReadPipelineOptions options) {
    options.setRunner(FlareRunner.class);
    options.setJobEndpoint("127.0.0.1:8099");
    if (options.getUberJar() == null || options.getUberJar().isEmpty()) {
      File shadowJar = findShadowJar();
      if (shadowJar != null) {
        options.setUberJar(shadowJar.getAbsolutePath());
      }
    }
    return options;
  }

  /** Locates the shadow (uber) JAR produced by this module's {@code shadowJar} task. */
  private static File findShadowJar() {
    String[] candidateDirs = {"build/libs", "example/flare-io-read/build/libs"};
    for (String dir : candidateDirs) {
      File[] matches =
          new File(dir)
              .listFiles(
                  (d, name) -> name.startsWith("flareio-read-") && name.endsWith("-all.jar"));
      if (matches != null && matches.length > 0) {
        Arrays.sort(matches, Comparator.comparing(File::getName));
        return matches[matches.length - 1];
      }
    }
    return null;
  }

  @Description("FlareDB endpoint URL")
  @Default.String(FlareDbIO.DEFAULT_DB_URL)
  String getDbUrl();

  void setDbUrl(String dbUrl);

  @Description("Path of the text file the read rows are written to")
  @Default.String("/home/ganesh/flare-db/flareio/flare-db/test-data/scores_out.txt")
  String getOutputFile();

  void setOutputFile(String outputFile);
}
