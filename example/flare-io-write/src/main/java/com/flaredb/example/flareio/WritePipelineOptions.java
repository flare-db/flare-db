package com.flaredb.example.flareio;

import com.flaredb.io.FlareDbIO;
import com.flaredb.runner.FlarePipelineOptions;
import com.flaredb.runner.FlareRunner;
import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface WritePipelineOptions extends FlarePipelineOptions {

  /**
   * Applies the standard FlareDB example defaults: run on {@link FlareRunner}, target the local
   * FlareDB job service, and auto-detect the shadow (uber) JAR produced by this module's {@code
   * shadowJar} task. An explicitly configured {@code --uberJar} takes precedence.
   *
   * @return the same options, for fluent use
   */
  static WritePipelineOptions applyFlareDefaults(WritePipelineOptions options) {
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
    String[] candidateDirs = {"build/libs", "example/flare-io-write/build/libs"};
    for (String dir : candidateDirs) {
      File[] matches =
          new File(dir)
              .listFiles(
                  (d, name) -> name.startsWith("flareio-write-") && name.endsWith("-all.jar"));
      if (matches != null && matches.length > 0) {
        Arrays.sort(matches, Comparator.comparing(File::getName));
        return matches[matches.length - 1];
      }
    }
    return null;
  }

  @Description("Path to the CSV file to load into FlareDB")
  @Default.String("/home/ganesh/flare-db/flareio/flare-db/test-data/scores.csv")
  String getCsvFile();

  void setCsvFile(String csvFile);

  @Description("Destination FlareDB table name")
  @Default.String("flare.default.scores")
  String getTable();

  void setTable(String table);

  @Description("FlareDB endpoint URL")
  @Default.String(FlareDbIO.DEFAULT_DB_URL)
  String getDbUrl();

  void setDbUrl(String dbUrl);
}
