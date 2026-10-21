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
   * shadowJar} task, and auto-detect the bundled sample input file. Explicitly configured {@code
   * --uberJar} and {@code --csvFile} values take precedence.
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
    if (options.getCsvFile() == null || options.getCsvFile().isEmpty()) {
      File csvFile = findSampleInput();
      if (csvFile != null) {
        options.setCsvFile(csvFile.getAbsolutePath());
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

  /**
   * Locates the bundled sample input file, probing paths relative to both the module directory
   * (when run via {@code :flareio-write:run}) and the repository root.
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

  /**
   * Returns the CSV file to load into FlareDB. Defaults to the bundled sample ({@code
   * test-data/scores.csv}) when not set; override with {@code --csvFile=/path/to/file}.
   */
  @Description("Path to the CSV file to load into FlareDB")
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
