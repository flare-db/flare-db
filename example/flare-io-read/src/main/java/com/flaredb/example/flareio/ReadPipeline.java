package com.flaredb.example.flareio;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flaredb.io.FlareDbIO;
import com.flaredb.runner.FlareRunner;

/**
 * Example pipeline that reads rows from FlareDB using {@link FlareDbIO}.
 *
 * <p>A SQL query is executed against FlareDB with {@link FlareDbIO.Read} over Arrow Flight SQL and
 * each returned row is written to a text file with {@link TextIO}.
 *
 * <pre>{@code
 * ./gradlew :flareio-read:shadowJar
 * ./gradlew :flareio-read:run
 * }</pre>
 */
public class ReadPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(ReadPipeline.class);

  public static void main(String[] args) {
    ReadPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(ReadPipelineOptions.class);
    options.setRunner(FlareRunner.class);
    options.setJobName("flareio-read-scores");
    options.setJobEndpoint("127.0.0.1:8099");
    options.setUberJar(
        "/home/ganesh/flare-db/flareio/flare-db/example/flare-io-read/build/libs/"
            + "flareio-read-0.1.0-all.jar");

    Pipeline pipeline = Pipeline.create(options);

    LOG.info("Reading rows from FlareDB at {}", options.getDbUrl());

    // Execute the SQL query on FlareDB and read the result rows.
    PCollection<Row> rows =
        pipeline.apply(
            "ReadFromFlareDb",
            FlareDbIO.read()
            .fromQuery("SELECT id, name, score FROM flare.default.scores WHERE score > 90 ORDER BY id")
            .withDbUrl(options.getDbUrl()));

    rows.apply(
        "LogRows",
        ParDo.of(
            new DoFn<Row, Void>() {
              @ProcessElement
              public void processElement(ProcessContext context) {
                LOG.info("Row: {}", context.element());
              }
            }));

    pipeline.run();
  }
}
