package com.flaredb.example.flareio;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flaredb.io.FlareDbIO;
import com.flaredb.runner.FlareRunner;

/**
 * Example pipeline that loads a CSV file into FlareDB using {@link FlareDbIO}.
 *
 * <p>The CSV is read with {@link TextIO}, each line is parsed into a schema-aware Beam {@link Row},
 * and the rows are streamed to a FlareDB table with {@link FlareDbIO.Write} over Arrow Flight.
 *
 * <pre>{@code
 * ./gradlew :flareio-write:shadowJar
 * ./gradlew :flareio-write:run
 * }</pre>
 */
public class WritePipeline {

  private static final Logger LOG = LoggerFactory.getLogger(WritePipeline.class);

  /** Beam schema matching the columns of {@code test-data/scores.csv}. */
  static final Schema SCORES_TABLE_SCHEMA =
      Schema.builder()
          .addInt64Field("id")
          .addStringField("name")
          .addStringField("city")
          .addInt64Field("age")
          .addInt64Field("score")
          .build();

  /** Parses a CSV line into a Beam {@link Row} conforming to {@link #SCORES_SCHEMA}. */
  static class CsvLineToRowFn extends DoFn<String, Row> {

    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<Row> out) {
      if (line == null || line.trim().isEmpty()) {
        return; // Skip blank lines.
      }

      String[] fields = line.split(",", -1);
      if (fields[0].equals("id")) {
        return; // Skip the header row.
      }
      if (fields.length != SCORES_TABLE_SCHEMA.getFieldCount()) {
        LOG.warn(
            "Skipping malformed CSV line (expected {} fields, got {}): {}",
            SCORES_TABLE_SCHEMA.getFieldCount(), fields.length, line);
        return;
      }

      try {
        Row row;
          row = Row.withSchema(SCORES_TABLE_SCHEMA)
                  .addValue(Long.valueOf(fields[0].trim()))
                  .addValue(fields[1].trim())
                  .addValue(fields[2].trim())
                  .addValue(Long.valueOf(fields[3].trim()))
                  .addValue(Long.valueOf(fields[4].trim()))
                  .build();
        out.output(row);
      } catch (NumberFormatException e) {
        LOG.warn("Skipping CSV line with a non-numeric field: {}", line);
      }
    }
  }

  public static void main(String[] args) {
    WritePipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(WritePipelineOptions.class);
    options.setRunner(FlareRunner.class);
    options.setJobName("flareio-write-scores");
    options.setJobEndpoint("127.0.0.1:8099");
    options.setUberJar(
        "/home/ganesh/flare-db/flareio/flare-db/example/flare-io-write/build/libs/"
            + "flareio-write-0.1.0-all.jar");

    Pipeline pipeline = Pipeline.create(options);

    LOG.info(
        "Loading {} into FlareDB table {} at {}",
        options.getCsvFile(), options.getTable(), options.getDbUrl());

    // Read the CSV, parse each row, and stream the rows into FlareDB.
    PCollection<Row> rows;
      rows = pipeline
              .apply("ReadCsv", TextIO.read().from(options.getCsvFile()))
              .apply("ParseCsvToRows", ParDo.of(new CsvLineToRowFn()))
              .setRowSchema(SCORES_TABLE_SCHEMA);

    rows.apply(
        "WriteToFlareDb",
        FlareDbIO.<Row>write()
            .to(options.getTable())
            .withDbUrl(options.getDbUrl()));

    pipeline.run();
  }
}
