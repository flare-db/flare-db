package com.flaredb.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FixedWindowExample {

  private static final Logger LOG = LoggerFactory.getLogger(FixedWindowExample.class);

  public static void main(String[] args) {

    FixedWindowPipelineOptions options =
        FixedWindowPipelineOptions.applyFlareDefaults(
            PipelineOptionsFactory.fromArgs(args).as(FixedWindowPipelineOptions.class));

    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> lines =
        pipeline.apply("ReadCSV", TextIO.read().from(options.getInputFile()));

    PCollection<KV<String, Integer>> records =
        lines.apply(
            "ParseCSV",
            ParDo.of(
                new DoFn<String, KV<String, Integer>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {

                    String line = c.element();

                    // Skip header.
                    if (line.startsWith("id,")) {
                      return;
                    }

                    String[] fields = line.split(",");

                    long id = Long.parseLong(fields[0]);
                    String name = fields[1];
                    int score = Integer.parseInt(fields[4]);

                    // Deterministic event timestamp.
                    Instant timestamp = Instant.ofEpochSecond(id - 1);

                    c.outputWithTimestamp(KV.of(name, score), timestamp);
                  }
                }));

    PCollection<KV<String, Integer>> windowed =
        records.apply("FixedWindow", Window.into(FixedWindows.of(Duration.standardSeconds(60))));

    // Runner-owned GroupByKey.
    // Elements with the same key are grouped independently per window.
    PCollection<KV<String, Iterable<Integer>>> grouped =
        windowed.apply("GroupByKey", GroupByKey.create());

    grouped.apply(
        "PrintGroupedResults",
        ParDo.of(
            new DoFn<KV<String, Iterable<Integer>>, Void>() {

              @ProcessElement
              public void processElement(ProcessContext c, BoundedWindow window) {

                LOG.info(
                    "key={} values={} timestamp={} window={}",
                    c.element().getKey(),
                    c.element().getValue(),
                    c.timestamp(),
                    window);
              }
            }));

    pipeline.run();
  }
}
