package com.flaredb.example;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flaredb.runner.FlareRunner;

public class WordCount {
    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    public static void main(String[] args) {
        WordCountPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .as(WordCountPipelineOptions.class);

        options.setRunner(FlareRunner.class);
        options.setJobEndpoint("127.0.0.1:8099");
        options.setUberJar(
                "/home/ganesh/flare-db/sdf/flare-db/example/wordcount/target/wordcount-1.0-SNAPSHOT.jar");

        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.read().from("/home/ganesh/flare-db/sdf/flare-db/example/wordcount/para.txt"))
                .apply("Split lines into words", FlatMapElements.into(TypeDescriptors.strings())
                        .via(line -> Arrays.asList(line.split(" "))))
                .apply("Remove empty words", Filter.by(word -> !word.isEmpty()))
                .apply("Count occurrences", Count.perElement())
                .apply("Convert counts to strings", MapElements.into(TypeDescriptors.strings())
                        .via(kv -> kv.getKey() + ": " + kv.getValue()))
                .apply("Log results", ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void process(ProcessContext ctx) {
                        LOG.info("Element: " + ctx.element());
                    }
                }));

        p.run();
    }
}
