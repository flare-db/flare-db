package com.flaredb.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlattenExample {

       private static final Logger LOG = LoggerFactory.getLogger(FlattenExample.class);

    public static void main(String[] args) {

        FlattenExamplePipelineOptions options =
                FlattenExamplePipelineOptions.applyFlareDefaults(
                        PipelineOptionsFactory.fromArgs(args)
                                .as(FlattenExamplePipelineOptions.class));

        Pipeline pipeline = Pipeline.create(options);

        PCollection<Integer> first =
                pipeline.apply(
                        "CreateFirst",
                        Create.of(1, 2, 3));

        PCollection<Integer> second =
                pipeline.apply(
                        "CreateSecond",
                        Create.of(4, 5, 6));

        PCollection<Integer> third =
                pipeline.apply(
                        "CreateThird",
                        Create.of(7, 8, 9));

        PCollection<Integer> flattened =
                PCollectionList
                        .of(first)
                        .and(second)
                        .and(third)
                        .apply("Flatten", Flatten.pCollections());

        flattened.apply(
                "Print",
                ParDo.of(new DoFn<Integer, Void>() {

                    @ProcessElement
                    public void processElement(ProcessContext context) {
                        LOG.info("Element: {}", context.element());
                    }
                }));

        pipeline.run().waitUntilFinish();
    }
}
