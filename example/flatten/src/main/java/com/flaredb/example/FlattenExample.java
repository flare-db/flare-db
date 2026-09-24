package com.flaredb.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.GroupByKey;
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

                            PCollection<KV<String, Integer>> inputA =
                                pipeline
                                    .apply("CreateA",
                                        Create.of(
                                            KV.of("a", 1),
                                            KV.of("b", 2)))
                                    .apply(
                                        "PrepareA",
                                        MapElements.into(
                                                TypeDescriptors.kvs(
                                                    TypeDescriptors.strings(),
                                                    TypeDescriptors.integers()))
                                            .via(kv -> KV.of(kv.getKey(), kv.getValue() * 10)));


                            PCollection<KV<String, Integer>> inputB =
                                pipeline
                                    .apply("CreateB",
                                        Create.of(
                                            KV.of("a", 3),
                                            KV.of("b", 4)))
                                    .apply(
                                        "PrepareB",
                                        MapElements.into(
                                                TypeDescriptors.kvs(
                                                    TypeDescriptors.strings(),
                                                    TypeDescriptors.integers()))
                                            .via(kv -> KV.of(kv.getKey(), kv.getValue() * 10)));


                            PCollection<KV<String, Integer>> inputC =
                                pipeline
                                    .apply("CreateC",
                                        Create.of(
                                            KV.of("a", 5),
                                            KV.of("b", 6)))
                                    .apply(
                                        "PrepareC",
                                        MapElements.into(
                                                TypeDescriptors.kvs(
                                                    TypeDescriptors.strings(),
                                                    TypeDescriptors.integers()))
                                            .via(kv -> KV.of(kv.getKey(), kv.getValue() * 10)));


                            PCollection<KV<String, Integer>> flattened =
                                PCollectionList.of(inputA)
                                    .and(inputB)
                                    .and(inputC)
                                    .apply("Flatten", Flatten.pCollections());


                            PCollection<KV<String, Iterable<Integer>>> grouped =
                                flattened.apply(
                                    "GBK",
                                    GroupByKey.create());

                            grouped.apply(
                                "Print",
                                ParDo.of(
                                    new DoFn<KV<String, Iterable<Integer>>, Void>() {

                                      @ProcessElement
                                      public void processElement(ProcessContext context) {
                                        KV<String, Iterable<Integer>> element = context.element();

                                        LOG.info(
                                            "key={} values={}",
                                            element.getKey(),
                                            element.getValue());
                                      }
                                    }));

                            pipeline.run();
    }
}
