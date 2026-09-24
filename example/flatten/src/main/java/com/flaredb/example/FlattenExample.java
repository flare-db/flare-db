package com.flaredb.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlattenExample {

  private static final Logger LOG = LoggerFactory.getLogger(FlattenExample.class);

  public static void main(String[] args) {

    FlattenExamplePipelineOptions options =
        FlattenExamplePipelineOptions.applyFlareDefaults(
            PipelineOptionsFactory.fromArgs(args).as(FlattenExamplePipelineOptions.class));
    Pipeline pipeline = Pipeline.create(options);

    PCollection<KV<String, Integer>> storeSales =
        pipeline.apply("StoreSales", Create.of(KV.of("apple", 10), KV.of("banana", 20)));

    PCollection<KV<String, Integer>> onlineSales =
        pipeline.apply("OnlineSales", Create.of(KV.of("apple", 15), KV.of("banana", 25)));

    PCollection<KV<String, Integer>> partnerSales =
        pipeline.apply("PartnerSales", Create.of(KV.of("apple", 5), KV.of("banana", 10)));

    PCollection<KV<String, Integer>> allSales =
        PCollectionList.of(storeSales)
            .and(onlineSales)
            .and(partnerSales)
            .apply("FlattenSales", Flatten.pCollections());

    PCollection<KV<String, Iterable<Integer>>> salesByProduct =
        allSales.apply("GroupByProduct", GroupByKey.create());

    salesByProduct.apply(
        "Print",
        ParDo.of(
            new DoFn<KV<String, Iterable<Integer>>, Void>() {
              @ProcessElement
              public void processElement(ProcessContext context) {
                KV<String, Iterable<Integer>> element = context.element();

                LOG.info("product={} sales={}", element.getKey(), element.getValue());
              }
            }));

    pipeline.run();
  }
}
