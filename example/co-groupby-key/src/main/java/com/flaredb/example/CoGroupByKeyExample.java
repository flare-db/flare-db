package com.flaredb.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flaredb.runner.FlareRunner;


public class CoGroupByKeyExample {

        private static final Logger LOG = LoggerFactory.getLogger(CoGroupByKeyExample.class);

    public static void main(String[] args) {

        CoGroupByKeyPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .as(CoGroupByKeyPipelineOptions.class);

        options.setRunner(FlareRunner.class);
        options.setJobEndpoint("127.0.0.1:8099");
        options.setUberJar(
                "/home/ganesh/flare-db/clilogs/flare-db/example/wordcount/build/libs/wordcount-0.2.0-all.jar");

        Pipeline pipeline = Pipeline.create();

        TupleTag<Integer> leftTag = new TupleTag<>();
        TupleTag<String> rightTag = new TupleTag<>();

        PCollection<KV<String, Integer>> left =
                pipeline.apply(
                        "CreateLeft",
                        Create.of(
                                KV.of("a", 1),
                                KV.of("a", 2),
                                KV.of("b", 3)));

        PCollection<KV<String, String>> right =
                pipeline.apply(
                        "CreateRight",
                        Create.of(
                                KV.of("a", "x"),
                                KV.of("a", "y"),
                                KV.of("c", "z")));

        PCollection<KV<String, CoGbkResult>> result =
                KeyedPCollectionTuple
                        .of(leftTag, left)
                        .and(rightTag, right)
                        .apply(
                                "CoGroupByKey",
                                CoGroupByKey.create());

        result.apply(
                "Print",
                ParDo.of(new DoFn<KV<String, CoGbkResult>, Void>() {

                    @ProcessElement
                    public void processElement(ProcessContext context) {
                        KV<String, CoGbkResult> element = context.element();

                        LOG.info(
                                element.getKey()
                                        + " left=" + element.getValue().getAll(leftTag)
                                        + " right=" + element.getValue().getAll(rightTag));
                    }
                }));

        pipeline.run();
    }
}