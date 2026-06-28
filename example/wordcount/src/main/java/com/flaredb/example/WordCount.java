package com.flaredb.example;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
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
        // FlareDB's endpoint
        options.setJobEndpoint("127.0.0.1:8099"); // default
        // path of your pipeline jar (set your jar path )
        options.setUberJar(
                "/home/ganesh/flare-db/example/flare-db/example/wordcount/target/wordcount-1.0-SNAPSHOT.jar");

        Pipeline p = Pipeline.create(options);

        // Thirukkural — Chapter 43: Arivudaimai (The Possession of Knowledge)
        // An ancient Tamil classic by the poet-saint Thiruvalluvar
        // Kurals 421–430
        p.apply("Create inputs", Create.of(
                "Wisdom is a weapon that guards against all woes a fort no foe can break " +
                        "Wisdom checks the straying senses expels evil and impels goodness " +
                        "To grasp the truth from everywhere from everyone is wisdom fair " +
                        "Speaking thoughts in clarity and reading subtle sense in others is wisdom "
                        +
                        "The wise befriend the world they bloom nor gloom equal in mind " +
                        "As moves the world so move the wise in tune with changing times and ways "
                        +
                        "The wise foresee what is to come the unwise lack in that wisdom " +
                        "Fear the frightful and act wisely not to fear the frightful is folly" +
                        "No frightful evil shocks the wise Who guard themselves against surprise"
                        +
                        "Who have wisdom they are all full Whatev'r they own, misfits are nil"))
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
