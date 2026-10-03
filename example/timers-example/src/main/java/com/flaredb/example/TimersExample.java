package com.flaredb.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end exercise of FlareDB's user-state + timer support.
 *
 * <p>A stateful, keyed {@link DoFn} buffers each element into a {@code BagState} and arms a
 * <em>processing-time</em> timer. When the timer fires, the runner re-runs the stage as a
 * timer-only bundle; the {@code @OnTimer} callback reads the accumulated state and clears it. If
 * timers, state, or the re-run seam are broken, no "timer fired" line is printed.
 */
public class TimersExample {

  private static final Logger LOG = LoggerFactory.getLogger(TimersExample.class);

  public static void main(String[] args) {

    TimersPipelineOptions options =
        TimersPipelineOptions.applyFlareDefaults(
            PipelineOptionsFactory.fromArgs(args).as(TimersPipelineOptions.class));

    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> lines =
        pipeline.apply("ReadCSV", TextIO.read().from(options.getInputFile()));

    PCollection<KV<String, String>> records =
        lines.apply(
            "ParseCSV",
            ParDo.of(
                new DoFn<String, KV<String, String>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    String line = c.element();

                    // Skip header.
                    if (line.startsWith("id,")) {
                      return;
                    }

                    String[] fields = line.split(",");
                    // Key by name so state and the timer are per-name.
                    c.output(KV.of(fields[1], fields[1]));
                  }
                }));

    // Stateful stage with a processing-time timer. It is terminal: the timer
    // callback's effect is the log line, so no downstream stage has to re-run.
    records.apply("BufferAndFireOnTimer", ParDo.of(new BufferAndFireOnTimer()));

    pipeline.run();
  }

  private static class BufferAndFireOnTimer extends DoFn<KV<String, String>, Void> {

    @StateId("buffer")
    private final StateSpec<BagState<String>> bufferSpec = StateSpecs.bag(StringUtf8Coder.of());

    @TimerId("flush")
    private final TimerSpec flushSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void processElement(
        @Element KV<String, String> element,
        @StateId("buffer") BagState<String> buffer,
        @TimerId("flush") Timer flush) {
      buffer.add(element.getValue());
      // Re-arm the processing-time timer 2 seconds from now; the last element
      // for a key wins, so the flush happens once the key goes quiet.
      //
      // `offset(...)` only *configures* the offset and returns the timer; a
      // terminal call (`setRelative()` here) is what actually arms it. Calling
      // `offset(...)` alone is a silent no-op.
      flush.offset(Duration.standardSeconds(2)).setRelative();
      LOG.info("buffered key={} value={}", element.getKey(), element.getValue());
    }

    @OnTimer("flush")
    public void onFlush(@Key String key, @StateId("buffer") BagState<String> buffer) {
      int count = 0;
      for (String ignored : buffer.read()) {
        count++;
      }
      LOG.info("processing-time timer fired: key={} bufferedElements={}", key, count);
      buffer.clear();
    }
  }
}
