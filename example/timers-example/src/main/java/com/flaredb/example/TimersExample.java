package com.flaredb.example;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.TreeMap;
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
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end exercise and regression check of FlareDB's user-state + timer support.
 *
 * <p>A stateful, keyed {@link DoFn} buffers each element into a {@code BagState} and arms a
 * <em>processing-time</em> timer. When the timer fires, the runner re-runs the stage as a
 * timer-only bundle; the {@code @OnTimer} callback reads the accumulated state, records {@code
 * key,count} and clears it. {@link #main} then compares the recorded per-key counts against counts
 * computed directly from the input file, so a broken timer, state, or re-run path fails the run
 * instead of silently printing nothing.
 *
 * <p>The {@code @OnTimer} callback writes the {@code key,count} lines to {@link
 * TimersPipelineOptions#getOutputFile()} itself, rather than emitting them downstream. A downstream
 * stage would have run (and completed) as soon as the first bundle produced its output, and FlareDB
 * does not yet re-run a completed consumer when an upstream stage is re-armed by a timer — that
 * needs the watermark holds / gating that land with windowed aggregation. Writing the file
 * in-process keeps this check focused on the timer, state, and re-run seam.
 *
 * <pre>{@code
 * ./gradlew :timers-example:shadowJar
 * ./gradlew :timers-example:run
 * }</pre>
 */
public class TimersExample {

  private static final Logger LOG = LoggerFactory.getLogger(TimersExample.class);

  public static void main(String[] args) throws Exception {

    TimersPipelineOptions options =
        TimersPipelineOptions.applyFlareDefaults(
            PipelineOptionsFactory.fromArgs(args).as(TimersPipelineOptions.class));

    // Start from a clean slate so a stale file cannot make a broken run look OK.
    String outputFile = options.getOutputFile();
    String eventFile = outputFile + ".event";
    Files.deleteIfExists(Paths.get(outputFile));
    Files.deleteIfExists(Paths.get(eventFile));
    for (String path : new String[] {outputFile, eventFile}) {
      if (Paths.get(path).getParent() != null) {
        Files.createDirectories(Paths.get(path).getParent());
      }
    }

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

    // Terminal stateful stage with a processing-time timer and an event-time
    // timer. On firing each records (key, count) / (key, event) and clears or
    // re-arms its state.
    records.apply(
        "BufferAndFireOnTimer", ParDo.of(new BufferAndFireOnTimer(outputFile, eventFile)));

    // FlareRunner.run blocks until the job completes, so the output files are
    // fully written when this returns.
    pipeline.run();

    verify(options.getInputFile(), outputFile, eventFile);
  }

  /**
   * Compares the recorded results against counts computed from the input, and fails the run if they
   * differ. The processing-time results must match the input exactly (one line per key); the
   * event-time timer must fire exactly twice per key (the reset firing plus one re-arm).
   */
  private static void verify(String inputFile, String outputFile, String eventFile)
      throws IOException {
    Map<String, Integer> expected = expectedCounts(inputFile);
    Map<String, Integer> actual = readFlushResults(outputFile);

    long expectedTotal = expected.values().stream().mapToLong(Integer::longValue).sum();
    long actualTotal = actual.values().stream().mapToLong(Integer::longValue).sum();

    LOG.info(
        "timers-example: expected keys={} total={}; flushed keys={} total={}",
        expected.size(),
        expectedTotal,
        actual.size(),
        actualTotal);

    if (!expected.equals(actual)) {
      throw new IllegalStateException(
          "timers-example FAILED: flushed per-key counts do not match the input."
              + "\n  expected="
              + expected
              + "\n  actual="
              + actual);
    }

    // The event-time timer fires at the bounded end (+inf) and re-arms once, so
    // every key must be recorded exactly twice: the drain after +inf is what
    // proves a re-armed event-time timer is not stranded.
    Map<String, Integer> eventFirings = countEventFirings(eventFile);
    for (String key : expected.keySet()) {
      int firings = eventFirings.getOrDefault(key, 0);
      if (firings != 2) {
        throw new IllegalStateException(
            "timers-example FAILED: event-time timer for key '"
                + key
                + " fired "
                + firings
                + " time(s), expected 2 (one plus one re-arm after +inf). firings="
                + eventFirings);
      }
    }
    if (!eventFirings.keySet().equals(expected.keySet())) {
      throw new IllegalStateException(
          "timers-example FAILED: event-time keys do not match the input. event="
              + eventFirings.keySet()
              + " expected="
              + expected.keySet());
    }

    LOG.info(
        "timers-example PASSED: {} keys each fired once (processing time, {} elements) and {} keys each fired twice (event time)",
        actual.size(),
        actualTotal,
        eventFirings.size());
  }

  /** Per-key number of event-time firings recorded by the pipeline. */
  private static Map<String, Integer> countEventFirings(String eventFile) throws IOException {
    Map<String, Integer> firings = new TreeMap<>();
    Path path = Paths.get(eventFile);
    if (!Files.exists(path)) {
      return firings;
    }
    for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
      if (line.isBlank()) {
        continue;
      }
      int comma = line.lastIndexOf(',');
      if (comma < 0) {
        continue;
      }
      firings.merge(line.substring(0, comma), 1, Integer::sum);
    }
    return firings;
  }

  /** Per-key element counts read directly from the input CSV (the expected results). */
  private static Map<String, Integer> expectedCounts(String inputFile) throws IOException {
    Map<String, Integer> counts = new TreeMap<>();
    for (String line : Files.readAllLines(Paths.get(inputFile), StandardCharsets.UTF_8)) {
      if (line.isEmpty() || line.startsWith("id,")) {
        continue;
      }
      String[] fields = line.split(",");
      if (fields.length < 2) {
        continue;
      }
      counts.merge(fields[1], 1, Integer::sum);
    }
    return counts;
  }

  /**
   * Per-key counts recorded by the pipeline. A key appearing more than once means the timer fired
   * twice, which is a bug even when the final count happens to match.
   */
  private static Map<String, Integer> readFlushResults(String outputFile) throws IOException {
    Map<String, Integer> counts = new TreeMap<>();
    Path path = Paths.get(outputFile);
    if (!Files.exists(path)) {
      return counts;
    }
    for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
      if (line.isBlank()) {
        continue;
      }
      int comma = line.lastIndexOf(',');
      if (comma < 0) {
        continue;
      }
      String key = line.substring(0, comma);
      int count = Integer.parseInt(line.substring(comma + 1).trim());
      if (counts.putIfAbsent(key, count) != null) {
        throw new IllegalStateException(
            "timers-example FAILED: key '" + key + "' fired more than once");
      }
    }
    return counts;
  }

  private static class BufferAndFireOnTimer extends DoFn<KV<String, String>, Void> {

    private final String outputFile;
    private final String eventFile;

    BufferAndFireOnTimer(String outputFile, String eventFile) {
      this.outputFile = outputFile;
      this.eventFile = eventFile;
    }

    @StateId("buffer")
    private final StateSpec<BagState<String>> bufferSpec = StateSpecs.bag(StringUtf8Coder.of());

    @StateId("eventArmed")
    private final StateSpec<BagState<String>> eventArmedSpec = StateSpecs.bag(StringUtf8Coder.of());

    @TimerId("flush")
    private final TimerSpec flushSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @TimerId("eventFlush")
    private final TimerSpec eventFlushSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void processElement(
        @Element KV<String, String> element,
        @StateId("buffer") BagState<String> buffer,
        @TimerId("flush") Timer flush,
        @TimerId("eventFlush") Timer eventFlush) {
      buffer.add(element.getValue());
      // Re-arm the processing-time timer 2 seconds from now; the last element
      // for a key wins, so the flush happens once the key goes quiet.
      //
      // `offset(...)` only *configures* the offset and returns the timer; a
      // terminal call (`setRelative()` here) is what actually arms it. Calling
      // `offset(...)` alone is a silent no-op.
      flush.offset(Duration.standardSeconds(2)).setRelative();

      // Arm an event-time timer at timestamp 0. With a bounded source the input
      // watermark only reaches it when the source reports +inf, so this fires at
      // the bounded end rather than on the wall clock.
      eventFlush.set(new Instant(0));

      LOG.info("buffered key={} value={}", element.getKey(), element.getValue());
    }

    @OnTimer("flush")
    public void onFlush(@Key String key, @StateId("buffer") BagState<String> buffer)
        throws IOException {
      int count = 0;
      for (String ignored : buffer.read()) {
        count++;
      }
      LOG.info("processing-time timer fired: key={} bufferedElements={}", key, count);

      appendLine(Paths.get(outputFile), key + "," + count);
      buffer.clear();
    }

    @OnTimer("eventFlush")
    public void onEventFlush(
        @Key String key,
        @StateId("eventArmed") BagState<String> eventArmed,
        @TimerId("eventFlush") Timer eventFlush)
        throws IOException {
      LOG.info("event-time timer fired: key={}", key);
      appendLine(Paths.get(eventFile), key + ",event");

      // Re-arm exactly once. The watermark is already +inf here, so the re-armed
      // timer is immediately due again and must be delivered (drained), not
      // stranded.
      if (eventArmed.isEmpty().read()) {
        eventArmed.add("rearmed");
        eventFlush.set(new Instant(0));
      }
    }

    private static void appendLine(Path target, String line) throws IOException {
      if (target.getParent() != null) {
        Files.createDirectories(target.getParent());
      }
      Files.write(
          target,
          (line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    }
  }
}
