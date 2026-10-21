package com.flaredb.testing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end exercise and regression check of FlareDB streaming execution model, driven by an
 * executable {@code TestStream}.
 *
 * <p>Instead of reading a file, the pipeline is scripted with explicit element and watermark
 * events. That makes every time-driven behavior deterministic: the same script always produces the
 * same panes. The script exercises <em>three</em> things the streaming work added:
 *
 * <ol>
 *   <li><b>Watermark-driven window readiness.</b> The first fixed window {@code [0s,10s)} only
 *       fires once the watermark passes its end, even though the aggregation could have run
 *       earlier.
 *   <li><b>Multiple panes per window.</b> An element arriving <em>after</em> the on-time firing
 *       produces a second, late pane — the trigger ("after watermark, with late firings") is now
 *       really evaluated across runs, not treated as an boolean.
 *   <li><b>Late data is not lost.</b> The late element is still aggregated into the window it
 *       belongs to (its event time), and counted exactly once.
 * </ol>
 *
 * <p>{@link #main} compares the recorded panes against the script and fails the run if they differ,
 * so a broken watermark, trigger, or re-run path fails the run instead of silently printing
 * nothing.
 *
 * <pre>{@code
 * ./gradlew :test-stream:shadowJar
 * ./gradlew :test-stream:run
 * }</pre>
 */
public class TestStreamExample {

  private static final Logger LOG = LoggerFactory.getLogger(TestStreamExample.class);

  // The fixed window is 10s wide; the watermark advances past its end to fire it on time.
  private static final Duration WINDOW_SIZE = Duration.standardSeconds(10);
  // Generous lateness so the late element produces a late pane rather than being expired.
  private static final Duration ALLOWED_LATENESS = Duration.standardSeconds(60);

  public static void main(String[] args) throws Exception {

    TestStreamPipelineOptions options =
        TestStreamPipelineOptions.applyFlareDefaults(
            PipelineOptionsFactory.fromArgs(args).as(TestStreamPipelineOptions.class));

    // Start from a clean slate so a stale file cannot make a broken run look OK.
    String outputFile = options.getOutputFile();
    Files.deleteIfExists(Paths.get(outputFile));
    if (Paths.get(outputFile).getParent() != null) {
      Files.createDirectories(Paths.get(outputFile).getParent());
    }

    Pipeline pipeline = Pipeline.create(options);

    // Script:
    //   t=1000  k1 -> "a"     (window [0,10s))
    //   t=2000  k1 -> "b"     (window [0,10s))
    //   t=1500  k2 -> "c"     (window [0,10s))
    //   watermark -> 10000    (window end = 9999, so this completes it on time)
    //   t=3000  k1 -> "late"  (late for [0,10s); must produce a late pane)
    //   watermark -> +inf     (finish the source)
    TestStream<KV<String, String>> stream =
        TestStream.<KV<String, String>>create(
                KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .addElements(TimestampedValue.of(KV.of("k1", "a"), new Instant(1_000)))
            .addElements(TimestampedValue.of(KV.of("k1", "b"), new Instant(2_000)))
            .addElements(TimestampedValue.of(KV.of("k2", "c"), new Instant(1_500)))
            .advanceWatermarkTo(new Instant(10_000))
            .addElements(TimestampedValue.of(KV.of("k1", "late"), new Instant(3_000)))
            .advanceWatermarkToInfinity();

    PCollection<KV<String, String>> elements = pipeline.apply(stream);

    PCollection<KV<String, String>> windowed =
        elements.apply(
            Window.<KV<String, String>>into(FixedWindows.of(WINDOW_SIZE))
                .triggering(
                    AfterWatermark.pastEndOfWindow()
                        .withLateFirings(AfterPane.elementCountAtLeast(1)))
                .withAllowedLateness(ALLOWED_LATENESS)
                .discardingFiredPanes());

    PCollection<KV<String, Iterable<String>>> grouped = windowed.apply(GroupByKey.create());

    grouped.apply("RecordPanes", ParDo.of(new RecordPanes(outputFile)));

    // FlareRunner.run blocks until the job completes, so the output file is fully
    // written when this returns.
    pipeline.run();

    verify(outputFile);
  }

  /**
   * Compares the recorded panes against the script, failing the run on any mismatch. Asserts the
   * behavioral properties (an on-time pane per key, a late pane for the late key, and no lost data)
   * rather than an exact pane count, which would be brittle as trigger semantics evolve.
   */
  private static void verify(String outputFile) throws IOException {
    Map<String, Set<String>> valuesByKey = new TreeMap<>();
    Map<String, Integer> onTimeByKey = new TreeMap<>();
    Map<String, Integer> lateByKey = new TreeMap<>();

    for (Pane pane : readPanes(outputFile)) {
      valuesByKey.computeIfAbsent(pane.key, k -> new TreeSet<>()).addAll(pane.values);
      if (pane.timing.equals("ON_TIME")) {
        onTimeByKey.merge(pane.key, 1, Integer::sum);
      } else if (pane.timing.equals("LATE")) {
        lateByKey.merge(pane.key, 1, Integer::sum);
      }
    }

    LOG.info("test-stream: observed panes by key: values={}", valuesByKey);
    LOG.info("test-stream: on-time panes={} late panes={}", onTimeByKey, lateByKey);

    Map<String, Set<String>> expected = new TreeMap<>();
    expected.put("k1", new TreeSet<>(Set.of("a", "b", "late")));
    expected.put("k2", new TreeSet<>(Set.of("c")));

    if (!expected.equals(valuesByKey)) {
      throw new IllegalStateException(
          "test-stream FAILED: aggregated values do not match the script."
              + "\n  expected="
              + expected
              + "\n  actual="
              + valuesByKey);
    }

    for (String key : expected.keySet()) {
      if (onTimeByKey.getOrDefault(key, 0) < 1) {
        throw new IllegalStateException(
            "test-stream FAILED: key '"
                + key
                + "' produced no on-time pane. onTime="
                + onTimeByKey);
      }
    }

    // The late element must fire a second, late pane for k1 — the core multi-pane check.
    if (lateByKey.getOrDefault("k1", 0) < 1) {
      throw new IllegalStateException(
          "test-stream FAILED: key 'k1' produced no late pane for the late element. late="
              + lateByKey);
    }

    LOG.info(
        "test-stream PASSED: {} key(s); k1 fired on-time + late ({} late pane(s)); no data lost",
        expected.size(),
        lateByKey.get("k1"));
  }

  /** One recorded pane: its key, timing, and the values it carried. */
  private record Pane(String key, String timing, Set<String> values) {}

  /**
   * Parses the {@code key|windowMax|timing|index|isLast|v1,v2} lines written by {@link
   * RecordPanes}.
   */
  private static java.util.List<Pane> readPanes(String outputFile) throws IOException {
    java.util.List<Pane> panes = new java.util.ArrayList<>();
    Path path = Paths.get(outputFile);
    if (!Files.exists(path)) {
      return panes;
    }
    for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
      if (line.isBlank()) {
        continue;
      }
      String[] fields = line.split("\\|", -1);
      if (fields.length < 6) {
        continue;
      }
      Set<String> values = new TreeSet<>();
      if (!fields[5].isBlank()) {
        for (String value : fields[5].split(",")) {
          values.add(value);
        }
      }
      panes.add(new Pane(fields[0], fields[2], values));
    }
    return panes;
  }

  /**
   * Records every pane {@code GroupByKey} emits, one line per pane, so the run can be verified
   * after the fact. A downstream stage is woken and re-run whenever the aggregation appends a new
   * pane.
   */
  private static class RecordPanes extends DoFn<KV<String, Iterable<String>>, Void> {

    private final String outputFile;

    RecordPanes(String outputFile) {
      this.outputFile = outputFile;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window, PaneInfo pane)
        throws IOException {
      StringBuilder values = new StringBuilder();
      for (String value : c.element().getValue()) {
        if (values.length() > 0) {
          values.append(',');
        }
        values.append(value);
      }
      String line =
          c.element().getKey()
              + "|"
              + window.maxTimestamp().getMillis()
              + "|"
              + pane.getTiming()
              + "|"
              + pane.getIndex()
              + "|"
              + pane.isLast()
              + "|"
              + values;
      LOG.info("pane: {}", line);

      Path target = Paths.get(outputFile);
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
