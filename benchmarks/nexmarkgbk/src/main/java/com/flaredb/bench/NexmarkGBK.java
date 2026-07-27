package com.flaredb.bench;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.sources.generator.Generator;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.flaredb.runner.FlareRunner;

/**
 * Nexmark GroupByKey Benchmark Pipeline.
 *
 * <pre>
 *  Driver (Pre-computation)
 * ┌──────────────────────────────────┐
 * │ Deterministically replay Nexmark │
 * │ generator to compute expected    │
 * │ grouped results for validation   │
 * └──────────────┬───────────────────┘
 *                │
 *                │ Expected Groups
 *                ▼
 *
 * ┌─────────────────────────────┐
 * │         Impulse             │
 * └──────────────┬──────────────┘
 *                │
 *                ▼
 * ┌─────────────────────────────┐
 * │ Generate Nexmark Events     │
 * │         (ParDo)             │
 * └──────────────┬──────────────┘
 *                │
 *                ▼
 * ┌─────────────────────────────┐
 * │ Filter Bid Events           │
 * │    event.bid != null        │
 * └──────────────┬──────────────┘
 *                │
 *                ▼
 * ┌─────────────────────────────┐
 * │ Map to KV                   │
 * │ KV&lt;auctionId, bidPrice&gt;     │
 * └──────────────┬──────────────┘
 *                │
 *                ▼
 * ┌─────────────────────────────┐
 * │        GroupByKey           │
 * └──────────────┬──────────────┘
 *                │
 *       ┌────────┴────────┐
 *       ▼                  ▼
 * Validate Per-Auction    Validate Completeness
 * • Bid count             • No missing groups
 * • Total bid value       • No duplicate groups
 * • Min / Max price       • Expected group count
 * • Hash signature
 *       │                  │
 *       └────────┬─────────┘
 *                ▼
 * ┌─────────────────────────────┐
 * │     Benchmark Results       │
 * │ • Runtime                   │
 * │ • Throughput                │
 * │ • Validation Status         │
 * └─────────────────────────────┘
 * </pre>
 */
public class NexmarkGBK {
    private static final Logger LOG = LoggerFactory.getLogger(NexmarkGBK.class);

    //  Data classes

    private static class AuctionStats implements Serializable {
        long bidCount;
        long totalBidValue;
        long minPrice = Long.MAX_VALUE;
        long maxPrice = Long.MIN_VALUE;

        void addBid(long price) {
            bidCount++;
            totalBidValue += price;
            minPrice = Math.min(minPrice, price);
            maxPrice = Math.max(maxPrice, price);
        }

        long avgPrice()     { return bidCount > 0 ? totalBidValue / bidCount : 0; }
        long safeMinPrice() { return bidCount > 0 ? minPrice : 0; }
        long safeMaxPrice() { return bidCount > 0 ? maxPrice : 0; }
    }

    private static class ValidationSignature implements Serializable {
        long bidCount;
        long totalBidValue;
        long minPrice = Long.MAX_VALUE;
        long maxPrice = Long.MIN_VALUE;
        long xorHash;
        long sumHash;

        void addBid(long price) {
            bidCount++;
            totalBidValue += price;
            minPrice = Math.min(minPrice, price);
            maxPrice = Math.max(maxPrice, price);
            long mixed = mix64(price);
            xorHash ^= mixed;
            sumHash += mixed;
        }

        long safeMinPrice() { return bidCount > 0 ? minPrice : 0; }
        long safeMaxPrice() { return bidCount > 0 ? maxPrice : 0; }
    }

    private static class ExpectedAuctionData implements Serializable {
        final AuctionStats stats = new AuctionStats();
        final ValidationSignature signature = new ValidationSignature();

        void addBid(long price) {
            stats.addBid(price);
            signature.addBid(price);
        }
    }

    //  Main

    public static void main(String[] args) {
        NexmarkGBKPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .as(NexmarkGBKPipelineOptions.class);

        options.setRunner(FlareRunner.class);
        options.setJobEndpoint("127.0.0.1:8099");
        options.setUberJar(
                "/home/ganesh/flaredb-bench/real/flare-db/benchmarks/nexmarkgbk/target/nexmarkgbk-1.0-SNAPSHOT.jar");

        Pipeline p = Pipeline.create(options);
        NexmarkUtils.setupPipeline(NexmarkUtils.CoderStrategy.HAND, p);

        int numEvents = options.getNumEvents();
        long baseTime = Instant.parse("2015-07-15T00:00:00.000Z").getMillis();

        LOG.info("Nexmark GBK benchmark: {} events", numEvents);

        // pre-compute expected grouped results
        Map<Long, ExpectedAuctionData> expected = computeExpectedGroups(numEvents, baseTime);

        // Pipeline
        PCollection<KV<Long, Long>> bids = p
                .apply("Impulse", Impulse.create())
                .apply("Generate", ParDo.of(new GenerateEvents(numEvents, baseTime)))
                .apply("Filter", Filter.by(e -> e.bid != null))
                .apply("MapToKV", ParDo.of(new DoFn<Event, KV<Long, Long>>() {
                    @ProcessElement
                    public void process(ProcessContext ctx) {
                        Bid bid = ctx.element().bid;
                        ctx.output(KV.of(bid.auction, bid.price));
                    }
                }));

        PCollection<KV<Long, Iterable<Long>>> grouped = bids.apply("GroupByKey", GroupByKey.create());

        // Phase 1 — validate every auction group & record GBK timing.
        PCollection<Long> validatedIds = grouped.apply("ValidatePerAuction",
                ParDo.of(new ValidatePerAuction(numEvents, baseTime)));

        // Phase 2 — collect all validated IDs and check completeness.
        validatedIds
                .apply("CollectIDs", ParDo.of(new DoFn<Long, KV<Long, Long>>() {
                    @ProcessElement
                    public void process(ProcessContext ctx) {
                        ctx.output(KV.of(0L, ctx.element()));
                    }
                }))
                .apply("CollectGroupByKey", GroupByKey.create())
                .apply("ValidateCompleteness", ParDo.of(
                        new ValidateCompleteness(numEvents, baseTime)));

        //Run & report
        long startNanos = System.nanoTime();
        p.run();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        double throughput = elapsedMs > 0 ? (numEvents * 1000.0) / elapsedMs : 0.0;

        // Compute GBK-specific statistics from the pre-computed expected data.
        long totalBidEvents = 0;
        long minGroupSize = Long.MAX_VALUE;
        long maxGroupSize = Long.MIN_VALUE;
        for (ExpectedAuctionData d : expected.values()) {
            totalBidEvents += d.stats.bidCount;
            minGroupSize = Math.min(minGroupSize, d.stats.bidCount);
            maxGroupSize = Math.max(maxGroupSize, d.stats.bidCount);
        }
        double avgGroupSize = (double) totalBidEvents / expected.size();

        String table = buildBenchmarkTable(numEvents, elapsedMs, throughput,
                expected, totalBidEvents, minGroupSize, maxGroupSize, avgGroupSize);
        Path outputPath = writeBenchmarkTable(options.getBenchmarkOutputPath(), table);
        LOG.info("Benchmark summary written to {}", outputPath.toAbsolutePath());
    }

    //  DoFns

    /** Generates all Nexmark events from a single Impulse seed. */
    private static class GenerateEvents extends DoFn<byte[], Event> {
        private final int numEvents;
        private final long baseTime;

        GenerateEvents(int numEvents, long baseTime) {
            this.numEvents = numEvents;
            this.baseTime = baseTime;
        }

        @ProcessElement
        public void process(ProcessContext ctx) {
            LOG.info("Generating {} Nexmark events in-process...", numEvents);

            NexmarkConfiguration config = new NexmarkConfiguration();
            config.numEvents = (long) numEvents;
            GeneratorConfig genConfig = new GeneratorConfig(config, baseTime, 0, numEvents, 0);
            Generator generator = new Generator(genConfig);

            long emitted = 0;
            while (generator.hasNext()) {
                ctx.output(generator.next().getValue());
                emitted++;
            }

            LOG.info("Successfully generated {} events.", emitted);
        }
    }

    /** Validates every per-auction group against the expected data. */
    private static class ValidatePerAuction
            extends DoFn<KV<Long, Iterable<Long>>, Long> {

        private final int numEvents;
        private final long baseTime;
        private transient Map<Long, ExpectedAuctionData> expected;

        // GBK timing counters
        private long totalProcessingNs;
        private long maxGroupNs;
        private long groupsProcessed;

        ValidatePerAuction(int numEvents, long baseTime) {
            this.numEvents = numEvents;
            this.baseTime = baseTime;
        }

        @Setup
        public void setup() {
            expected = computeExpectedGroups(numEvents, baseTime);
        }

        @FinishBundle
        public void finishBundle(FinishBundleContext ctx) {
            if (groupsProcessed > 0) {
                long avgNs = totalProcessingNs / groupsProcessed;
                LOG.info("GBK timing for this bundle: {} groups, total={}ms avg={}ms max={}ms",
                        groupsProcessed,
                        TimeUnit.NANOSECONDS.toMillis(totalProcessingNs),
                        TimeUnit.NANOSECONDS.toMillis(avgNs),
                        TimeUnit.NANOSECONDS.toMillis(maxGroupNs));
                totalProcessingNs = 0;
                maxGroupNs = 0;
                groupsProcessed = 0;
            }
        }

        @ProcessElement
        public void process(ProcessContext ctx) {
            long startNs = System.nanoTime();

            KV<Long, Iterable<Long>> element = ctx.element();
            long auctionId = element.getKey();

            ExpectedAuctionData exp = expected.get(auctionId);
            if (exp == null) {
                throw new IllegalStateException(
                        "Unexpected output group for auction " + auctionId);
            }

            AuctionStats actual = new AuctionStats();
            ValidationSignature actualSig = new ValidationSignature();
            for (Long price : element.getValue()) {
                actual.addBid(price);
                actualSig.addBid(price);
            }

            check("bid count",   auctionId, exp.stats.bidCount,       actual.bidCount);
            check("total value", auctionId, exp.stats.totalBidValue,  actual.totalBidValue);
            check("min price",   auctionId, exp.stats.safeMinPrice(), actual.safeMinPrice());
            check("max price",   auctionId, exp.stats.safeMaxPrice(), actual.safeMaxPrice());
            check("sig xorHash", auctionId, exp.signature.xorHash,    actualSig.xorHash);
            check("sig sumHash", auctionId, exp.signature.sumHash,    actualSig.sumHash);

            long elapsed = System.nanoTime() - startNs;
            totalProcessingNs += elapsed;
            maxGroupNs = Math.max(maxGroupNs, elapsed);
            groupsProcessed++;

            ctx.output(auctionId);
        }

        private static void check(String field, long id, long exp, long act) {
            if (exp != act) {
                throw new IllegalStateException(
                        field + " mismatch for auction " + id
                                + ": expected=" + exp + " actual=" + act);
            }
        }
    }

    /** Completeness check: no missing, duplicate, or wrong-count groups. */
    private static class ValidateCompleteness
            extends DoFn<KV<Long, Iterable<Long>>, Void> {

        private final int numEvents;
        private final long baseTime;
        private transient Map<Long, ExpectedAuctionData> expected;

        ValidateCompleteness(int numEvents, long baseTime) {
            this.numEvents = numEvents;
            this.baseTime = baseTime;
        }

        @Setup
        public void setup() {
            expected = computeExpectedGroups(numEvents, baseTime);
        }

        @ProcessElement
        public void process(ProcessContext ctx) {
            Set<Long> seen = new HashSet<>();
            Set<Long> duplicates = new HashSet<>();

            for (Long auctionId : ctx.element().getValue()) {
                if (!seen.add(auctionId)) {
                    duplicates.add(auctionId);
                }
            }

            if (!duplicates.isEmpty()) {
                throw new IllegalStateException(
                        "Duplicate output groups for auctions " + duplicates);
            }

            Set<Long> missing = new HashSet<>(expected.keySet());
            missing.removeAll(seen);
            if (!missing.isEmpty()) {
                throw new IllegalStateException(
                        "Missing output groups for auctions " + missing);
            }

            if (seen.size() != expected.size()) {
                throw new IllegalStateException(String.format(
                        "Group count mismatch: expected=%d actual=%d",
                        expected.size(), seen.size()));
            }
        }
    }

    //  Pre-computation

    private static Map<Long, ExpectedAuctionData> computeExpectedGroups(
            int numEvents, long baseTime) {
        NexmarkConfiguration config = new NexmarkConfiguration();
        config.numEvents = (long) numEvents;
        GeneratorConfig genConfig = new GeneratorConfig(config, baseTime, 0, numEvents, 0);
        Generator generator = new Generator(genConfig);

        Map<Long, ExpectedAuctionData> groups = new TreeMap<>();
        while (generator.hasNext()) {
            Event event = generator.next().getValue();
            if (event.bid != null) {
                Bid bid = event.bid;
                groups.computeIfAbsent(bid.auction, k -> new ExpectedAuctionData())
                        .addBid(bid.price);
            }
        }
        return groups;
    }


    private static long mix64(long value) {
        long z = value + 0x9e3779b97f4a7c15L;
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    //  Benchmark output

    private static String buildBenchmarkTable(
            int numEvents, long elapsedMs, double throughput,
            Map<Long, ExpectedAuctionData> expected,
            long totalBidEvents, long minGroupSize, long maxGroupSize,
            double avgGroupSize) {

        StringBuilder b = new StringBuilder();
        b.append("Nexmark GBK Benchmark Summary\n");
        b.append("============================\n");
        b.append(String.format("%-24s | %s%n",   "Total Events",        numEvents));
        b.append(String.format("%-24s | %s%n",   "Bid Events",          totalBidEvents));
        b.append(String.format("%-24s | %d%n",   "Runtime (ms)",        elapsedMs));
        b.append(String.format("%-24s | %.2f%n", "Throughput (evt/s)",  throughput));
        b.append('\n');
        b.append("--- GroupByKey Metrics ---\n");
        b.append(String.format("%-24s | %d%n",   "Auction Groups",      expected.size()));
        b.append(String.format("%-24s | %d%n",   "Min Group Size",      minGroupSize));
        b.append(String.format("%-24s | %d%n",   "Max Group Size",      maxGroupSize));
        b.append(String.format("%-24s | %.1f%n", "Avg Group Size",      avgGroupSize));
        b.append('\n');
        b.append(String.format(
                "%-12s | %-10s | %-12s | %-10s | %-10s | %-14s%n",
                "Auction", "Bid Count", "Avg Price", "Min Price", "Max Price", "Total Bid Value"));
        b.append(String.format(
                "%-12s-+-%-10s-+-%-12s-+-%-10s-+-%-10s-+-%-14s%n",
                repeat('-', 12), repeat('-', 10), repeat('-', 12),
                repeat('-', 10), repeat('-', 10), repeat('-', 14)));
        for (Map.Entry<Long, ExpectedAuctionData> entry : expected.entrySet()) {
            AuctionStats s = entry.getValue().stats;
            b.append(String.format(
                    "%-12d | %-10d | %-12d | %-10d | %-10d | %-14d%n",
                    entry.getKey(), s.bidCount, s.avgPrice(),
                    s.safeMinPrice(), s.safeMaxPrice(), s.totalBidValue));
        }
        b.append('\n');
        b.append("Note: results are computed deterministically from the Nexmark generator. "
                + "Runtime measured until FlareRunner.run() returns. "
                + "GBK timing is logged per bundle during execution.");
        return b.toString();
    }

    private static Path writeBenchmarkTable(String outputPath, String table) {
        Path path = Paths.get(outputPath);
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(path, table, StandardCharsets.UTF_8);
            return path;
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to write benchmark summary to " + path.toAbsolutePath(), e);
        }
    }

    private static String repeat(char ch, int count) {
        StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sb.append(ch);
        }
        return sb.toString();
    }

}
