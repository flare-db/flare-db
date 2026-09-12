package com.flaredb.benchmarks.nexmark.queries;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkQueryName;
import org.apache.beam.sdk.nexmark.queries.BoundedSideInputJoin;
import org.apache.beam.sdk.nexmark.queries.NexmarkQuery;
import org.apache.beam.sdk.nexmark.queries.Query0;
import org.apache.beam.sdk.nexmark.queries.Query1;
import org.apache.beam.sdk.nexmark.queries.Query2;
import org.apache.beam.sdk.nexmark.queries.Query3;
import org.apache.beam.sdk.nexmark.queries.Query7;
import org.apache.beam.sdk.nexmark.queries.Query9;

/**
 * Registry of batch-based Nexmark queries operating in the default Global Window.
 */
public class BatchQueryRegistry {

    public static Map<NexmarkQueryName, NexmarkQuery<?>> createBatchQueries(NexmarkConfiguration configuration) {
        Map<NexmarkQueryName, NexmarkQuery<?>> queries = new LinkedHashMap<>();

        // Query 0: PASSTHROUGH
        queries.put(NexmarkQueryName.PASSTHROUGH,
            new NexmarkQuery<>(configuration, new Query0()));

        // Query 1: CURRENCY_CONVERSION
        queries.put(NexmarkQueryName.CURRENCY_CONVERSION,
            new NexmarkQuery<>(configuration, new Query1(configuration)));

        // Query 2: SELECTION
        queries.put(NexmarkQueryName.SELECTION,
            new NexmarkQuery<>(configuration, new Query2(configuration)));

        // Query 3: LOCAL_ITEM_SUGGESTION (Batch join in GlobalWindow)
        queries.put(NexmarkQueryName.LOCAL_ITEM_SUGGESTION,
            new NexmarkQuery<>(configuration, new Query3(configuration)));

        // Query 7: HIGHEST_BID
        queries.put(NexmarkQueryName.HIGHEST_BID,
            new NexmarkQuery<>(configuration, new Query7(configuration)));

        // Query 8: MONITOR_NEW_USERS (Batch Global Window join)
        queries.put(NexmarkQueryName.MONITOR_NEW_USERS,
            new NexmarkQuery<>(configuration, new Query8BatchGlobal(configuration)));

        // Query 9: WINNING_BIDS
        queries.put(NexmarkQueryName.WINNING_BIDS,
            new NexmarkQuery<>(configuration, new Query9(configuration)));

        // Bounded Side Input Join (Stream/Batch enrichment join)
        queries.put(NexmarkQueryName.BOUNDED_SIDE_INPUT_JOIN,
            new NexmarkQuery<>(configuration, new BoundedSideInputJoin(configuration)));

        return queries;
    }
}
