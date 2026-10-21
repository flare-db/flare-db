package com.flaredb.benchmarks.nexmark.queries;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkQueryName;
import org.apache.beam.sdk.nexmark.queries.BoundedSideInputJoin;
import org.apache.beam.sdk.nexmark.queries.NexmarkQuery;
import org.apache.beam.sdk.nexmark.queries.Query0;
import org.apache.beam.sdk.nexmark.queries.Query1;
import org.apache.beam.sdk.nexmark.queries.Query10;
import org.apache.beam.sdk.nexmark.queries.Query11;
import org.apache.beam.sdk.nexmark.queries.Query12;
import org.apache.beam.sdk.nexmark.queries.Query13;
import org.apache.beam.sdk.nexmark.queries.Query14;
import org.apache.beam.sdk.nexmark.queries.Query2;
import org.apache.beam.sdk.nexmark.queries.Query3;
import org.apache.beam.sdk.nexmark.queries.Query4;
import org.apache.beam.sdk.nexmark.queries.Query5;
import org.apache.beam.sdk.nexmark.queries.Query6;
import org.apache.beam.sdk.nexmark.queries.Query7;
import org.apache.beam.sdk.nexmark.queries.Query9;
import org.apache.beam.sdk.nexmark.queries.SessionSideInputJoin;

/** Registry of Nexmark queries available through the batch launcher. */
public class BatchQueryRegistry {

  public static Map<NexmarkQueryName, NexmarkQuery<?>> createBatchQueries(
      NexmarkConfiguration configuration) {
    Map<NexmarkQueryName, NexmarkQuery<?>> queries = new LinkedHashMap<>();

    // Query 0: PASSTHROUGH
    queries.put(NexmarkQueryName.PASSTHROUGH, new NexmarkQuery<>(configuration, new Query0()));

    // Query 1: CURRENCY_CONVERSION
    queries.put(
        NexmarkQueryName.CURRENCY_CONVERSION,
        new NexmarkQuery<>(configuration, new Query1(configuration)));

    // Query 2: SELECTION
    queries.put(
        NexmarkQueryName.SELECTION, new NexmarkQuery<>(configuration, new Query2(configuration)));

    // Query 3: LOCAL_ITEM_SUGGESTION (Batch join in GlobalWindow)
    queries.put(
        NexmarkQueryName.LOCAL_ITEM_SUGGESTION,
        new NexmarkQuery<>(configuration, new Query3(configuration)));

    // Query 4: AVERAGE_PRICE_FOR_CATEGORY
    queries.put(
        NexmarkQueryName.AVERAGE_PRICE_FOR_CATEGORY,
        new NexmarkQuery<>(configuration, new Query4(configuration)));

    // Query 5: HOT_ITEMS
    queries.put(
        NexmarkQueryName.HOT_ITEMS, new NexmarkQuery<>(configuration, new Query5(configuration)));

    // Query 6: AVERAGE_SELLING_PRICE_BY_SELLER
    queries.put(
        NexmarkQueryName.AVERAGE_SELLING_PRICE_BY_SELLER,
        new NexmarkQuery<>(configuration, new Query6(configuration)));

    // Query 7: HIGHEST_BID
    queries.put(
        NexmarkQueryName.HIGHEST_BID, new NexmarkQuery<>(configuration, new Query7(configuration)));

    // Query 8: MONITOR_NEW_USERS (Batch Global Window join)
    queries.put(
        NexmarkQueryName.MONITOR_NEW_USERS,
        new NexmarkQuery<>(configuration, new Query8BatchGlobal(configuration)));

    // Query 9: WINNING_BIDS
    queries.put(
        NexmarkQueryName.WINNING_BIDS,
        new NexmarkQuery<>(configuration, new Query9(configuration)));

    // Query 10: LOG_TO_SHARDED_FILES
    Query10 shardedLogs = new Query10(configuration);
    shardedLogs.setMaxNumWorkers(1);
    queries.put(
        NexmarkQueryName.LOG_TO_SHARDED_FILES, new NexmarkQuery<>(configuration, shardedLogs));

    // Query 11: USER_SESSIONS
    queries.put(
        NexmarkQueryName.USER_SESSIONS,
        new NexmarkQuery<>(configuration, new Query11(configuration)));

    // Query 12: PROCESSING_TIME_WINDOWS
    queries.put(
        NexmarkQueryName.PROCESSING_TIME_WINDOWS,
        new NexmarkQuery<>(configuration, new Query12(configuration)));

    // Query 13: PORTABILITY_BATCH
    queries.put(
        NexmarkQueryName.PORTABILITY_BATCH,
        new NexmarkQuery<>(configuration, new Query13(configuration)));

    // Query 14: RESHUFFLE
    queries.put(
        NexmarkQueryName.RESHUFFLE, new NexmarkQuery<>(configuration, new Query14(configuration)));

    // Bounded Side Input Join (Stream/Batch enrichment join)
    queries.put(
        NexmarkQueryName.BOUNDED_SIDE_INPUT_JOIN,
        new NexmarkQuery<>(configuration, new BoundedSideInputJoin(configuration)));

    // Session Side Input Join
    queries.put(
        NexmarkQueryName.SESSION_SIDE_INPUT_JOIN,
        new NexmarkQuery<>(configuration, new SessionSideInputJoin(configuration)));

    return queries;
  }
}
