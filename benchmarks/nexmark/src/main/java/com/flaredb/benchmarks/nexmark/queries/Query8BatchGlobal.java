package com.flaredb.benchmarks.nexmark.queries;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.IdNameReserve;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryTransform;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryUtil;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Query 8 (Batch / Global Window): Monitor New Users.
 * Select people who have entered the system and created auctions in the batch dataset (Global Window).
 */
public class Query8BatchGlobal extends NexmarkQueryTransform<IdNameReserve> {
  private final NexmarkConfiguration configuration;

  public Query8BatchGlobal(NexmarkConfiguration configuration) {
    super("Query8");
    this.configuration = configuration;
  }

  @Override
  public PCollection<IdNameReserve> expand(PCollection<Event> events) {
    PCollection<KV<Long, Person>> personsById =
        events
            .apply(NexmarkQueryUtil.JUST_NEW_PERSONS)
            .apply("PersonById", NexmarkQueryUtil.PERSON_BY_ID);

    PCollection<KV<Long, Auction>> auctionsBySeller =
        events
            .apply(NexmarkQueryUtil.JUST_NEW_AUCTIONS)
            .apply("AuctionBySeller", NexmarkQueryUtil.AUCTION_BY_SELLER);

    return KeyedPCollectionTuple.of(NexmarkQueryUtil.PERSON_TAG, personsById)
        .and(NexmarkQueryUtil.AUCTION_TAG, auctionsBySeller)
        .apply(CoGroupByKey.create())
        .apply(
            name + ".Select",
            ParDo.of(
                new DoFn<KV<Long, CoGbkResult>, IdNameReserve>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    @Nullable Person person =
                        c.element().getValue().getOnly(NexmarkQueryUtil.PERSON_TAG, null);
                    if (person == null) {
                      return;
                    }
                    for (Auction auction :
                        c.element().getValue().getAll(NexmarkQueryUtil.AUCTION_TAG)) {
                      c.output(new IdNameReserve(person.id, person.name, auction.reserve));
                    }
                  }
                }));
  }
}
