package com.flaredb.io;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.arrow.ArrowConversion;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.value.AutoValue;

/**
 * {@link PTransform}s for reading and writing data to/from <a href="https://www.flare-db.com/">FlareDB</a>
 *
 * <h2>Reading from FlareDB</h2>
 *
 * <pre>{@code
 * PCollection<Row> rows = pipeline.apply(
 *     FlareDbIO.read()
 *         .fromQuery("SELECT * FROM flare.default.my_table"));
 *         .withDbUrl("grpc://localhost:8099")
 * }</pre>
 *
 * <h2>Writing to FlareDB</h2>
 *
 * <pre>{@code
 * rows.apply(
 *     FlareDbIO.<Row>write()
 *         .to("flare.default.my_table"));
 *         .withDbUrl("grpc://localhost:8099")
 * }</pre>
 *
 */
public class FlareDbIO {

  private static final Logger LOG = LoggerFactory.getLogger(FlareDbIO.class);
  public static final String DEFAULT_DB_URL = "grpc://localhost:8099";

  /** Create a read transform for FlareDB. */
  public static Read read() {
    return new AutoValue_FlareDbIO_Read.Builder().setDbUrl(DEFAULT_DB_URL).build();
  }

  /** Like {@link #read()} but returns Beam Rows formatted for schema-aware row processing. */
  public static Read readTableRows() {
    return read();
  }

  /** Create a write transform for FlareDB. */
  public static <T> Write<T> write() {
    return new AutoValue_FlareDbIO_Write.Builder<T>()
        .setDbUrl(DEFAULT_DB_URL)
        .setBatchSize(1024)
        .build();
  }

  /** Helper to parse a database URL into host and port. */
  static ConnectionParams parseDbUrl(String dbUrl) {
    String raw = dbUrl.trim();
    if (raw.startsWith("grpc://")) {
      raw = raw.substring(7);
    } else if (raw.startsWith("http://")) {
      raw = raw.substring(7);
    } else if (raw.startsWith("https://")) {
      raw = raw.substring(8);
    }

    String host = "localhost";
    int port = 8099;
    if (raw.contains(":")) {
      String[] parts = raw.split(":");
      host = parts[0];
      port = Integer.parseInt(parts[1]);
    } else if (!raw.isEmpty()) {
      host = raw;
    }
    return new ConnectionParams(host, port);
  }

  static FlightClient createClient(BufferAllocator allocator, ConnectionParams params) {
    Location location = Location.forGrpcInsecure(params.host, params.port);
    return FlightClient.builder(allocator, location).build();
  }

  static class ConnectionParams implements Serializable {
    private static final long serialVersionUID = 1L;
    final String host;
    final int port;

    ConnectionParams(String host, int port) {
      this.host = host;
      this.port = port;
    }
  }

  static class SerializableEndpoint implements Serializable {
    private static final long serialVersionUID = 1L;
    private final byte[] ticketBytes;
    private final @Nullable String host;
    private final int port;

    SerializableEndpoint(byte[] ticketBytes, @Nullable String host, int port) {
      this.ticketBytes = ticketBytes;
      this.host = host;
      this.port = port;
    }

    static SerializableEndpoint fromFlightEndpoint(
        FlightEndpoint endpoint, String defaultHost, int defaultPort) {
      byte[] ticket = endpoint.getTicket().getBytes();
      List<Location> locations = endpoint.getLocations();
      if (locations != null && !locations.isEmpty()) {
        URI uri = locations.get(0).getUri();
        return new SerializableEndpoint(ticket, uri.getHost(), uri.getPort());
      }
      return new SerializableEndpoint(ticket, defaultHost, defaultPort);
    }

    Ticket getTicket() {
      return new Ticket(ticketBytes);
    }

    String getHost(String defaultHost) {
      return host != null ? host : defaultHost;
    }

    int getPort(int defaultPort) {
      return port > 0 ? port : defaultPort;
    }
  }

  // READ

  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Row>> {

    abstract String dbUrl();

    abstract @Nullable String query();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDbUrl(String dbUrl);

      abstract Builder setQuery(String query);

      abstract Read build();
    }

    /** Sets the FlareDB URL (e.g. {@code "grpc://localhost:8099"}). */
    public Read withDbUrl(String dbUrl) {
      return builder().setDbUrl(dbUrl).build();
    }

    /** Sets the SQL query to execute on FlareDB (e.g. {@code "SELECT * FROM flare.default.my_table"}). */
    public Read fromQuery(String query) {
      return builder().setQuery(query).build();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      checkArgument(query() != null, "fromQuery() is required");

      ConnectionParams params = parseDbUrl(dbUrl());
      Schema beamSchema;
      try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
          FlightClient client = createClient(allocator, params)) {
        FlightSqlClient sqlClient = new FlightSqlClient(client);
        FlightInfo info = sqlClient.execute(checkNotNull(query(), "query"));
        beamSchema = ArrowConversion.ArrowSchemaTranslator.toBeamSchema(info.getSchema());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while fetching Flight SQL schema", e);
      } catch (Exception e) {
        throw new RuntimeException("Failed to fetch Flight SQL schema", e);
      }

      return input
          .apply(org.apache.beam.sdk.io.Read.from(new FlareBoundedSource(this, beamSchema)))
          .setRowSchema(beamSchema);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("dbUrl", dbUrl()));
      builder.addIfNotNull(DisplayData.item("query", query()));
    }
  }

  /** BoundedSource that splits and reads rows from FlareDB Flight SQL server endpoints. */
  static class FlareBoundedSource extends BoundedSource<Row> {
    private final Read spec;
    private final Schema beamSchema;
    private final @Nullable SerializableEndpoint endpoint;

    FlareBoundedSource(Read spec, Schema beamSchema) {
      this(spec, beamSchema, null);
    }

    FlareBoundedSource(Read spec, Schema beamSchema, @Nullable SerializableEndpoint endpoint) {
      this.spec = spec;
      this.beamSchema = beamSchema;
      this.endpoint = endpoint;
    }

    @Override
    public List<? extends BoundedSource<Row>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      if (endpoint != null) {
        return Collections.singletonList(this);
      }

      ConnectionParams params = parseDbUrl(spec.dbUrl());
      List<BoundedSource<Row>> sources = new ArrayList<>();
      try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
          FlightClient client = createClient(allocator, params)) {
        FlightSqlClient sqlClient = new FlightSqlClient(client);
        FlightInfo info = sqlClient.execute(checkNotNull(spec.query(), "query"));
        for (FlightEndpoint fe : info.getEndpoints()) {
          SerializableEndpoint se =
              SerializableEndpoint.fromFlightEndpoint(fe, params.host, params.port);
          sources.add(new FlareBoundedSource(spec, beamSchema, se));
        }
      }

      if (sources.isEmpty()) {
        sources.add(this);
      }
      return sources;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      if (endpoint != null) {
        return -1;
      }
      ConnectionParams params = parseDbUrl(spec.dbUrl());
      try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
          FlightClient client = createClient(allocator, params)) {
        FlightSqlClient sqlClient = new FlightSqlClient(client);
        FlightInfo info = sqlClient.execute(checkNotNull(spec.query(), "query"));
        return info.getBytes();
      }
    }

    @Override
    public BoundedReader<Row> createReader(PipelineOptions options) {
      return new FlightBoundedReader(this);
    }

    @Override
    public void validate() {
      checkArgument(spec.dbUrl() != null, "dbUrl is required");
      checkArgument(spec.query() != null, "query is required");
    }

    @Override
    public Coder<Row> getOutputCoder() {
      return RowCoder.of(beamSchema);
    }
  }

  /** Reader that streams Flight SQL record batches and emits Beam Rows. */
  @SuppressWarnings("initialization.fields.uninitialized")
  static class FlightBoundedReader extends BoundedSource.BoundedReader<Row> {
    private static final Counter RECORDS_READ = Metrics.counter(FlareDbIO.class, "recordsRead");

    private final FlareBoundedSource source;
    private transient BufferAllocator allocator;
    private transient FlightClient client;
    private transient FlightStream stream;
    private transient Iterator<Row> currentBatchIterator;
    private transient Row current;
    private FlareBoundedSource currentSource;

    FlightBoundedReader(FlareBoundedSource source) {
      this.source = source;
      this.currentSource = source;
    }

    @Override
    public boolean start() throws IOException {
      allocator = new RootAllocator(Long.MAX_VALUE);
      Read spec = source.spec;
      ConnectionParams params = parseDbUrl(spec.dbUrl());

      SerializableEndpoint endpoint = source.endpoint;
      if (endpoint == null) {
        try (BufferAllocator discoveryAllocator = new RootAllocator(Long.MAX_VALUE);
            FlightClient discoveryClient = createClient(discoveryAllocator, params)) {
          FlightSqlClient sqlClient = new FlightSqlClient(discoveryClient);
          FlightInfo info = sqlClient.execute(checkNotNull(spec.query(), "query"));
          List<FlightEndpoint> endpoints = info.getEndpoints();
          if (endpoints.isEmpty()) {
            return false;
          }
          endpoint =
              SerializableEndpoint.fromFlightEndpoint(endpoints.get(0), params.host, params.port);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while discovering Flight endpoints", e);
        } catch (Exception e) {
          throw new IOException("Failed to discover Flight endpoints", e);
        }
        currentSource = new FlareBoundedSource(spec, source.beamSchema, endpoint);
      }

      client =
          createClient(
              allocator,
              new ConnectionParams(endpoint.getHost(params.host), endpoint.getPort(params.port)));
      stream = client.getStream(endpoint.getTicket());
      currentBatchIterator = Collections.emptyIterator();
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      while (true) {
        if (currentBatchIterator.hasNext()) {
          current = currentBatchIterator.next();
          RECORDS_READ.inc();
          return true;
        }
        if (stream.next()) {
          VectorSchemaRoot root = stream.getRoot();
          if (root.getRowCount() > 0) {
            Iterator<Row> lazyIterator =
                ArrowConversion.rowsFromRecordBatch(source.beamSchema, root);
            List<Row> materializedRows = new ArrayList<>();
            while (lazyIterator.hasNext()) {
              Row lazyRow = lazyIterator.next();
              materializedRows.add(
                  Row.withSchema(source.beamSchema).addValues(lazyRow.getValues()).build());
            }
            currentBatchIterator = materializedRows.iterator();
          }
        } else {
          return false;
        }
      }
    }

    @Override
    public Row getCurrent() {
      return current;
    }

    @Override
    public void close() throws IOException {
      try {
        if (stream != null) {
          stream.close();
        }
      } catch (Exception e) {
        LOG.warn("Error closing FlightStream", e);
      }
      try {
        if (client != null) {
          client.close();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Interrupted closing FlightClient", e);
      }
      try {
        if (allocator != null) {
          allocator.close();
        }
      } catch (Exception e) {
        LOG.warn("Error closing BufferAllocator", e);
      }
    }

    @Override
    public BoundedSource<Row> getCurrentSource() {
      return currentSource;
    }
  }

  // WRITE

  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {

    abstract String dbUrl();

    abstract @Nullable String table();

    abstract int batchSize();

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDbUrl(String dbUrl);

      abstract Builder<T> setTable(String table);

      abstract Builder<T> setBatchSize(int batchSize);

      abstract Write<T> build();
    }

    /** Sets the FlareDB URL (e.g. {@code "grpc://localhost:8099"}). */
    public Write<T> withDbUrl(String dbUrl) {
      return builder().setDbUrl(dbUrl).build();
    }

    /** Sets the destination table (e.g. {@code "flare.default.my_table"}). */
    public Write<T> to(String table) {
      return builder().setTable(table).build();
    }

    /** Sets batch size for writing rows. */
    public Write<T> withBatchSize(int batchSize) {
      checkArgument(batchSize > 0, "batchSize must be positive");
      return builder().setBatchSize(batchSize).build();
    }

    @Override
    public PDone expand(PCollection<T> input) {
      checkArgument(table() != null, "to() destination table is required");
      Schema inputSchema = checkNotNull(input.getSchema(), "input PCollection requires a schema");

      @SuppressWarnings("unchecked")
      PCollection<Row> rowCollection = (PCollection<Row>) input;

      rowCollection.apply(ParDo.of(new FlightWriteFn(this, inputSchema)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("dbUrl", dbUrl()));
      builder.addIfNotNull(DisplayData.item("table", table()));
      builder.add(DisplayData.item("batchSize", batchSize()));
    }
  }

  /** DoFn that buffers Beam Rows and streams them as Arrow record batches to FlareDB. */
  @SuppressWarnings("initialization.fields.uninitialized")
  static class FlightWriteFn extends DoFn<Row, Void> {
    private static final Counter RECORDS_WRITTEN = Metrics.counter(FlareDbIO.class, "recordsWritten");
    private static final Counter BATCHES_WRITTEN = Metrics.counter(FlareDbIO.class, "batchesWritten");

    private final Write<?> spec;
    private final Schema beamSchema;
    private transient @Nullable BufferAllocator allocator;
    private transient @Nullable FlightClient client;
    private transient FlightClient.@Nullable ClientStreamListener listener;
    private transient @Nullable VectorSchemaRoot root;
    private transient List<Row> batch;

    FlightWriteFn(Write<?> spec, Schema beamSchema) {
      this.spec = spec;
      this.beamSchema = beamSchema;
    }

    @StartBundle
    public void startBundle() {
      batch = new ArrayList<>();
    }

    @ProcessElement
    public void processElement(@Element Row row) {
      checkArgument(
          row.getSchema().equivalent(beamSchema),
          "FlareDbIO.write() requires all rows to use the same schema.");
      batch.add(row);
      if (batch.size() >= spec.batchSize()) {
        flush();
      }
    }

    @FinishBundle
    public void finishBundle() {
      RuntimeException failure = null;
      try {
        flush();
      } catch (RuntimeException e) {
        failure = e;
      }

      try {
        closeConnection();
      } catch (RuntimeException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }

      if (failure != null) {
        throw failure;
      }
    }

    @Teardown
    public void teardown() {
      try {
        closeConnection();
      } catch (RuntimeException e) {
        LOG.warn("Error closing Flight write connection during teardown", e);
      }
    }

    private void ensureConnection() {
      if (client == null) {
        BufferAllocator currentAllocator = new RootAllocator(Long.MAX_VALUE);
        allocator = currentAllocator;
        ConnectionParams params = parseDbUrl(spec.dbUrl());
        FlightClient currentClient = createClient(currentAllocator, params);
        client = currentClient;

        org.apache.arrow.vector.types.pojo.Schema arrowSchema =
            ArrowConversion.ArrowSchemaTranslator.toArrowSchema(beamSchema);
        VectorSchemaRoot currentRoot = VectorSchemaRoot.create(arrowSchema, currentAllocator);
        root = currentRoot;

        FlightDescriptor descriptor =
            FlightDescriptor.path(checkNotNull(spec.table(), "table"));
        listener = currentClient.startPut(descriptor, currentRoot, new AsyncPutListener());
      }
    }

    @SuppressWarnings("nullness")
    private void flush() {
      if (batch == null || batch.isEmpty()) {
        return;
      }
      ensureConnection();

      for (int colIdx = 0; colIdx < beamSchema.getFieldCount(); colIdx++) {
        FieldVector vector = root.getVector(colIdx);
        vector.allocateNew();
        Schema.Field field = beamSchema.getField(colIdx);
        for (int rowIdx = 0; rowIdx < batch.size(); rowIdx++) {
          Object value = batch.get(rowIdx).getValue(colIdx);
          if (value == null) {
            vector.setNull(rowIdx);
          } else {
            setVectorValue(vector, rowIdx, value, field.getType());
          }
        }
        vector.setValueCount(batch.size());
      }
      root.setRowCount(batch.size());

      listener.putNext();
      RECORDS_WRITTEN.inc(batch.size());
      BATCHES_WRITTEN.inc();
      root.clear();
      batch.clear();
    }

    @SuppressWarnings("nullness")
    private void setVectorValue(
        FieldVector vector, int index, Object value, Schema.FieldType type) {
      switch (type.getTypeName()) {
        case BYTE -> ((TinyIntVector) vector).setSafe(index, ((Number) value).byteValue());
        case INT16 -> ((SmallIntVector) vector).setSafe(index, ((Number) value).shortValue());
        case INT32 -> ((IntVector) vector).setSafe(index, ((Number) value).intValue());
        case INT64 -> ((BigIntVector) vector).setSafe(index, ((Number) value).longValue());
        case FLOAT -> ((Float4Vector) vector).setSafe(index, ((Number) value).floatValue());
        case DOUBLE -> ((Float8Vector) vector).setSafe(index, ((Number) value).doubleValue());
        case BOOLEAN -> ((BitVector) vector).setSafe(index, ((Boolean) value) ? 1 : 0);
        case STRING -> ((VarCharVector) vector)
              .setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
        case BYTES -> ((VarBinaryVector) vector).setSafe(index, (byte[]) value);
        case DATETIME -> {
            long millis;
            if (value instanceof org.joda.time.ReadableInstant readableInstant) {
                millis = readableInstant.getMillis();
            } else {
                millis = ((Number) value).longValue();
            }
            ((TimeStampMilliTZVector) vector).setSafe(index, millis);
            }
        default -> throw new IllegalArgumentException(
              "Unsupported Beam type for FlareDbIO.write(): " + type.getTypeName());
      }
    }

    private void closeConnection() {
      RuntimeException failure = null;
      FlightClient.ClientStreamListener currentListener = listener;
      listener = null;
      try {
        if (currentListener != null) {
          currentListener.completed();
          currentListener.getResult();
        }
      } catch (RuntimeException e) {
        failure = e;
      }

      VectorSchemaRoot currentRoot = root;
      root = null;
      try {
        if (currentRoot != null) {
          currentRoot.close();
        }
      } catch (Exception e) {
        if (failure == null) {
          failure = new RuntimeException("Error closing VectorSchemaRoot", e);
        } else {
          failure.addSuppressed(e);
        }
      }

      FlightClient currentClient = client;
      client = null;
      try {
        if (currentClient != null) {
          currentClient.close();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (failure == null) {
          failure = new RuntimeException("Interrupted closing FlightClient", e);
        } else {
          failure.addSuppressed(e);
        }
      } catch (Exception e) {
        if (failure == null) {
          failure = new RuntimeException("Error closing FlightClient", e);
        } else {
          failure.addSuppressed(e);
        }
      }

      BufferAllocator currentAllocator = allocator;
      allocator = null;
      try {
        if (currentAllocator != null) {
          currentAllocator.close();
        }
      } catch (Exception e) {
        if (failure == null) {
          failure = new RuntimeException("Error closing BufferAllocator", e);
        } else {
          failure.addSuppressed(e);
        }
      }

      if (failure != null) {
        throw failure;
      }
    }
  }
}
