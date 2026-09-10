package com.flaredb.io;

import static org.junit.Assert.assertEquals;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Test;

public class FlareDbIOTest {

  @Test
  public void testParseDbUrlWithScheme() {
    FlareDbIO.ConnectionParams params = FlareDbIO.parseDbUrl("grpc://127.0.0.1:47470");
    assertEquals("127.0.0.1", params.host);
    assertEquals(47470, params.port);
  }

  @Test
  public void testParseDbUrlWithoutScheme() {
    FlareDbIO.ConnectionParams params = FlareDbIO.parseDbUrl("localhost:50051");
    assertEquals("localhost", params.host);
    assertEquals(50051, params.port);
  }

  @Test
  public void testBeamToArrowSchemaConversion() {
    Schema beamSchema =
        Schema.builder()
            .addStringField("id")
            .addInt64Field("age")
            .addDoubleField("score")
            .addBooleanField("active")
            .build();

    org.apache.arrow.vector.types.pojo.Schema arrowSchema =
        ArrowConversion.ArrowSchemaTranslator.toArrowSchema(beamSchema);

    assertEquals(4, arrowSchema.getFields().size());
    assertEquals("id", arrowSchema.getFields().get(0).getName());
    assertEquals(ArrowType.ArrowTypeID.Utf8, arrowSchema.getFields().get(0).getType().getTypeID());

    assertEquals("age", arrowSchema.getFields().get(1).getName());
    assertEquals(ArrowType.ArrowTypeID.Int, arrowSchema.getFields().get(1).getType().getTypeID());

    assertEquals("score", arrowSchema.getFields().get(2).getName());
    assertEquals(
        ArrowType.ArrowTypeID.FloatingPoint, arrowSchema.getFields().get(2).getType().getTypeID());

    assertEquals("active", arrowSchema.getFields().get(3).getName());
    assertEquals(ArrowType.ArrowTypeID.Bool, arrowSchema.getFields().get(3).getType().getTypeID());
  }

  @Test
  public void testArrowViewTypesToBeamSchema() {
    org.apache.arrow.vector.types.pojo.Schema arrowSchema =
        new org.apache.arrow.vector.types.pojo.Schema(
            java.util.Arrays.asList(
                new org.apache.arrow.vector.types.pojo.Field(
                    "name",
                    org.apache.arrow.vector.types.pojo.FieldType.nullable(
                        ArrowType.Utf8View.INSTANCE),
                    java.util.Collections.emptyList()),
                new org.apache.arrow.vector.types.pojo.Field(
                    "payload",
                    org.apache.arrow.vector.types.pojo.FieldType.nullable(
                        ArrowType.BinaryView.INSTANCE),
                    java.util.Collections.emptyList())));

    Schema beamSchema = ArrowConversion.ArrowSchemaTranslator.toBeamSchema(arrowSchema);

    assertEquals(2, beamSchema.getFieldCount());
    assertEquals(Schema.TypeName.STRING, beamSchema.getField("name").getType().getTypeName());
    assertEquals(Schema.TypeName.BYTES, beamSchema.getField("payload").getType().getTypeName());
  }

  @Test
  public void testReadBuilder() {
    FlareDbIO.Read read =
        FlareDbIO.read()
            .withDbUrl("grpc://localhost:47470")
            .fromQuery("SELECT * FROM flare.default.users");

    assertEquals("grpc://localhost:47470", read.dbUrl());
    assertEquals("SELECT * FROM flare.default.users", read.query());
  }

  @Test
  public void testWriteBuilder() {
    FlareDbIO.Write<Object> write =
        FlareDbIO.write()
            .withDbUrl("grpc://localhost:47470")
            .to("flare.default.users")
            .withBatchSize(512);

    assertEquals("grpc://localhost:47470", write.dbUrl());
    assertEquals("flare.default.users", write.table());
    assertEquals(512, write.batchSize());
  }
}
