package com.flaredb.runner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.junit.Test;

public class PortableCoderRewritesTest {

  private static UnionCoder unionCoder() {
    return UnionCoder.of(Arrays.asList(StringUtf8Coder.of(), VarIntCoder.of()));
  }

  private static RunnerApi.Coder opaqueCoder(Coder<?> coder) {
    return RunnerApi.Coder.newBuilder()
        .setSpec(
            RunnerApi.FunctionSpec.newBuilder()
                .setUrn(PortableCoderRewrites.JAVA_SERIALIZED_CODER_URN)
                .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(coder))))
        .build();
  }

  /** A minimal pipeline whose coders map holds a single SDK-private coder under {@code coderId}. */
  private static RunnerApi.Pipeline pipelineWith(String coderId, Coder<?> coder) {
    return RunnerApi.Pipeline.newBuilder()
        .setComponents(RunnerApi.Components.newBuilder().putCoders(coderId, opaqueCoder(coder)))
        .build();
  }

  @Test
  public void wrapsUnionCoderInLengthPrefixAndRetainsInnerCoder() {
    RunnerApi.Pipeline rewritten =
        PortableCoderRewrites.wrapOpaqueCoders(pipelineWith("UnionCoder", unionCoder()));

    RunnerApi.Coder wrapped = rewritten.getComponents().getCodersOrThrow("UnionCoder");
    assertEquals(PortableCoderRewrites.LENGTH_PREFIX_CODER_URN, wrapped.getSpec().getUrn());
    assertEquals(Collections.singletonList("UnionCoder_inner"), wrapped.getComponentCoderIdsList());

    RunnerApi.Coder inner = rewritten.getComponents().getCodersOrThrow("UnionCoder_inner");
    assertEquals(PortableCoderRewrites.JAVA_SERIALIZED_CODER_URN, inner.getSpec().getUrn());
  }

  /**
   * Regression for Nexmark Query 3: an opaque, non-self-delimiting coder (the anonymous {@code
   * Event.CODER}, id {@code "Anonymous"}) must be wrapped so the runner can delimit it.
   */
  @Test
  public void wrapsOpaqueJavasdkCoderWithArbitraryId() {
    // "Anonymous" is the exact id the SDK assigns to the Nexmark Event custom coder.
    RunnerApi.Pipeline rewritten =
        PortableCoderRewrites.wrapOpaqueCoders(pipelineWith("Anonymous", StringUtf8Coder.of()));

    RunnerApi.Coder wrapped = rewritten.getComponents().getCodersOrThrow("Anonymous");
    assertEquals(PortableCoderRewrites.LENGTH_PREFIX_CODER_URN, wrapped.getSpec().getUrn());
    assertEquals(Collections.singletonList("Anonymous_inner"), wrapped.getComponentCoderIdsList());
  }

  @Test
  public void leavesVoidCoderUntouched() {
    RunnerApi.Pipeline rewritten =
        PortableCoderRewrites.wrapOpaqueCoders(pipelineWith("VoidCoder", VoidCoder.of()));

    RunnerApi.Coder coder = rewritten.getComponents().getCodersOrThrow("VoidCoder");
    assertEquals(PortableCoderRewrites.JAVA_SERIALIZED_CODER_URN, coder.getSpec().getUrn());
    assertFalse(rewritten.getComponents().containsCoders("VoidCoder_inner"));
  }

  @Test
  public void leavesVarIntCoderUntouched() {
    RunnerApi.Pipeline rewritten =
        PortableCoderRewrites.wrapOpaqueCoders(pipelineWith("VarIntCoder", VarIntCoder.of()));

    RunnerApi.Coder coder = rewritten.getComponents().getCodersOrThrow("VarIntCoder");
    assertEquals(PortableCoderRewrites.JAVA_SERIALIZED_CODER_URN, coder.getSpec().getUrn());
    assertFalse(rewritten.getComponents().containsCoders("VarIntCoder_inner"));
  }

  /**
   * Schema-aware outputs use this opaque SDK coder in the Java portable representation. The harness
   * must retain the original coder so a {@code Row} output receiver knows its schema, while Fn Data
   * uses the length-prefixed sibling for opaque element framing.
   */
  @Test
  public void leavesSchemaCoderUntouched() {
    Schema schema = Schema.builder().addStringField("value").build();
    RunnerApi.Pipeline rewritten =
        PortableCoderRewrites.wrapOpaqueCoders(pipelineWith("SchemaCoder", SchemaCoder.of(schema)));

    RunnerApi.Coder coder = rewritten.getComponents().getCodersOrThrow("SchemaCoder");
    assertEquals(PortableCoderRewrites.JAVA_SERIALIZED_CODER_URN, coder.getSpec().getUrn());
    assertFalse(rewritten.getComponents().containsCoders("SchemaCoder_inner"));

    RunnerApi.Coder wireCoder =
        rewritten
            .getComponents()
            .getCodersOrThrow("SchemaCoder" + PortableCoderRewrites.BOUNDARY_LENGTH_PREFIX_SUFFIX);
    assertEquals(PortableCoderRewrites.LENGTH_PREFIX_CODER_URN, wireCoder.getSpec().getUrn());
    assertEquals(Collections.singletonList("SchemaCoder"), wireCoder.getComponentCoderIdsList());
  }

  @Test
  public void addsBoundaryWrapperForCompositeContainingSchemaCoder() {
    Schema schema = Schema.builder().addStringField("value").build();
    RunnerApi.Coder kvCoder =
        RunnerApi.Coder.newBuilder()
            .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn("beam:coder:kv:v1").build())
            .addComponentCoderIds("StringUtf8Coder")
            .addComponentCoderIds("SchemaCoder")
            .build();
    RunnerApi.Pipeline pipeline =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(
                RunnerApi.Components.newBuilder()
                    .putCoders("SchemaCoder", opaqueCoder(SchemaCoder.of(schema)))
                    .putCoders(
                        "StringUtf8Coder",
                        RunnerApi.Coder.newBuilder()
                            .setSpec(
                                RunnerApi.FunctionSpec.newBuilder()
                                    .setUrn("beam:coder:string_utf8:v1")
                                    .build())
                            .build())
                    .putCoders("KvCoder1", kvCoder))
            .build();

    RunnerApi.Pipeline rewritten = PortableCoderRewrites.wrapOpaqueCoders(pipeline);

    assertEquals(kvCoder, rewritten.getComponents().getCodersOrThrow("KvCoder1"));
    RunnerApi.Coder wireCoder =
        rewritten
            .getComponents()
            .getCodersOrThrow("KvCoder1" + PortableCoderRewrites.BOUNDARY_LENGTH_PREFIX_SUFFIX);
    assertEquals(PortableCoderRewrites.LENGTH_PREFIX_CODER_URN, wireCoder.getSpec().getUrn());
    assertEquals(Collections.singletonList("KvCoder1"), wireCoder.getComponentCoderIdsList());
  }

  @Test
  public void applyingTwiceIsANoOp() {
    RunnerApi.Pipeline once =
        PortableCoderRewrites.wrapOpaqueCoders(pipelineWith("UnionCoder", unionCoder()));

    RunnerApi.Pipeline twice = PortableCoderRewrites.wrapOpaqueCoders(once);

    assertEquals(once, twice);
  }

  /**
   * The harness rehydrates coders from the descriptor's components map. This asserts it resolves
   * the rewritten entry to a {@code LengthPrefixCoder} around the real {@link UnionCoder}, and that
   * the wire format is {@code VarInt(len) | union bytes} (which is what the Rust runner expects).
   */
  @Test
  @SuppressWarnings({
    "unchecked",
    "deprecation"
  }) // Context.OUTER models the harness's LengthPrefixCoder framing
  public void harnessRehydratesWrappedUnionCoderAndRoundTrips() throws Exception {
    RunnerApi.Pipeline rewritten =
        PortableCoderRewrites.wrapOpaqueCoders(pipelineWith("UnionCoder", unionCoder()));
    RehydratedComponents rehydrated = RehydratedComponents.forComponents(rewritten.getComponents());

    Coder<?> rehydratedCoder = rehydrated.getCoder("UnionCoder");
    assertTrue(
        "expected a LengthPrefixCoder, got " + rehydratedCoder.getClass(),
        rehydratedCoder instanceof LengthPrefixCoder);

    RawUnionValue value = new RawUnionValue(0, "hello");

    Coder<RawUnionValue> coder = (Coder<RawUnionValue>) rehydratedCoder;
    ByteArrayOutputStream actualOut = new ByteArrayOutputStream();
    coder.encode(value, actualOut);
    byte[] actual = actualOut.toByteArray();

    // Expected framing: VarInt(length) followed by unionCoder.encode(value, OUTER).
    ByteArrayOutputStream payloadOut = new ByteArrayOutputStream();
    unionCoder().encode(value, payloadOut, Coder.Context.OUTER);
    byte[] payload = payloadOut.toByteArray();
    ByteArrayOutputStream expectedOut = new ByteArrayOutputStream();
    VarInt.encode(payload.length, expectedOut);
    expectedOut.write(payload);
    byte[] expected = expectedOut.toByteArray();

    assertArrayEquals(expected, actual);
    assertEquals(value, coder.decode(new ByteArrayInputStream(actual)));
  }
}
