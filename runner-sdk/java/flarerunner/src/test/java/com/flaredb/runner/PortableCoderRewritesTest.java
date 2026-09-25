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

  /** A minimal pipeline whose coders map holds a single SDK-private coder under {@code coderId}. */
  private static RunnerApi.Pipeline pipelineWith(String coderId, Coder<?> coder) {
    RunnerApi.Coder coderProto =
        RunnerApi.Coder.newBuilder()
            .setSpec(
                RunnerApi.FunctionSpec.newBuilder()
                    .setUrn(PortableCoderRewrites.JAVA_SERIALIZED_CODER_URN)
                    .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(coder))))
            .build();
    return RunnerApi.Pipeline.newBuilder()
        .setComponents(RunnerApi.Components.newBuilder().putCoders(coderId, coderProto))
        .build();
  }

  @Test
  public void wrapsUnionCoderInLengthPrefixAndRetainsInnerCoder() {
    RunnerApi.Pipeline rewritten =
        PortableCoderRewrites.wrapJoinCoders(pipelineWith("UnionCoder", unionCoder()));

    RunnerApi.Coder wrapped = rewritten.getComponents().getCodersOrThrow("UnionCoder");
    assertEquals(PortableCoderRewrites.LENGTH_PREFIX_CODER_URN, wrapped.getSpec().getUrn());
    assertEquals(Collections.singletonList("UnionCoder_inner"), wrapped.getComponentCoderIdsList());

    RunnerApi.Coder inner = rewritten.getComponents().getCodersOrThrow("UnionCoder_inner");
    assertEquals(PortableCoderRewrites.JAVA_SERIALIZED_CODER_URN, inner.getSpec().getUrn());
  }

  @Test
  public void leavesCodersThatAreNotSdkPrivateJoinCodersUntouched() {
    RunnerApi.Pipeline rewritten =
        PortableCoderRewrites.wrapJoinCoders(pipelineWith("StringUtf8Coder", StringUtf8Coder.of()));

    RunnerApi.Coder coder = rewritten.getComponents().getCodersOrThrow("StringUtf8Coder");
    assertEquals(PortableCoderRewrites.JAVA_SERIALIZED_CODER_URN, coder.getSpec().getUrn());
    assertFalse(rewritten.getComponents().containsCoders("StringUtf8Coder_inner"));
  }

  @Test
  public void applyingTwiceIsANoOp() {
    RunnerApi.Pipeline once =
        PortableCoderRewrites.wrapJoinCoders(pipelineWith("UnionCoder", unionCoder()));

    RunnerApi.Pipeline twice = PortableCoderRewrites.wrapJoinCoders(once);

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
        PortableCoderRewrites.wrapJoinCoders(pipelineWith("UnionCoder", unionCoder()));
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
