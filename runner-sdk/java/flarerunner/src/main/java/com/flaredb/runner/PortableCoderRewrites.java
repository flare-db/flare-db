package com.flaredb.runner;

import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.transforms.join.CoGbkResult.CoGbkResultCoder;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rewrites SDK-private join coders that a portable runner cannot delimit into a length-prefixed
 * form the FlareDB runner can carry opaquely.
 *
 * <p>Java's {@code CoGroupByKey} lowers to a {@code Flatten} + {@code GroupByKey} over values
 * encoded with {@link UnionCoder} (a {@code RawUnionValue} is {@code varint tag | element bytes}).
 * It is not a Beam model coder, so {@code PipelineTranslation} emits it as an opaque {@code
 * beam:coders:javasdk:0.1} blob with no {@code component_coder_ids}. The runner sees only the blob
 * and cannot tell where the value ends, so it mis-frames and desynchronizes the element stream (the
 * {@link UnionCoder} tag is a varint, not a length). {@link CoGbkResultCoder} has the same shape.
 *
 * <p>The fix rewrites each such coder entry <em>in place</em> to {@code
 * beam:coder:length_prefix:v1} wrapping the original, which is retained under a derived {@code
 * "_inner"} id. Because every reference in the pipeline is by coder id, all uses (the union-table
 * {@code KvCoder} and the GBK output {@code IterableCoder}) are rewritten at once. The descriptor
 * is the single source of truth for both the SDK harness and the runner, so both sides agree: the
 * harness resolves {@code LengthPrefixCoder.of(UnionCoder)} and the runner treats the value as
 * opaque, self-delimiting bytes. No wire-format change is forced on the SDK, and the payload
 * round-trips byte-for-byte.
 *
 * <p>This mirrors the Python portable runner, which length-prefixes coders it does not understand
 * and stores them as opaque bytes (see {@code fn_api_runner/translations.py}, {@code
 * maybe_length_prefixed_and_safe_coder}).
 */
final class PortableCoderRewrites {

  private static final Logger LOG = LoggerFactory.getLogger(PortableCoderRewrites.class);

  /** URN the Java SDK uses to transport a coder as an opaque, SDK-private serialized blob. */
  static final String JAVA_SERIALIZED_CODER_URN = "beam:coders:javasdk:0.1";

  /** URN of {@code beam:coder:length_prefix:v1}. */
  static final String LENGTH_PREFIX_CODER_URN = "beam:coder:length_prefix:v1";

  /** Suffix appended to a wrapped coder id to name the retained, unwrapped inner coder. */
  static final String INNER_SUFFIX = "_inner";

  private PortableCoderRewrites() {}

  /**
   * Returns {@code pipeline} with every SDK-private join coder wrapped in a {@code length_prefix}
   * coder. Returns the input unchanged when there is nothing to wrap.
   */
  static RunnerApi.Pipeline wrapJoinCoders(RunnerApi.Pipeline pipeline) {
    Components components = pipeline.getComponents();
    Components.Builder builder = components.toBuilder();
    boolean wrappedAny = false;

    for (Map.Entry<String, RunnerApi.Coder> entry : components.getCodersMap().entrySet()) {
      String id = entry.getKey();
      RunnerApi.Coder coder = entry.getValue();

      // An "_inner" entry is the unwrapped coder this transform retains; never wrap it again, so a
      // second pass is a no-op.
      if (id.endsWith(INNER_SUFFIX)) {
        continue;
      }
      if (!coder.hasSpec() || !JAVA_SERIALIZED_CODER_URN.equals(coder.getSpec().getUrn())) {
        continue;
      }
      if (!isSdkPrivateJoinCoder(coder)) {
        continue;
      }

      String innerId = id + INNER_SUFFIX;
      builder.putCoders(innerId, coder);
      builder.putCoders(
          id,
          RunnerApi.Coder.newBuilder()
              .setSpec(FunctionSpec.newBuilder().setUrn(LENGTH_PREFIX_CODER_URN))
              .addComponentCoderIds(innerId)
              .build());
      wrappedAny = true;
    }

    if (!wrappedAny) {
      return pipeline;
    }
    return pipeline.toBuilder().setComponents(builder.build()).build();
  }

  /**
   * Whether {@code coder} is one of the SDK-private join coders the runner cannot delimit. Detected
   * by deserializing the payload rather than matching the id, since ids are uniquified (e.g. {@code
   * UnionCoder2}).
   */
  private static boolean isSdkPrivateJoinCoder(RunnerApi.Coder coder) {
    Object deserialized;
    try {
      deserialized =
          SerializableUtils.deserializeFromByteArray(
              coder.getSpec().getPayload().toByteArray(), "SDK-private join coder");
    } catch (RuntimeException e) {
      LOG.debug("Could not deserialize SDK coder payload; leaving the coder untouched", e);
      return false;
    }
    return deserialized instanceof UnionCoder || deserialized instanceof CoGbkResultCoder;
  }
}
