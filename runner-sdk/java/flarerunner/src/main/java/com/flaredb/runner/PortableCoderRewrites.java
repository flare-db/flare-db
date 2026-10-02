package com.flaredb.runner;

import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;

/**
 * Rewrites opaque, SDK-private {@code beam:coders:javasdk:0.1} coders into a length-prefixed form
 * the FlareDB runner can carry as self-delimiting bytes.
 *
 * <p>The runner cannot interpret an SDK-private serialized coder blob, so it treats such coders as
 * opaque {@code VarInt(length) | bytes}. That only works if the SDK length-prefixes the value; many
 * SDK-private coders are not self-delimiting (e.g. Java's {@code CoGroupByKey} union coders, or the
 * Nexmark {@code Event} custom coder), which desynchronizes the element stream. Wrapping each such
 * coder in {@code beam:coder:length_prefix:v1} makes both sides agree — the harness encodes with
 * {@code LengthPrefixCoder.of(original)} and the runner reads opaque bytes — without forcing any
 * wire-format change on the SDK.
 *
 * <p>Coders the runner already understands natively ({@code VoidCoder}, {@code VarIntCoder}) are
 * left untouched to avoid double length-prefixing. This mirrors the Python portable runner's {@code
 * maybe_length_prefixed_and_safe_coder}.
 */
final class PortableCoderRewrites {

  /** URN the Java SDK uses to transport a coder as an opaque, SDK-private serialized blob. */
  static final String JAVA_SERIALIZED_CODER_URN = "beam:coders:javasdk:0.1";

  /** URN of {@code beam:coder:length_prefix:v1}. */
  static final String LENGTH_PREFIX_CODER_URN = "beam:coder:length_prefix:v1";

  /** Suffix appended to a wrapped coder id to name the retained, unwrapped inner coder. */
  static final String INNER_SUFFIX = "_inner";

  /**
   * Coder ids the runner decodes natively (rather than as length-prefixed opaque bytes), so they
   * must not be wrapped. These match the ids the SDK assigns via {@code approximateSimpleName}.
   */
  private static final String VOID_CODER_ID = "VoidCoder";

  private static final String VAR_INT_CODER_ID = "VarIntCoder";

  private PortableCoderRewrites() {}

  /**
   * Returns {@code pipeline} with every opaque {@code beam:coders:javasdk:0.1} coder wrapped in a
   * {@code length_prefix} coder, except the two the runner handles natively. Returns the input
   * unchanged when there is nothing to wrap.
   */
  static RunnerApi.Pipeline wrapOpaqueCoders(RunnerApi.Pipeline pipeline) {
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
      if (isRunnerHandledCoder(id)) {
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
   * Whether the runner decodes this coder id natively rather than as length-prefixed opaque bytes.
   *
   * <p>Matched by coder id (not payload) because that is the contract the runner itself uses: it
   * special-cases these two ids and treats every other javasdk coder as length-prefixed bytes.
   */
  private static boolean isRunnerHandledCoder(String id) {
    return VOID_CODER_ID.equals(id) || VAR_INT_CODER_ID.equals(id);
  }
}
