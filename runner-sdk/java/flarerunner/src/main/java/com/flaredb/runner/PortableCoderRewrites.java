package com.flaredb.runner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.util.construction.RehydratedComponents;

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
 * left untouched to avoid double length-prefixing. Schema coders remain the PCollection coder
 * because the SDK harness inspects its schema to provide {@code Row} receivers to user functions.
 * They also get a length-prefixed sibling used by FlareDB's Data-channel wire coder, preserving
 * opaque element framing without hiding schema metadata from the harness.
 */
final class PortableCoderRewrites {

  /** URN the Java SDK uses to transport a coder as an opaque, SDK-private serialized blob. */
  static final String JAVA_SERIALIZED_CODER_URN = "beam:coders:javasdk:0.1";

  /** URN of {@code beam:coder:length_prefix:v1}. */
  static final String LENGTH_PREFIX_CODER_URN = "beam:coder:length_prefix:v1";

  /** Suffix appended to a wrapped coder id to name the retained, unwrapped inner coder. */
  static final String INNER_SUFFIX = "_inner";

  /** Suffix FlareDB resolves for a boundary-only length-prefixed coder. */
  static final String BOUNDARY_LENGTH_PREFIX_SUFFIX = "_lp";

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
    RehydratedComponents rehydratedComponents = RehydratedComponents.forComponents(components);
    Set<String> schemaDependentCoderIds = schemaDependentCoderIds(components, rehydratedComponents);
    boolean wrappedAny = false;

    // Keep the original coder visible to schema-aware DoFns. The boundary wire coder
    // resolves the "_lp" sibling and uses it to frame the complete element opaquely.
    for (String id : schemaDependentCoderIds) {
      builder.putCoders(id + BOUNDARY_LENGTH_PREFIX_SUFFIX, lengthPrefixCoder(id));
      wrappedAny = true;
    }

    for (Map.Entry<String, RunnerApi.Coder> entry : components.getCodersMap().entrySet()) {
      String id = entry.getKey();
      RunnerApi.Coder coder = entry.getValue();

      // An "_inner" entry is the unwrapped coder this transform retains; never wrap it again, so a
      // second pass is a no-op.
      if (isGeneratedCoderId(id)) {
        continue;
      }
      if (schemaDependentCoderIds.contains(id)) {
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
      builder.putCoders(id, lengthPrefixCoder(innerId));
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

  private static boolean isGeneratedCoderId(String id) {
    return id.endsWith(INNER_SUFFIX) || id.endsWith(BOUNDARY_LENGTH_PREFIX_SUFFIX);
  }

  private static RunnerApi.Coder lengthPrefixCoder(String componentId) {
    return RunnerApi.Coder.newBuilder()
        .setSpec(FunctionSpec.newBuilder().setUrn(LENGTH_PREFIX_CODER_URN))
        .addComponentCoderIds(componentId)
        .build();
  }

  private static Set<String> schemaDependentCoderIds(
      Components components, RehydratedComponents rehydratedComponents) {
    Set<String> dependentIds = new HashSet<>();
    for (Map.Entry<String, RunnerApi.Coder> entry : components.getCodersMap().entrySet()) {
      String id = entry.getKey();
      if (!isGeneratedCoderId(id) && isSchemaCoder(rehydratedComponents, id)) {
        dependentIds.add(id);
      }
    }

    boolean changed;
    do {
      changed = false;
      for (Map.Entry<String, RunnerApi.Coder> entry : components.getCodersMap().entrySet()) {
        String id = entry.getKey();
        if (isGeneratedCoderId(id) || dependentIds.contains(id)) {
          continue;
        }
        if (entry.getValue().getComponentCoderIdsList().stream().anyMatch(dependentIds::contains)) {
          dependentIds.add(id);
          changed = true;
        }
      }
    } while (changed);

    return dependentIds;
  }

  private static boolean isSchemaCoder(RehydratedComponents components, String id) {
    try {
      return components.getCoder(id) instanceof SchemaCoder;
    } catch (IOException | RuntimeException e) {
      // Invalid or incomplete coder graphs are handled by the normal rewrite path. Preserve the
      // prior behavior and let the harness surface the decoding failure during execution.
      return false;
    }
  }
}
