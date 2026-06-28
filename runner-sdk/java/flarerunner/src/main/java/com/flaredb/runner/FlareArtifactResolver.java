package com.flaredb.runner;

import java.io.File;
import java.util.Collections;
import java.util.List;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.construction.ArtifactResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ArtifactResolver} implementation used by the FlareDB runner.
 *
 * <p>This resolver stages the application's uber (fat) JAR as the sole pipeline artifact. When the
 * Beam Job Service requests artifacts, the configured uber JAR is returned so that it can be
 * uploaded and made available to worker environments during pipeline execution.
 */
public class FlareArtifactResolver implements ArtifactResolver {
  private static final Logger LOG = LoggerFactory.getLogger(FlareArtifactResolver.class);

  private final String uberJarPath;

  /**
   * Creates a new artifact resolver using the supplied pipeline options.
   *
   * <p>Jar path must be set using pipeline options eg: options.setUberJar("/path-to-jar") The
   * constructor validates the configured path before job submission begins.
   *
   * @param options pipeline options containing FlareDB-specific configuration
   * @throws IllegalArgumentException if the uber JAR is not specified or does not reference an
   *     existing file
   */
  public FlareArtifactResolver(PipelineOptions options) {
    FlarePipelineOptions flareOptions = options.as(FlarePipelineOptions.class);
    this.uberJarPath = flareOptions.getUberJar();

    if (uberJarPath == null || uberJarPath.isEmpty()) {
      throw new IllegalArgumentException("UberJar path must be specified via --uberJar option");
    }

    File jarFile = new File(uberJarPath);
    if (!jarFile.exists()) {
      throw new IllegalArgumentException("UberJar not found: " + uberJarPath);
    }
    if (!jarFile.isFile()) {
      throw new IllegalArgumentException("UberJar path is not a file: " + uberJarPath);
    }

    LOG.info("Will stage uber JAR: {}", uberJarPath);
  }

  @Override
  public void register(ResolutionFn fn) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public RunnerApi.Pipeline resolveArtifacts(RunnerApi.Pipeline pipeline) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<RunnerApi.ArtifactInformation> resolveArtifacts(
      List<RunnerApi.ArtifactInformation> artifacts) {
    // Runner sends empty list, we return the uber JAR
    if (artifacts.isEmpty()) {
      RunnerApi.ArtifactFilePayload payload =
          RunnerApi.ArtifactFilePayload.newBuilder().setPath(uberJarPath).build();

      RunnerApi.ArtifactInformation artifact =
          RunnerApi.ArtifactInformation.newBuilder()
              .setTypeUrn("beam:artifact:type:file:v1")
              .setTypePayload(payload.toByteString())
              .setRoleUrn("beam:artifact:role:staging_to:v1")
              .build();

      return Collections.singletonList(artifact);
    }

    // Otherwise return as-is
    return artifacts;
  }
}
