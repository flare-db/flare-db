package com.flaredb.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

public class FlareArtifactResolverTest {

  private static PipelineOptions optionsWithUberJar(String path) {
    FlarePipelineOptions options = PipelineOptionsFactory.as(FlarePipelineOptions.class);
    options.setUberJar(path);
    return options;
  }

  private static File newTempJar() throws IOException {
    File jar = File.createTempFile("flare-artifact-resolver-test", ".jar");
    jar.deleteOnExit();
    return jar;
  }

  @Test
  public void constructorThrowsWhenUberJarNotSet() {
    PipelineOptions options = PipelineOptionsFactory.as(FlarePipelineOptions.class);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> new FlareArtifactResolver(options));
    assertTrue(exception.getMessage().contains("--uberJar"));
  }

  @Test
  public void constructorThrowsWhenUberJarIsEmpty() {
    PipelineOptions options = optionsWithUberJar("");

    assertThrows(IllegalArgumentException.class, () -> new FlareArtifactResolver(options));
  }

  @Test
  public void constructorThrowsWhenUberJarDoesNotExist() {
    PipelineOptions options = optionsWithUberJar("/no/such/path/app.jar");

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> new FlareArtifactResolver(options));
    assertTrue(exception.getMessage().contains("not found"));
  }

  @Test
  public void constructorThrowsWhenUberJarIsADirectory() {
    PipelineOptions options = optionsWithUberJar(System.getProperty("java.io.tmpdir"));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> new FlareArtifactResolver(options));
    assertTrue(exception.getMessage().contains("not a file"));
  }

  @Test
  public void constructorSucceedsWhenUberJarIsAnExistingFile() throws IOException {
    File jar = newTempJar();

    new FlareArtifactResolver(optionsWithUberJar(jar.getAbsolutePath()));
  }

  @Test
  public void resolveArtifactsWithEmptyListReturnsUberJarArtifact() throws IOException {
    File jar = newTempJar();
    FlareArtifactResolver resolver =
        new FlareArtifactResolver(optionsWithUberJar(jar.getAbsolutePath()));

    List<RunnerApi.ArtifactInformation> resolved =
        resolver.resolveArtifacts(Collections.emptyList());

    assertEquals(1, resolved.size());
    RunnerApi.ArtifactInformation artifact = resolved.get(0);
    assertEquals("beam:artifact:type:file:v1", artifact.getTypeUrn());
    assertEquals("beam:artifact:role:staging_to:v1", artifact.getRoleUrn());

    RunnerApi.ArtifactFilePayload payload =
        RunnerApi.ArtifactFilePayload.parseFrom(artifact.getTypePayload());
    assertEquals(jar.getAbsolutePath(), payload.getPath());
  }

  @Test
  public void resolveArtifactsWithNonEmptyListReturnsItUnchanged() throws IOException {
    File jar = newTempJar();
    FlareArtifactResolver resolver =
        new FlareArtifactResolver(optionsWithUberJar(jar.getAbsolutePath()));

    RunnerApi.ArtifactInformation existing =
        RunnerApi.ArtifactInformation.newBuilder().setTypeUrn("beam:artifact:type:url:v1").build();
    List<RunnerApi.ArtifactInformation> input = Collections.singletonList(existing);

    List<RunnerApi.ArtifactInformation> resolved = resolver.resolveArtifacts(input);

    assertSame(input, resolved);
  }

  @Test
  public void registerIsNotSupported() throws IOException {
    File jar = newTempJar();
    FlareArtifactResolver resolver =
        new FlareArtifactResolver(optionsWithUberJar(jar.getAbsolutePath()));

    assertThrows(UnsupportedOperationException.class, () -> resolver.register(artifact -> null));
  }

  @Test
  public void resolveArtifactsForPipelineIsNotSupported() throws IOException {
    File jar = newTempJar();
    FlareArtifactResolver resolver =
        new FlareArtifactResolver(optionsWithUberJar(jar.getAbsolutePath()));

    assertThrows(
        UnsupportedOperationException.class,
        () -> resolver.resolveArtifacts(RunnerApi.Pipeline.getDefaultInstance()));
  }
}
