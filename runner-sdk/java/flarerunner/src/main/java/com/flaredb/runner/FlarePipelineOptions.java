package com.flaredb.runner;

import java.util.List;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PortablePipelineOptions;

/**
 * Pipeline options for the FlareDB Beam runner.
 *
 * <p>These options configure how a Beam pipeline is packaged and submitted to a FlareDB Job
 * Service. In addition to the options defined by {@link PortablePipelineOptions}, this interface
 * provides FlareDB-specific configuration for staging application artifacts.
 */
public interface FlarePipelineOptions extends PortablePipelineOptions {

  /**
   * Returns the files that should be staged to the worker environment before pipeline execution.
   */
  @Description("Files to stage to workers")
  @Override
  List<String> getFilesToStage();

  /** Sets the files that should be staged to the worker environment before pipeline execution. */
  @Override
  void setFilesToStage(List<String> files);

  /**
   * Returns the path to the application's uber (fat) JAR that will be staged and executed by
   * FlareDB workers.
   */
  @Description("Path to the uber JAR to stage to workers")
  String getUberJar();

  /**
   * Sets the path to the application's uber (fat) JAR that will be staged and executed by FlareDB
   * workers.
   *
   * @param path path to the uber JAR
   */
  void setUberJar(String path);

  /*@Override
  @Description("Default environment type for Flare runner")
  @Default.String("PROCESS")
  String getDefaultEnvironmentType();

  @Override
  void setDefaultEnvironmentType("PROCESS");*/

}
