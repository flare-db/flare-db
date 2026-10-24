package com.flaredb.testing;

import com.flaredb.runner.FlarePipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

public interface TestFlarePipelineOptions extends TestPipelineOptions, FlarePipelineOptions {

  class Registrar implements PipelineOptionsRegistrar {
    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return ImmutableList.of(TestFlarePipelineOptions.class);
    }
  }
}
