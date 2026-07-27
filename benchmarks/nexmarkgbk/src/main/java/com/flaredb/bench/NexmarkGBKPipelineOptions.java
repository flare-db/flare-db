package com.flaredb.bench;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

import com.flaredb.runner.FlarePipelineOptions;


public interface NexmarkGBKPipelineOptions extends FlarePipelineOptions{

    @Description("Number of Nexmark events to generate for the benchmark")
    @Default.Integer(1000000)
    int getNumEvents();

    void setNumEvents(int value);

    @Description("Path of the text file where the benchmark summary table will be written")
    @Default.String("target/nexmark-gbk-benchmark.txt")
    String getBenchmarkOutputPath();

    void setBenchmarkOutputPath(String value);
}
