package com.flaredb.example.flareio;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

import com.flaredb.io.FlareDbIO;
import com.flaredb.runner.FlarePipelineOptions;

public interface ReadPipelineOptions extends FlarePipelineOptions {

    @Description("FlareDB endpoint URL")
    @Default.String(FlareDbIO.DEFAULT_DB_URL)
    String getDbUrl();

    void setDbUrl(String dbUrl);

    @Description("Path of the text file the read rows are written to")
    @Default.String("/home/ganesh/flare-db/flareio/flare-db/test-data/scores_out.txt")
    String getOutputFile();

    void setOutputFile(String outputFile);
}
