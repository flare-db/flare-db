package com.flaredb.example.flareio;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

import com.flaredb.io.FlareDbIO;
import com.flaredb.runner.FlarePipelineOptions;
public interface WritePipelineOptions extends FlarePipelineOptions {

    @Description("Path to the CSV file to load into FlareDB")
    @Default.String("/home/ganesh/flare-db/flareio/flare-db/test-data/scores.csv")
    String getCsvFile();

    void setCsvFile(String csvFile);

    @Description("Destination FlareDB table name")
    @Default.String("flare.default.scores")
    String getTable();

    void setTable(String table);

    @Description("FlareDB endpoint URL")
    @Default.String(FlareDbIO.DEFAULT_DB_URL)
    String getDbUrl();

    void setDbUrl(String dbUrl);
  }
