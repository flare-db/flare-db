package cloud.langbeam.flare;

import java.util.List;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PortablePipelineOptions;

public interface FlarePipelineOptions extends PortablePipelineOptions {


    @Description("Files to stage to workers")
    @Override
    List<String> getFilesToStage();
    @Override
    void setFilesToStage(List<String> files);

    @Description("Path to the uber JAR to stage to workers")
    String getUberJar();
    void setUberJar(String path);

    /*@Override
    @Description("Default environment type for Flare runner")
    @Default.String("PROCESS")
    String getDefaultEnvironmentType();

    @Override
    void setDefaultEnvironmentType("PROCESS");*/

}
