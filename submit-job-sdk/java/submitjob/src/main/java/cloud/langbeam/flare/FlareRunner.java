package cloud.langbeam.flare;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;

public class FlareRunner extends PipelineRunner<FlarePipelineJob> {

    @Override
    public FlarePipelineJob run(Pipeline pipeline) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
