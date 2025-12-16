package cloud.langbeam.flare;

import java.io.IOException;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;

public class FlarePipelineJob implements PipelineResult {

    @Override
    public State getState() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public State cancel() throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public State waitUntilFinish(Duration duration) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public State waitUntilFinish() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public MetricResults metrics() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
