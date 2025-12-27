package cloud.langbeam.flare;

import java.io.IOException;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.joda.time.Duration;

class FlarePipelineJob implements PipelineResult {

    private final ByteString jobId;

    FlarePipelineJob(ByteString jobId) {
        this.jobId = jobId;
    }

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
