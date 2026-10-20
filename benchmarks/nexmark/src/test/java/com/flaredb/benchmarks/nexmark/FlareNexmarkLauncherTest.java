package com.flaredb.benchmarks.nexmark;

import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;
import org.junit.Test;

/** Tests benchmark lifecycle cleanup. */
public class FlareNexmarkLauncherTest {
  private static final class CloseTrackingResult implements PipelineResult, AutoCloseable {
    private boolean closed;

    @Override
    public State getState() {
      return State.DONE;
    }

    @Override
    public State cancel() {
      return State.DONE;
    }

    @Override
    public State waitUntilFinish(Duration duration) {
      return State.DONE;
    }

    @Override
    public State waitUntilFinish() {
      return State.DONE;
    }

    @Override
    public MetricResults metrics() {
      throw new UnsupportedOperationException("not needed for this test");
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  @Test
  public void closesOwnedPipelineResults() throws Exception {
    CloseTrackingResult result = new CloseTrackingResult();

    FlareNexmarkLauncher.closePipelineResult(result);

    assertTrue(result.closed);
  }
}
