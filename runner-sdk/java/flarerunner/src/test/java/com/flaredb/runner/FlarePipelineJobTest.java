package com.flaredb.runner;

import static org.junit.Assert.assertThrows;

import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.joda.time.Duration;
import org.junit.Test;

/**
 * {@link FlarePipelineJob} currently only carries the submitted job id; every {@link
 * org.apache.beam.sdk.PipelineResult} operation is documented as unimplemented. These tests pin
 * down that contract so it fails loudly (rather than silently changing behavior) once monitoring,
 * cancellation, or metrics support is added.
 */
public class FlarePipelineJobTest {

  private static FlarePipelineJob newJob() {
    return new FlarePipelineJob(ByteString.copyFromUtf8("job-1"));
  }

  @Test
  public void getStateIsNotSupported() {
    assertThrows(UnsupportedOperationException.class, () -> newJob().getState());
  }

  @Test
  public void cancelIsNotSupported() {
    assertThrows(UnsupportedOperationException.class, () -> newJob().cancel());
  }

  @Test
  public void waitUntilFinishWithDurationIsNotSupported() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> newJob().waitUntilFinish(Duration.standardSeconds(1)));
  }

  @Test
  public void waitUntilFinishIsNotSupported() {
    assertThrows(UnsupportedOperationException.class, () -> newJob().waitUntilFinish());
  }

  @Test
  public void metricsIsNotSupported() {
    assertThrows(UnsupportedOperationException.class, () -> newJob().metrics());
  }
}
