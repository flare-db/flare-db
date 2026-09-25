package com.flaredb.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceBlockingStub;
import org.apache.beam.runners.portability.CloseableResource;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Test;

/**
 * {@link FlarePipelineJob#getState()} and {@link FlarePipelineJob#cancel()} are backed by live
 * Job Service calls, exercised here against an in-process fake server. {@link
 * FlarePipelineJob#waitUntilFinish} and {@link FlarePipelineJob#metrics()} remain unimplemented;
 * those tests pin that contract down so it fails loudly once support is added.
 */
public class FlarePipelineJobTest {

  private Server server;
  private CloseableResource<JobServiceBlockingStub> jobService;

  /**
   * A fake Job Service that returns the given state for both getState and cancel, and records the
   * job id it was called with.
   */
  private static class FakeJobService extends JobServiceGrpc.JobServiceImplBase {
    private final JobState.Enum state;
    final AtomicReference<String> lastGetStateJobId = new AtomicReference<>();
    final AtomicReference<String> lastCancelJobId = new AtomicReference<>();

    FakeJobService(JobState.Enum state) {
      this.state = state;
    }

    @Override
    public void getState(
        GetJobStateRequest request, StreamObserver<JobStateEvent> responseObserver) {
      lastGetStateJobId.set(request.getJobId());
      responseObserver.onNext(JobStateEvent.newBuilder().setState(state).build());
      responseObserver.onCompleted();
    }

    @Override
    public void cancel(
        CancelJobRequest request, StreamObserver<CancelJobResponse> responseObserver) {
      lastCancelJobId.set(request.getJobId());
      responseObserver.onNext(CancelJobResponse.newBuilder().setState(state).build());
      responseObserver.onCompleted();
    }
  }

  private FlarePipelineJob newJob(String jobId, FakeJobService fakeJobService) throws IOException {
    String serverName = "flare-pipeline-job-test-" + System.nanoTime();
    server =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(fakeJobService)
            .build()
            .start();
    JobServiceBlockingStub stub =
        JobServiceGrpc.newBlockingStub(
            InProcessChannelBuilder.forName(serverName).directExecutor().build());
    jobService = CloseableResource.of(stub, unused -> server.shutdownNow());
    return new FlarePipelineJob(jobId, /* jobServerTimeout= */ 60, jobService);
  }

  @After
  public void tearDown() throws Exception {
    if (jobService != null) {
      jobService.close();
    }
  }

  @Test
  public void getStateReturnsMappedStateFromJobService() throws Exception {
    FlarePipelineJob job = newJob("job-1", new FakeJobService(JobState.Enum.RUNNING));

    assertEquals(State.RUNNING, job.getState());
  }

  @Test
  public void getStatePassesThroughTheJobId() throws Exception {
    FakeJobService fakeJobService = new FakeJobService(JobState.Enum.DONE);
    FlarePipelineJob job = newJob("job-42", fakeJobService);

    job.getState();

    assertEquals("job-42", fakeJobService.lastGetStateJobId.get());
  }

  @Test
  public void cancelReturnsMappedStateFromJobService() throws Exception {
    FlarePipelineJob job = newJob("job-1", new FakeJobService(JobState.Enum.CANCELLED));

    assertEquals(State.CANCELLED, job.cancel());
  }

  @Test
  public void cancelPassesThroughTheJobId() throws Exception {
    FakeJobService fakeJobService = new FakeJobService(JobState.Enum.CANCELLED);
    FlarePipelineJob job = newJob("job-42", fakeJobService);

    job.cancel();

    assertEquals("job-42", fakeJobService.lastCancelJobId.get());
  }

  @Test
  public void transitionalStatesMapToRunning() throws Exception {
    assertEquals(
        State.RUNNING, newJob("job-1", new FakeJobService(JobState.Enum.STARTING)).getState());
    assertEquals(
        State.RUNNING, newJob("job-1", new FakeJobService(JobState.Enum.CANCELLING)).getState());
    assertEquals(
        State.RUNNING, newJob("job-1", new FakeJobService(JobState.Enum.UPDATING)).getState());
  }

  @Test
  public void unspecifiedStateMapsToUnknown() throws Exception {
    assertEquals(
        State.UNKNOWN, newJob("job-1", new FakeJobService(JobState.Enum.UNSPECIFIED)).getState());
  }

  @Test
  public void waitUntilFinishWithDurationIsNotSupported() throws Exception {
    FlarePipelineJob job = newJob("job-1", new FakeJobService(JobState.Enum.RUNNING));
    assertThrows(
        UnsupportedOperationException.class,
        () -> job.waitUntilFinish(Duration.standardSeconds(1)));
  }

  @Test
  public void waitUntilFinishIsNotSupported() throws Exception {
    FlarePipelineJob job = newJob("job-1", new FakeJobService(JobState.Enum.RUNNING));
    assertThrows(UnsupportedOperationException.class, job::waitUntilFinish);
  }

  @Test
  public void metricsIsNotSupported() throws Exception {
    FlarePipelineJob job = newJob("job-1", new FakeJobService(JobState.Enum.RUNNING));
    assertThrows(UnsupportedOperationException.class, job::metrics);
  }
}
