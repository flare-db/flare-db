package com.flaredb.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceBlockingStub;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.GaugeData;
import org.apache.beam.runners.core.metrics.MonitoringInfoEncodings;
import org.apache.beam.runners.portability.CloseableResource;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Test;

/**
 * {@link FlarePipelineJob#getState()}, {@link FlarePipelineJob#cancel()}, and {@link
 * FlarePipelineJob#metrics()} are backed by live Job Service calls, exercised here against an
 * in-process fake server. {@link FlarePipelineJob#waitUntilFinish} remains unimplemented.
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
    private final GetJobMetricsResponse metricsResponse;
    final AtomicReference<String> lastGetStateJobId = new AtomicReference<>();
    final AtomicReference<String> lastCancelJobId = new AtomicReference<>();
    final AtomicReference<String> lastGetJobMetricsJobId = new AtomicReference<>();

    FakeJobService(JobState.Enum state) {
      this(state, GetJobMetricsResponse.getDefaultInstance());
    }

    FakeJobService(JobState.Enum state, GetJobMetricsResponse metricsResponse) {
      this.state = state;
      this.metricsResponse = metricsResponse;
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

    @Override
    public void getJobMetrics(
        GetJobMetricsRequest request, StreamObserver<GetJobMetricsResponse> responseObserver) {
      lastGetJobMetricsJobId.set(request.getJobId());
      responseObserver.onNext(metricsResponse);
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
  public void exposesTheJobId() throws Exception {
    assertEquals("job-42", newJob("job-42", new FakeJobService(JobState.Enum.RUNNING)).getJobId());
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
  public void metricsQueriesAttemptedCounters() throws Exception {
    MonitoringInfo counter =
        MonitoringInfo.newBuilder()
            .setUrn("beam:metric:user:sum_int64:v1")
            .setType("beam:metrics:sum_int64:v1")
            .setPayload(MonitoringInfoEncodings.encodeInt64Counter(7))
            .putLabels("NAMESPACE", "test")
            .putLabels("NAME", "count")
            .putLabels("PTRANSFORM", "step")
            .build();
    MonitoringInfo distribution =
        MonitoringInfo.newBuilder()
            .setUrn("beam:metric:user:distribution_int64:v1")
            .setType("beam:metrics:distribution_int64:v1")
            .setPayload(
                MonitoringInfoEncodings.encodeInt64Distribution(
                    DistributionData.create(10, 2, 4, 6)))
            .putLabels("NAMESPACE", "test")
            .putLabels("NAME", "latency")
            .putLabels("PTRANSFORM", "step")
            .build();
    MonitoringInfo gauge =
        MonitoringInfo.newBuilder()
            .setUrn("beam:metric:user:latest_int64:v1")
            .setType("beam:metrics:latest_int64:v1")
            .setPayload(
                MonitoringInfoEncodings.encodeInt64Gauge(GaugeData.create(9, new Instant(200))))
            .putLabels("NAMESPACE", "test")
            .putLabels("NAME", "last")
            .putLabels("PTRANSFORM", "step")
            .build();
    GetJobMetricsResponse response =
        GetJobMetricsResponse.newBuilder()
            .setMetrics(
                org.apache.beam.model.jobmanagement.v1.JobApi.MetricResults.newBuilder()
                    .addAttempted(counter)
                    .addAttempted(distribution)
                    .addAttempted(gauge))
            .build();
    FakeJobService fakeJobService = new FakeJobService(JobState.Enum.DONE, response);
    FlarePipelineJob job = newJob("job-42", fakeJobService);

    Iterator<MetricResult<Long>> counters =
        job.metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named("test", "count"))
                    .build())
            .getCounters()
            .iterator();

    assertEquals(Long.valueOf(7), counters.next().getAttempted());
    assertEquals(false, counters.hasNext());

    MetricResult<DistributionResult> distributionResult =
        job.metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named("test", "latency"))
                    .build())
            .getDistributions()
            .iterator()
            .next();
    assertEquals(10, distributionResult.getAttempted().getSum());
    assertEquals(2, distributionResult.getAttempted().getCount());
    assertEquals(4, distributionResult.getAttempted().getMin());
    assertEquals(6, distributionResult.getAttempted().getMax());

    MetricResult<GaugeResult> gaugeResult =
        job.metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named("test", "last"))
                    .build())
            .getGauges()
            .iterator()
            .next();
    assertEquals(9, gaugeResult.getAttempted().getValue());
    assertEquals(new Instant(200), gaugeResult.getAttempted().getTimestamp());
    assertEquals("job-42", fakeJobService.lastGetJobMetricsJobId.get());
  }
}
