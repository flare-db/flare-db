package com.flaredb.runner;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceBlockingStub;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.portability.CloseableResource;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;

/**
 * Represents a FlareDB pipeline job submitted for execution.
 *
 * <p>This class is returned by {@link FlareRunner#run(org.apache.beam.sdk.Pipeline)} after a
 * successful job submission. It encapsulates the Beam Job API job identifier and implements the
 * {@link PipelineResult} contract.
 *
 * <p>{@link #getState()}, {@link #cancel()}, and {@link #metrics()} are backed by live calls to the
 * Job Service. {@link #waitUntilFinish} is not currently supported.
 *
 * <p>Owns the job-service channel handed to it by {@link FlareRunner#run}; callers that want to
 * release it deterministically (rather than leaving it open for further {@link #getState()} or
 * {@link #cancel()} calls) should call {@link #close()}.
 */
public class FlarePipelineJob implements PipelineResult, AutoCloseable {

  private final String jobId;
  private final int jobServerTimeout;
  private final CloseableResource<JobServiceBlockingStub> jobService;

  FlarePipelineJob(
      String jobId, int jobServerTimeout, CloseableResource<JobServiceBlockingStub> jobService) {
    this.jobId = jobId;
    this.jobServerTimeout = jobServerTimeout;
    this.jobService = jobService;
  }

  /** Returns the FlareDB Job API identifier for this run. */
  public String getJobId() {
    return jobId;
  }

  @Override
  public State getState() {
    JobStateEvent response =
        jobService
            .get()
            .withDeadlineAfter(jobServerTimeout, TimeUnit.SECONDS)
            .getState(GetJobStateRequest.newBuilder().setJobId(jobId).build());
    return toPipelineState(response.getState());
  }

  @Override
  public State cancel() {
    CancelJobResponse response =
        jobService
            .get()
            .withDeadlineAfter(jobServerTimeout, TimeUnit.SECONDS)
            .cancel(CancelJobRequest.newBuilder().setJobId(jobId).build());
    return toPipelineState(response.getState());
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
    GetJobMetricsResponse response =
        jobService
            .get()
            .withDeadlineAfter(jobServerTimeout, TimeUnit.SECONDS)
            .getJobMetrics(GetJobMetricsRequest.newBuilder().setJobId(jobId).build());

    MetricsContainerStepMap attempted = new MetricsContainerStepMap();
    if (response.hasMetrics()) {
      attempted.getUnboundContainer().update(response.getMetrics().getAttemptedList());
    }
    return MetricsContainerStepMap.asAttemptedOnlyMetricResults(attempted);
  }

  @Override
  public void close() throws IOException {
    try {
      jobService.close();
    } catch (CloseableResource.CloseException e) {
      throw new IOException("Error closing job service channel", e);
    }
  }

  /**
   * Maps a Job API {@link JobState.Enum} to the corresponding Beam SDK {@link State}.
   *
   * <p>{@code STARTING} and {@code CANCELLING} are non-terminal transitional states with no exact
   * {@link State} counterpart; both map to {@link State#RUNNING} since the job is neither done nor
   * safe to treat as terminal yet. {@code UPDATING} maps to {@link State#RUNNING} for the same
   * reason.
   */
  private static State toPipelineState(JobState.Enum jobState) {
    switch (jobState) {
      case STOPPED:
        return State.STOPPED;
      case RUNNING:
      case STARTING:
      case CANCELLING:
      case UPDATING:
        return State.RUNNING;
      case DONE:
        return State.DONE;
      case FAILED:
        return State.FAILED;
      case CANCELLED:
        return State.CANCELLED;
      case UPDATED:
        return State.UPDATED;
      case DRAINING:
        return State.DRAINING;
      case DRAINED:
        return State.DRAINED;
      case UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        return State.UNKNOWN;
    }
  }
}
