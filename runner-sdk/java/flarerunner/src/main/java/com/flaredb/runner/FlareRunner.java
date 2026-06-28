package com.flaredb.runner;

import java.util.concurrent.TimeUnit;

import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceBlockingStub;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingService;
import org.apache.beam.runners.portability.CloseableResource;
import org.apache.beam.runners.portability.CloseableResource.CloseException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlareRunner extends PipelineRunner<FlarePipelineJob> {
    private static final Logger LOG = LoggerFactory.getLogger(FlareRunner.class);

    private final FlarePipelineOptions options;

    private final ManagedChannelFactory channelFactory;

    private final String endpoint;

    private FlareRunner(FlarePipelineOptions options, String endpoint, ManagedChannelFactory channelFactory) {

        this.options = options;
        this.endpoint = endpoint;
        this.channelFactory = channelFactory;
    }

    static FlareRunner create(PipelineOptions options, ManagedChannelFactory channelFactory) {
        FlarePipelineOptions flarePipelineOptions = PipelineOptionsValidator.validate(
                FlarePipelineOptions.class,
                options);

        String endpoint = flarePipelineOptions.getJobEndpoint();

        return new FlareRunner(flarePipelineOptions, endpoint, channelFactory);
    }

    public static FlareRunner fromOptions(PipelineOptions options) {

        return create(options, ManagedChannelFactory.createDefault());
    }

    @Override
    public FlarePipelineJob run(Pipeline pipeline) {

        RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline);

        PrepareJobRequest prepareJobRequest = PrepareJobRequest.newBuilder()
                .setJobName(options.getJobName())
                .setPipeline(pipelineProto)
                .setPipelineOptions(PipelineOptionsTranslation.toProto(options))
                .build();

        ManagedChannel jobServiceChannel = channelFactory
                .forDescriptor(ApiServiceDescriptor.newBuilder().setUrl(endpoint).build());

        JobServiceBlockingStub jobService = JobServiceGrpc.newBlockingStub(jobServiceChannel);

        try (CloseableResource<JobServiceBlockingStub> wrappedJobService = CloseableResource.of(jobService,
                unused -> jobServiceChannel.shutdown())) {

            final int jobServerTimeout = options.as(FlarePipelineOptions.class).getJobServerTimeout();
            PrepareJobResponse prepareJobResponse = jobService
                    .withDeadlineAfter(jobServerTimeout, TimeUnit.SECONDS)
                    .withWaitForReady()
                    .prepare(prepareJobRequest);
            LOG.info("PrepareJobResponse received for jobName={}", options.getJobName());

            ApiServiceDescriptor artifactStagingEndpoint = prepareJobResponse.getArtifactStagingEndpoint();
            String stagingSessionToken = prepareJobResponse.getStagingSessionToken();

            try (CloseableResource<ManagedChannel> artifactChannel = CloseableResource.of(
                    channelFactory.forDescriptor(artifactStagingEndpoint), ManagedChannel::shutdown)) {

                ArtifactStagingService.offer(
                        new ArtifactRetrievalService(new FlareArtifactResolver(options)),
                        ArtifactStagingServiceGrpc.newStub(artifactChannel.get()),
                        stagingSessionToken);
            } catch (CloseableResource.CloseException e) {
                LOG.warn("Error closing artifact staging channel", e);
                // CloseExceptions should only be thrown while closing the channel.
            } catch (Exception e) {
                throw new RuntimeException("Error staging files.", e);
            }

            RunJobRequest runJobRequest = RunJobRequest.newBuilder()
                    .setPreparationId(prepareJobResponse.getPreparationId())
                    .build();

            LOG.info("Created run job request: {}", runJobRequest);
            // Run the job and wait for a result, we don't set a timeout here because
            // it may take a long time for a job to complete and streaming
            // jobs never return a response.
            RunJobResponse runJobResponse = jobService.run(runJobRequest);

            LOG.info("RunJobResponse received jobName={}", options.getJobName());
            LOG.info("Job Eexecution Completed");
            ByteString jobId = runJobResponse.getJobIdBytes();

            return new FlarePipelineJob(jobId);
        } catch (CloseException e) {
            throw new RuntimeException(e);
        }
    }

}
