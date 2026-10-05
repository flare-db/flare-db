use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use beam_model_rs::v1::{
    Coder, Components, Environment, PCollection, PTransform, WindowingStrategy,
};
use uuid::Uuid;

use crate::{
    jobservice::urns::beam_urns,
    store::element_store::FlareElementStore,
    transforms::{flatten::Flatten, gbk::GroupByKey, impluse::Impulse},
    utils::teststream::TestStream,
};

pub mod flatten;
pub mod gbk;
pub mod impluse;

#[async_trait]
pub trait FlareTransform {
    fn urn() -> &'static str
    where
        Self: Sized;

    fn id(&self) -> String;

    fn with(
        id: String,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
        name: String,
    ) -> Self
    where
        Self: Sized;

    async fn execute(&self, ctx: ExecutionContext) -> Result<(), anyhow::Error>;
    //-> Result<Elements, TransformError>;

    fn output_pcol_ids(&self) -> HashSet<String>;

    fn unique_name(&self) -> String;

    fn windowing_strategies(&self) -> HashMap<String, WindowingStrategy>;

    fn coders(&self) -> HashMap<String, Coder>;

    fn environments(&self) -> HashMap<String, Environment>;

    fn transfrom_spec(&self) -> HashMap<String, PTransform>;

    fn pcollections(&self, components: &Components) -> HashMap<String, PCollection>;

    /// Whether this transform synthesizes its output from encoded element bytes
    /// and therefore needs the output PCollection's element coder to decode them.
    /// Only runner *sources* that replay pre-encoded elements (e.g. `TestStream`)
    /// return `true`.
    fn needs_output_coder(&self) -> bool {
        false
    }

    /// Whether completing this stage's bundle marks it a finished source.
    ///
    /// A bounded source (e.g. `Impulse`) emits everything in one bundle, so its
    /// completion is its end. A runner source that drives an event stream
    /// (`TestStream`) returns `false` here and reports completion explicitly, so it
    /// can run again for the next scripted event.
    fn source_auto_finishes(&self) -> bool {
        true
    }

    // TODO: add methods needed to build the ProcessBundleDescriptor object.
}

// Idea: TransformConfig as input to with as we start adding more parameters

/*pub struct TransformConfig {
    pub id: String,
    pub name: String,
    pub inputs: HashMap<String, String>,
    pub outputs: HashMap<String, String>,
    pub display_name: Option<String>,
    pub environment_id: Option<String>,
    pub side_inputs: HashMap<String, String>,
} */

pub struct ExecutionContext {
    //pub instruction_id: String,
    ///pub transform_id: String,
    pub store: Arc<FlareElementStore>,
    /// Input PCollections, in the order their incoming edges were discovered.
    ///
    /// Single-input runner transforms (e.g. [`GroupByKey`]) receive a one-element
    /// list; fan-in transforms such as [`Flatten`] receive all of their inputs so
    /// each can be mapped back to its producer/coder at the data boundary.
    pub input_pcollection_ids: Vec<String>,
    pub output_pcollection_id: String,
    pub consumer_transfrom_id: String, //pub coder: String,
    /// The owning stage's id. Stable across the stage's re-runs, so it is the
    /// reader identity for incremental input reads: a re-run reads only the rows
    /// its upstream appended since the previous run.
    pub stage_id: String,
    /// The owning stage's input windowing strategy, when the executor could
    /// resolve it. Runner-native transforms read its trigger, accumulation mode,
    /// and allowed lateness (see [`trigger::TriggerSpec`]).
    pub windowing_strategy: Option<WindowingStrategy>,
    /// Current processing time in epoch milliseconds, for triggers that fire on
    /// the wall clock (`AfterProcessingTime`).
    pub processing_time: i64,
    /// The owning stage's input watermark at the start of this bundle.
    ///
    /// A windowed aggregation uses this to decide which windows are ready: Beam's
    /// default trigger (`AfterWatermark.pastEndOfWindow`) fires a window once the
    /// input watermark reaches its end, i.e.
    /// `window.max_timestamp_millis() <= input_watermark`.
    pub input_watermark: i64,
    /// The primary output PCollection's element coder, when the transform asked
    /// for it (see [`FlareTransform::needs_output_coder`]). A runner source decodes
    /// its pre-encoded elements with it.
    pub output_coder: Option<crate::coders::StandardBeamCoders>,
    /// Sink for source progress reports (watermark / processing time / finish).
    pub source_reports: Option<SourceReportSink>,
}
pub type FlareRunnerTransform = Arc<dyn FlareTransform + Send + Sync>;

/// A progress report from a runner source that drives time itself.
///
/// A bounded source (`Impulse`) finishes implicitly when its single bundle
/// completes. A `TestStream`, by contrast, plays one scripted event per bundle and
/// must tell the dispatcher two things: how far event time / processing time moved
/// (so watermarks and the clock advance), and whether it still has events left (so
/// the dispatcher re-arms it instead of treating it as finished).
#[derive(Debug, Clone)]
pub struct SourceProgress {
    /// The reporting stage (the transform instance id).
    pub stage_id: String,
    /// New source watermark to report, if this event advanced the watermark.
    pub watermark: Option<i64>,
    /// New processing time (epoch millis) to set, if this event advanced the
    /// processing-time clock.
    pub processing_time: Option<i64>,
    /// The source has emitted all of its events and is finished.
    pub done: bool,
}

/// Sink handed to a runner source through [`ExecutionContext`] so it can report
/// [`SourceProgress`] back to the dispatcher loop.
#[derive(Clone)]
pub struct SourceReportSink(pub(crate) tokio::sync::mpsc::UnboundedSender<SourceProgress>);

impl SourceReportSink {
    /// Send a progress report; a closed receiver means the job is shutting down, in
    /// which case the report is dropped.
    pub fn report(&self, progress: SourceProgress) {
        let _ = self.0.send(progress);
    }
}

pub fn from_urn(
    urn: &str,
    name: String,
    inputs: HashMap<String, String>,
    outputs: HashMap<String, String>,
) -> FlareRunnerTransform {
    from_urn_with_payload(urn, name, inputs, outputs, &[])
}

/// Like [`from_urn`], but forwards a runner transform's `FunctionSpec` payload.
///
/// Runner transforms that are configured by their payload — today only
/// `TestStream`, whose serialized [`TestStreamPayload`] carries the scripted
/// events — read it here. Most transforms ignore it.
pub fn from_urn_with_payload(
    urn: &str,
    name: String,
    inputs: HashMap<String, String>,
    outputs: HashMap<String, String>,
    payload: &[u8],
) -> FlareRunnerTransform {
    let transform: FlareRunnerTransform = match urn {
        beam_urns::IMPULSE_TRANSFORM => Arc::new(Impulse::with(
            Uuid::new_v4().to_string(),
            inputs,
            outputs,
            name,
        )),
        beam_urns::GROUP_BY_KEY_TRANSFORM => Arc::new(GroupByKey::with(
            Uuid::new_v4().to_string(),
            inputs,
            outputs,
            name,
        )),
        beam_urns::FLATTEN_TRANSFORM => Arc::new(Flatten::with(
            Uuid::new_v4().to_string(),
            inputs,
            outputs,
            name,
        )),
        beam_urns::TEST_STREAM_TRANSFORM => {
            let stream = TestStream::with(Uuid::new_v4().to_string(), inputs, outputs, name);
            stream.set_script(payload);
            Arc::new(stream)
        }
        _ => panic!("Unknown URN {}", urn),
    };
    transform
}
