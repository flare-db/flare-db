//! Beam trigger semantics: when a window fires, and how each firing is described.
//!
//! A [`Trigger`] is parsed from the portable `WindowingStrategy.trigger` proto and
//! run by a [`TriggerRunner`], a small state machine mirroring Beam's
//! `TriggerStateMachine`: it consumes elements (`on_element`), is asked whether it
//! is ready to fire (`should_fire`) given the current watermark/processing time,
//! and is told when it fired (`on_fire`) so once-triggers latch and repeating
//! triggers reset.
//!
//! This is the "ungrouping" half of the streams-and-tables model: grouping turns a
//! stream into a table, and the trigger drives table → stream conversion. The
//! windowing strategy also carries the [`AccumulationMode`] (does a new pane
//! replace or extend the prior one) and `allowed_lateness` (how long a window
//! remains open after its end).
//!
//! Supported trigger specifications are the standard Beam set: `Default`
//! (`AfterWatermark.pastEndOfWindow`), `AfterWatermark` with early/late firings,
//! `AfterProcessingTime`, `ElementCount`, `Always`, `Never`, `AfterAll`,
//! `AfterAny`, `AfterEach`, `Repeat`, and `OrFinally`.

use beam_model_rs::v1::{
    Trigger as TriggerProto, WindowingStrategy, accumulation_mode, timestamp_transform,
    trigger as trigger_proto,
};

use crate::coders::primitives::{PaneInfo, PaneTiming};

/// A parsed Beam trigger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Trigger {
    /// The default: `AfterWatermark.pastEndOfWindow()` — one on-time pane.
    Default,
    /// Never fires until the window expires (a single on-time pane at expiration).
    Never,
    /// Ready on every evaluation.
    Always,
    /// Ready after `count` elements have arrived in the pane.
    ElementCount {
        count: u64,
    },
    /// Ready `delay_millis` after the first element arrives in the pane.
    AfterProcessingTime {
        delay_millis: i64,
    },
    /// Approximated as [`Trigger::Always`]: the runner treats upstream as caught
    /// up. (Beam uses this to synchronize with upstream processing time.)
    AfterSynchronizedProcessingTime,
    /// Ready via `early` before the end of the window and via `late` after the
    /// on-time firing. A missing `late` means "always" (the Beam default).
    AfterWatermark {
        early: Option<Box<Trigger>>,
        late: Option<Box<Trigger>>,
    },
    AfterAll(Vec<Trigger>),
    AfterAny(Vec<Trigger>),
    AfterEach(Vec<Trigger>),
    Repeat(Box<Trigger>),
    OrFinally {
        main: Box<Trigger>,
        finally: Box<Trigger>,
    },
}

impl Trigger {
    /// Parse the trigger from a `WindowingStrategy.trigger` proto. An absent
    /// trigger is the Beam default.
    pub fn from_proto(proto: Option<&TriggerProto>) -> Trigger {
        use trigger_proto::Trigger as T;
        let Some(inner) = proto.and_then(|proto| proto.trigger.as_ref()) else {
            return Trigger::Default;
        };
        match inner {
            T::Default(_) => Trigger::Default,
            T::Never(_) => Trigger::Never,
            T::Always(_) => Trigger::Always,
            T::ElementCount(ec) => Trigger::ElementCount {
                count: ec.element_count.max(1) as u64,
            },
            T::AfterProcessingTime(apt) => Trigger::AfterProcessingTime {
                delay_millis: total_delay_millis(apt),
            },
            T::AfterSynchronizedProcessingTime(_) => Trigger::AfterSynchronizedProcessingTime,
            T::AfterEndOfWindow(eow) => Trigger::AfterWatermark {
                early: eow
                    .early_firings
                    .as_ref()
                    .map(|t| Box::new(Trigger::from_proto(Some(t.as_ref())))),
                late: eow
                    .late_firings
                    .as_ref()
                    .map(|t| Box::new(Trigger::from_proto(Some(t.as_ref())))),
            },
            T::AfterAll(all) => Trigger::AfterAll(parse_all(&all.subtriggers)),
            T::AfterAny(any) => Trigger::AfterAny(parse_all(&any.subtriggers)),
            T::AfterEach(each) => Trigger::AfterEach(parse_all(&each.subtriggers)),
            T::Repeat(repeat) => {
                Trigger::Repeat(Box::new(Trigger::from_proto(repeat.subtrigger.as_deref())))
            }
            T::OrFinally(or) => Trigger::OrFinally {
                main: Box::new(Trigger::from_proto(or.main.as_deref())),
                finally: Box::new(Trigger::from_proto(or.finally.as_deref())),
            },
        }
    }
}

fn parse_all(subtriggers: &[TriggerProto]) -> Vec<Trigger> {
    subtriggers
        .iter()
        .map(|t| Trigger::from_proto(Some(t)))
        .collect()
}

/// Sum the delays in an `AfterProcessingTime` chain. Alignment transforms are
/// ignored (documented approximation); a pure delay chain is the common case.
fn total_delay_millis(apt: &trigger_proto::AfterProcessingTime) -> i64 {
    let mut total = 0i64;
    for transform in &apt.timestamp_transforms {
        if let Some(timestamp_transform::TimestampTransform::Delay(delay)) =
            transform.timestamp_transform.as_ref()
        {
            total += delay.delay_millis.max(0);
        }
    }
    total
}

/// How a new pane relates to the prior pane for the same window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AccumulationMode {
    /// The aggregation is reset after each firing.
    #[default]
    Discarding,
    /// The aggregation accumulates across firings.
    Accumulating,
    /// Outputs retractions of the prior pane (unsupported; treated as accumulating).
    Retracting,
}

impl AccumulationMode {
    fn from_proto(value: i32) -> Self {
        match accumulation_mode::Enum::try_from(value) {
            Ok(accumulation_mode::Enum::Accumulating) | Ok(accumulation_mode::Enum::Retracting) => {
                AccumulationMode::Accumulating
            }
            _ => AccumulationMode::Discarding,
        }
    }
}

/// The trigger-relevant parts of a windowing strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerSpec {
    pub trigger: Trigger,
    pub accumulation: AccumulationMode,
    /// Milliseconds past the end of a window before it becomes droppable.
    pub allowed_lateness: i64,
}

impl TriggerSpec {
    /// Resolve a spec from a windowing strategy; absent strategy means the Beam
    /// defaults (default trigger, discarding, no lateness).
    pub fn from_windowing_strategy(strategy: Option<&WindowingStrategy>) -> Self {
        match strategy {
            None => Self {
                trigger: Trigger::Default,
                accumulation: AccumulationMode::Discarding,
                allowed_lateness: 0,
            },
            Some(strategy) => Self {
                trigger: Trigger::from_proto(strategy.trigger.as_ref()),
                accumulation: AccumulationMode::from_proto(strategy.accumulation_mode),
                allowed_lateness: strategy.allowed_lateness,
            },
        }
    }

    /// The watermark at which a window becomes expired: its end plus the allowed
    /// lateness. Once the output watermark passes this, the window's state may be
    /// garbage-collected and later data is dropped as too-late.
    pub fn earliest_completion(&self, window_end_millis: i64) -> i64 {
        window_end_millis.saturating_add(self.allowed_lateness)
    }
}

/// Inputs to a trigger evaluation.
#[derive(Debug, Clone, Copy)]
pub struct TriggerContext {
    /// Whether the input watermark has passed the end of the window.
    pub end_of_window: bool,
    /// Whether the window has expired (watermark past end-of-window plus the
    /// allowed lateness), i.e. its state is droppable and no farther firings
    /// occur.
    pub expired: bool,
    /// Current processing time in epoch milliseconds.
    pub processing_time: i64,
}

/// Per-window trigger state, mirroring the trigger tree.
#[derive(Debug, Clone, Default)]
struct NodeState {
    fired_once: bool,
    count: u64,
    first_arrival: Option<i64>,
    index: usize,
    finished: bool,
    children: Vec<NodeState>,
}

fn fresh(trigger: &Trigger) -> NodeState {
    let mut state = NodeState::default();
    match trigger {
        Trigger::AfterWatermark { early, late } => {
            state.children = vec![
                early.as_ref().map(|t| fresh(t)).unwrap_or_default(),
                late.as_ref().map(|t| fresh(t)).unwrap_or_default(),
            ];
        }
        Trigger::AfterAll(subs) | Trigger::AfterAny(subs) | Trigger::AfterEach(subs) => {
            state.children = subs.iter().map(fresh).collect();
        }
        Trigger::Repeat(sub) => state.children = vec![fresh(sub)],
        Trigger::OrFinally { main, finally } => {
            state.children = vec![fresh(main), fresh(finally)];
        }
        Trigger::Default
        | Trigger::Never
        | Trigger::Always
        | Trigger::ElementCount { .. }
        | Trigger::AfterProcessingTime { .. }
        | Trigger::AfterSynchronizedProcessingTime => {}
    }
    state
}

/// A stateful run of a trigger for one window.
#[derive(Debug, Clone)]
pub struct TriggerRunner {
    trigger: Trigger,
    state: NodeState,
}

impl TriggerRunner {
    pub fn new(trigger: Trigger) -> Self {
        let state = fresh(&trigger);
        Self { trigger, state }
    }

    /// Feed one element arriving in this window.
    pub fn on_element(&mut self, processing_time: i64) {
        element(&self.trigger, &mut self.state, processing_time);
    }

    /// Whether the trigger is ready to fire now.
    pub fn should_fire(&self, ctx: TriggerContext) -> bool {
        should_fire(&self.trigger, &self.state, ctx)
    }

    /// Record that a firing happened, latching once-triggers and resetting
    /// repeating ones.
    pub fn on_fire(&mut self, ctx: TriggerContext) {
        fire(&self.trigger, &mut self.state, ctx);
    }

    /// Whether the trigger has finished and will never fire again.
    pub fn is_finished(&self) -> bool {
        self.state.finished
    }
}

fn element(trigger: &Trigger, state: &mut NodeState, processing_time: i64) {
    match trigger {
        Trigger::ElementCount { .. } => state.count += 1,
        Trigger::AfterProcessingTime { .. } => {
            if state.first_arrival.is_none() {
                state.first_arrival = Some(processing_time);
            }
        }
        Trigger::AfterWatermark { early, late } => {
            if !state.fired_once {
                if let Some(early) = early {
                    element(early, &mut state.children[0], processing_time);
                }
            } else if let Some(late) = late {
                element(late, &mut state.children[1], processing_time);
            }
        }
        Trigger::AfterAll(subs) | Trigger::AfterAny(subs) | Trigger::AfterEach(subs) => {
            for (sub, child) in subs.iter().zip(state.children.iter_mut()) {
                element(sub, child, processing_time);
            }
        }
        Trigger::Repeat(sub) => {
            if let Some(child) = state.children.first_mut() {
                element(sub, child, processing_time);
            }
        }
        Trigger::OrFinally { main, finally } => {
            element(main, &mut state.children[0], processing_time);
            element(finally, &mut state.children[1], processing_time);
        }
        Trigger::Default
        | Trigger::Never
        | Trigger::Always
        | Trigger::AfterSynchronizedProcessingTime => {}
    }
}

fn should_fire(trigger: &Trigger, state: &NodeState, ctx: TriggerContext) -> bool {
    match trigger {
        Trigger::Default => ctx.end_of_window && !state.fired_once,
        // `Never` produces only the final on-time pane, at window expiration.
        Trigger::Never => ctx.expired && !state.fired_once,
        Trigger::Always | Trigger::AfterSynchronizedProcessingTime => true,
        Trigger::ElementCount { count } => state.count >= *count && !state.fired_once,
        Trigger::AfterProcessingTime { delay_millis } => {
            state
                .first_arrival
                .map(|first| ctx.processing_time >= first + delay_millis)
                .unwrap_or(false)
                && !state.fired_once
        }
        Trigger::AfterWatermark { early, late } => {
            if !ctx.end_of_window {
                early
                    .as_ref()
                    .map(|early| should_fire(early, &state.children[0], ctx))
                    .unwrap_or(false)
            } else if !state.fired_once {
                true
            } else {
                late.as_ref()
                    .map(|late| should_fire(late, &state.children[1], ctx))
                    .unwrap_or(true)
            }
        }
        Trigger::AfterAll(subs) => subs
            .iter()
            .zip(state.children.iter())
            .all(|(sub, child)| should_fire(sub, child, ctx)),
        Trigger::AfterAny(subs) => subs
            .iter()
            .zip(state.children.iter())
            .any(|(sub, child)| should_fire(sub, child, ctx)),
        Trigger::AfterEach(subs) => subs
            .get(state.index)
            .map(|sub| should_fire(sub, &state.children[state.index], ctx))
            .unwrap_or(false),
        Trigger::Repeat(sub) => state
            .children
            .first()
            .map(|child| should_fire(sub, child, ctx))
            .unwrap_or(false),
        Trigger::OrFinally { main, finally } => {
            !state.finished
                && (should_fire(main, &state.children[0], ctx)
                    || should_fire(finally, &state.children[1], ctx))
        }
    }
}

fn fire(trigger: &Trigger, state: &mut NodeState, ctx: TriggerContext) {
    match trigger {
        Trigger::Default
        | Trigger::Never
        | Trigger::ElementCount { .. }
        | Trigger::AfterProcessingTime { .. } => {
            state.fired_once = true;
        }
        Trigger::AfterWatermark { early, late } => {
            if !ctx.end_of_window {
                // An early firing fires the early subtrigger; it must not latch the
                // on-time flag.
                if let Some(early) = early {
                    fire(early, &mut state.children[0], ctx);
                }
            } else if !state.fired_once {
                state.fired_once = true;
                if let Some(early) = early {
                    state.children[0] = fresh(early);
                }
            } else if let Some(late) = late {
                fire(late, &mut state.children[1], ctx);
            }
        }
        Trigger::Always | Trigger::AfterSynchronizedProcessingTime => {}
        Trigger::AfterAll(subs) | Trigger::AfterAny(subs) => {
            for (sub, child) in subs.iter().zip(state.children.iter_mut()) {
                fire(sub, child, ctx);
            }
        }
        Trigger::AfterEach(subs) => {
            if let Some(sub) = subs.get(state.index) {
                fire(sub, &mut state.children[state.index], ctx);
            }
            state.index += 1;
            if state.index >= subs.len() {
                state.finished = true;
            }
        }
        Trigger::Repeat(sub) => {
            if let Some(child) = state.children.first_mut() {
                fire(sub, child, ctx);
                *child = fresh(sub);
            }
        }
        Trigger::OrFinally { main, finally } => {
            if should_fire(finally, &state.children[1], ctx) {
                state.finished = true;
            } else {
                fire(main, &mut state.children[0], ctx);
            }
        }
    }
}

/// Build the [`PaneInfo`] for a firing.
///
/// `timing` is early for a speculative (pre-watermark) firing, on-time for the
/// first firing after the window end, and late thereafter.
pub fn pane_info(timing: PaneTiming, index: i64, is_first: bool, is_last: bool) -> PaneInfo {
    PaneInfo {
        is_first,
        is_last,
        timing,
        index,
        non_speculative_index: if timing == PaneTiming::Early {
            -1
        } else {
            index
        },
    }
}

/// The pane timing for a firing at `end_of_window`, given whether it is the
/// window's first firing.
pub fn pane_timing(end_of_window: bool, is_first_firing: bool) -> PaneTiming {
    if !end_of_window {
        PaneTiming::Early
    } else if is_first_firing {
        PaneTiming::OnTime
    } else {
        PaneTiming::Late
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beam_model_rs::v1::{
        Trigger as TriggerProto, WindowingStrategy,
        trigger::{AfterEndOfWindow, Default as ProtoDefault, ElementCount, Never},
    };

    fn ctx(end_of_window: bool) -> TriggerContext {
        TriggerContext {
            end_of_window,
            expired: end_of_window,
            processing_time: 1_000,
        }
    }

    fn wrap(inner: trigger_proto::Trigger) -> TriggerProto {
        TriggerProto {
            trigger: Some(inner),
        }
    }

    #[test]
    fn absent_trigger_is_the_default_and_fires_once_at_end_of_window() {
        assert_eq!(Trigger::from_proto(None), Trigger::Default);

        let mut runner = TriggerRunner::new(Trigger::Default);
        assert!(!runner.should_fire(ctx(false)));
        assert!(runner.should_fire(ctx(true)));

        runner.on_fire(ctx(true));
        // Default fires exactly once.
        assert!(!runner.should_fire(ctx(true)));
    }

    #[test]
    fn element_count_waits_for_the_count_then_latches() {
        let mut runner = TriggerRunner::new(Trigger::ElementCount { count: 3 });
        runner.on_element(0);
        runner.on_element(0);
        assert!(!runner.should_fire(ctx(false)));
        runner.on_element(0);
        assert!(runner.should_fire(ctx(false)));
        runner.on_fire(ctx(false));
        assert!(!runner.should_fire(ctx(false)), "a once-trigger latches");
    }

    #[test]
    fn after_processing_time_fires_only_after_the_delay() {
        let mut runner = TriggerRunner::new(Trigger::AfterProcessingTime { delay_millis: 50 });
        runner.on_element(1_000);
        assert!(!runner.should_fire(TriggerContext {
            end_of_window: false,
            expired: false,
            processing_time: 1_040,
        }));
        assert!(runner.should_fire(TriggerContext {
            end_of_window: false,
            expired: false,
            processing_time: 1_050,
        }));
    }

    #[test]
    fn after_watermark_runs_early_then_on_time_then_late() {
        // early = AfterPane.elementCount(1), late = AfterPane.elementCount(1)
        let trigger = Trigger::AfterWatermark {
            early: Some(Box::new(Trigger::ElementCount { count: 1 })),
            late: Some(Box::new(Trigger::ElementCount { count: 1 })),
        };
        let mut runner = TriggerRunner::new(trigger);

        // Before the end of the window with no elements: no early firing.
        assert!(!runner.should_fire(ctx(false)));

        // One element arrives: the early trigger is ready.
        runner.on_element(0);
        assert!(runner.should_fire(ctx(false)));
        runner.on_fire(ctx(false));

        // End of window: the on-time firing.
        assert!(runner.should_fire(ctx(true)));
        runner.on_fire(ctx(true));

        // After on-time, the late trigger needs a new element.
        assert!(!runner.should_fire(ctx(true)));
        runner.on_element(0);
        assert!(runner.should_fire(ctx(true)));
    }

    #[test]
    fn after_watermark_defaults_late_to_always() {
        let trigger = Trigger::AfterWatermark {
            early: None,
            late: None,
        };
        let mut runner = TriggerRunner::new(trigger);
        assert!(runner.should_fire(ctx(true)));
        runner.on_fire(ctx(true));
        // No explicit late trigger means "always".
        assert!(runner.should_fire(ctx(true)));
    }

    #[test]
    fn never_fires_only_at_window_expiration() {
        let mut runner = TriggerRunner::new(Trigger::Never);
        // Passed end of window but not yet expired.
        assert!(!runner.should_fire(TriggerContext {
            end_of_window: true,
            expired: false,
            processing_time: 0,
        }));
        assert!(runner.should_fire(ctx(true)));
        runner.on_fire(ctx(true));
        assert!(!runner.should_fire(ctx(true)));
    }

    #[test]
    fn repeat_resets_its_subtrigger_after_each_firing() {
        let mut runner = TriggerRunner::new(Trigger::Repeat(Box::new(Trigger::ElementCount {
            count: 2,
        })));
        runner.on_element(0);
        assert!(!runner.should_fire(ctx(false)));
        runner.on_element(0);
        assert!(runner.should_fire(ctx(false)));
        runner.on_fire(ctx(false));
        // Reset: needs two more.
        assert!(!runner.should_fire(ctx(false)));
        runner.on_element(0);
        runner.on_element(0);
        assert!(runner.should_fire(ctx(false)));
    }

    #[test]
    fn after_all_requires_every_subtrigger() {
        let mut runner = TriggerRunner::new(Trigger::AfterAll(vec![
            Trigger::ElementCount { count: 2 },
            Trigger::ElementCount { count: 3 },
        ]));
        runner.on_element(0);
        runner.on_element(0);
        assert!(!runner.should_fire(ctx(false)), "second subtrigger needs 3");
        runner.on_element(0);
        assert!(runner.should_fire(ctx(false)));
    }

    #[test]
    fn after_any_needs_one_subtrigger() {
        let mut runner = TriggerRunner::new(Trigger::AfterAny(vec![
            Trigger::ElementCount { count: 5 },
            Trigger::ElementCount { count: 1 },
        ]));
        runner.on_element(0);
        assert!(runner.should_fire(ctx(false)));
    }

    #[test]
    fn after_each_advances_through_subtriggers_and_finishes() {
        // Elements feed every subtrigger, so use distinct thresholds to make the
        // advance observable.
        let mut runner = TriggerRunner::new(Trigger::AfterEach(vec![
            Trigger::ElementCount { count: 1 },
            Trigger::ElementCount { count: 2 },
        ]));
        runner.on_element(0);
        assert!(runner.should_fire(ctx(false)));
        runner.on_fire(ctx(false));
        // Advanced to the second subtrigger, which needs a second element.
        assert!(!runner.should_fire(ctx(false)));
        runner.on_element(0);
        assert!(runner.should_fire(ctx(false)));
        runner.on_fire(ctx(false));
        assert!(runner.is_finished());
        assert!(!runner.should_fire(ctx(false)));
    }

    #[test]
    fn or_finally_finishes_when_the_finally_trigger_fires() {
        let mut runner = TriggerRunner::new(Trigger::OrFinally {
            main: Box::new(Trigger::ElementCount { count: 100 }),
            finally: Box::new(Trigger::ElementCount { count: 1 }),
        });
        runner.on_element(0);
        assert!(runner.should_fire(ctx(false)));
        runner.on_fire(ctx(false));
        assert!(runner.is_finished());
    }

    #[test]
    fn parses_nested_proto_triggers() {
        // AfterEndOfWindow { late: Default } with no early firings.
        let after_end_of_window = wrap(trigger_proto::Trigger::AfterEndOfWindow(Box::new(
            AfterEndOfWindow {
                early_firings: None,
                late_firings: Some(Box::new(wrap(trigger_proto::Trigger::Default(
                    ProtoDefault {},
                )))),
            },
        )));
        assert_eq!(
            Trigger::from_proto(Some(&after_end_of_window)),
            Trigger::AfterWatermark {
                early: None,
                late: Some(Box::new(Trigger::Default)),
            }
        );

        let inner = trigger_proto::Trigger::AfterAny(trigger_proto::AfterAny {
            subtriggers: vec![
                wrap(trigger_proto::Trigger::ElementCount(ElementCount {
                    element_count: 2,
                })),
                wrap(trigger_proto::Trigger::Never(Never {})),
            ],
        });
        assert_eq!(
            Trigger::from_proto(Some(&wrap(inner))),
            Trigger::AfterAny(vec![Trigger::ElementCount { count: 2 }, Trigger::Never])
        );
    }

    #[test]
    fn spec_reads_accumulation_and_lateness_from_the_strategy() {
        let strategy = WindowingStrategy {
            allowed_lateness: 500,
            accumulation_mode: accumulation_mode::Enum::Accumulating as i32,
            ..WindowingStrategy::default()
        };
        let spec = TriggerSpec::from_windowing_strategy(Some(&strategy));
        assert_eq!(spec.trigger, Trigger::Default);
        assert_eq!(spec.accumulation, AccumulationMode::Accumulating);
        assert_eq!(spec.allowed_lateness, 500);
        assert_eq!(spec.earliest_completion(1_000), 1_500);
    }

    #[test]
    fn pane_timing_reflects_early_on_time_and_late() {
        assert_eq!(pane_timing(false, true), PaneTiming::Early);
        assert_eq!(pane_timing(true, true), PaneTiming::OnTime);
        assert_eq!(pane_timing(true, false), PaneTiming::Late);

        let early = pane_info(PaneTiming::Early, 0, true, false);
        assert_eq!(early.non_speculative_index, -1);
        let on_time = pane_info(PaneTiming::OnTime, 0, true, true);
        assert_eq!(on_time.timing, PaneTiming::OnTime);
    }
}
