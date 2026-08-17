use anyhow::Result;

use crate::engine::harness::{
    control::{ControlChannel, FlareControlService, start_control_server},
    data::{DataChannel, FlareDataService, start_data_server},
    log::{FlareLogService, LogChannel, start_log_server},
    state::{FlareStateService, StateChannel, start_state_server},
};

pub mod control;
pub mod data;
pub mod log;
pub mod state;

pub struct Channels {
    /// Control plane
    control: ControlChannel,
    /// Data plane
    data: DataChannel,
    /// Logging channel
    log: LogChannel,
    /// State API channel.
    state: StateChannel,
}

impl Channels {
    /// Create a builder for the harness channels and their gRPC services.
    pub fn builder() -> ChannelsBuilder {
        ChannelsBuilder
    }

    /// Split the channel bundle into the channels expected by the executor.
    pub fn into_parts(self) -> (ControlChannel, DataChannel, LogChannel, StateChannel) {
        (self.control, self.data, self.log, self.state)
    }
    // Todo: Move reset channels here
    // wait_connected()
}

/// The gRPC services that correspond to a [`Channels`] bundle.
///
/// Keeping the services paired with the channels makes it harder to
/// accidentally register a service from one harness with channels from
/// another harness.
pub struct ChannelServices {
    pub control: FlareControlService,
    pub data: FlareDataService,
    pub log: FlareLogService,
    pub state: FlareStateService,
}

/// Builder for a complete set of Beam Fn harness channels.
#[derive(Debug, Default, Clone, Copy)]
pub struct ChannelsBuilder;

impl ChannelsBuilder {
    /// Start all harness servers and build their matching channels.
    pub async fn build(self) -> Result<(Channels, ChannelServices)> {
        let (control, control_service) = start_control_server().await?;
        let (data, data_service) = start_data_server().await?;
        let (log, log_service) = start_log_server().await?;
        let (state, state_service) = start_state_server().await?;

        Ok((
            Channels {
                control,
                data,
                log,
                state,
            },
            ChannelServices {
                control: control_service,
                data: data_service,
                log: log_service,
                state: state_service,
            },
        ))
    }
}
