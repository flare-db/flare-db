use std::{pin::Pin, sync::Arc};

use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, IpcMessage,
    SchemaAsIpc, Ticket,
    encode::FlightDataEncoderBuilder,
    flight_service_server::FlightServiceServer,
    sql::{CommandStatementQuery, SqlInfo, TicketStatementQuery, server::FlightSqlService},
};

use arrow_ipc::writer::IpcWriteOptions;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::{Stream, StreamExt};
use log::debug;
use paimon::{Catalog, FileSystemCatalog};
use paimon_datafusion::SQLContext;
use tonic::{Request, Response, Status, Streaming};

use crate::store::element_store::create_catalog;

pub struct FlareIO {
    catalog: Arc<FileSystemCatalog>,
    sql_info: DashMap<i32, SqlInfo>,
    sql_ctx: SQLContext,
    statements: DashMap<Vec<u8>, String>,
}

impl FlareIO {
    pub async fn new() -> Result<Self, anyhow::Error> {
        let store_path = crate::utils::path::flare_warehouse_dir();
        let store_base = store_path.to_str().unwrap_or(".").to_string();

        let catalog = create_catalog(store_base, "default".to_string()).await?;

        let catalog = Arc::new(catalog);

        let mut sql_ctx = SQLContext::new();

        sql_ctx
            .register_catalog("flare", Arc::clone(&catalog) as Arc<dyn Catalog>)
            .await?;

        Ok(Self {
            catalog,
            sql_ctx,
            sql_info: DashMap::new(),
            statements: DashMap::new(),
        })
    }

    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }
}

#[async_trait]
impl FlightSqlService for FlareIO {
    type FlightService = Self;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let output = futures::stream::iter(vec![Ok(HandshakeResponse {
            protocol_version: 0,
            payload: Vec::new().into(),
        })]);

        Ok(Response::new(Box::pin(output)))
    }

    async fn register_sql_info(&self, id: i32, result: &SqlInfo) {
        self.sql_info.insert(id, result.clone());
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // validate sql and if referenced catalogs/tables can be resolved.
        let df = self.sql_ctx.sql(&query.query).await.map_err(|err| {
            Status::invalid_argument(format!("Failed to prepare SQL statement: {}", err))
        })?;

        let statement_handle = uuid::Uuid::new_v4().as_bytes().to_vec();
        self.statements
            .insert(statement_handle.clone(), query.query);

        let ticket = TicketStatementQuery {
            statement_handle: statement_handle.into(),
        };
        let ticket = Ticket {
            ticket: prost::Message::encode_to_vec(&ticket).into(),
        };

        let schema = df.schema().inner();

        let descriptor = request.into_inner();

        let options = IpcWriteOptions::default();
        let IpcMessage(schema_bytes) = SchemaAsIpc::new(schema.as_ref(), &options)
            .try_into()
            .map_err(|err| Status::internal(format!("Failed to encode schema: {}", err)))?;

        // Return FlightInfo.
        let flight_info = FlightInfo {
            flight_descriptor: Some(descriptor),
            schema: schema_bytes,

            endpoint: vec![FlightEndpoint {
                ticket: Some(ticket),
                ..Default::default()
            }],
            // We don't know these without executing the query.
            total_records: -1,
            total_bytes: -1,

            ..Default::default()
        };

        Ok(Response::new(flight_info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send>>>,
        Status,
    > {
        // Resolve the statement handle.
        let statement_handle = ticket.statement_handle.to_vec();

        let query = self
            .statements
            .get(&statement_handle)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| Status::not_found("Statement handle not found"))?;

        debug!(
            "Executing FlightSQL statement - handle: {:?}, sql: {}",
            statement_handle, query
        );
        let df =
            self.sql_ctx.sql(&query).await.map_err(|err| {
                Status::internal(format!("Failed to plan SQL statement: {}", err))
            })?;

        let schema = df.schema().inner().clone();

        // Execute
        let stream = df
            .execute_stream()
            .await
            .map_err(|err| Status::internal(format!("Failed to execute SQL statement: {}", err)))?;

        let stream = stream.map(|result| {
            result
                .map_err(|err| arrow_flight::error::FlightError::from_external_error(Box::new(err)))
        });

        // FlightData stream
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream);

        let flight_stream = flight_stream
            .map(|result| result.map_err(|err| Status::internal(format!("Flight error: {}", err))));

        Ok(Response::new(Box::pin(flight_stream)))
    }
}
