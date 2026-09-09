use std::{pin::Pin, sync::Arc};

use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, IpcMessage,
    PutResult, SchemaAsIpc, Ticket,
    decode::FlightRecordBatchStream,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::FlightServiceServer,
    sql::{
        Any, CommandStatementQuery, DoPutUpdateResult, SqlInfo, TicketStatementQuery,
        server::{FlightSqlService, PeekableFlightDataStream},
    },
};

use arrow_ipc::writer::IpcWriteOptions;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::{Stream, StreamExt};
use log::debug;
use paimon::{Catalog, FileSystemCatalog};
use paimon_datafusion::SQLContext;
use tonic::{Request, Response, Status, Streaming};

use crate::store::element_store::{FlareElementStore, create_catalog};

pub struct FlareIO {
    catalog: Arc<FileSystemCatalog>,
    sql_info: DashMap<i32, SqlInfo>,
    sql_ctx: SQLContext,
    statements: DashMap<Vec<u8>, String>,
    store: FlareElementStore,
}

impl FlareIO {
    pub async fn new() -> Result<Self, anyhow::Error> {
        let store_path = crate::utils::path::flare_warehouse_dir();
        let store_base = store_path.to_str().unwrap_or(".").to_string();

        let catalog = create_catalog(store_base.clone(), "default".to_string()).await?;

        let catalog = Arc::new(catalog);

        let store = FlareElementStore::new(
            store_base,
            "default".to_string(),
            Some(Arc::clone(&catalog)),
        )
        .await?;

        let mut sql_ctx = SQLContext::new();

        sql_ctx
            .register_catalog("flare", Arc::clone(&catalog) as Arc<dyn Catalog>)
            .await?;

        Ok(Self {
            catalog,
            sql_ctx,
            sql_info: DashMap::new(),
            statements: DashMap::new(),
            store,
        })
    }

    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    /// Decode an Arrow Flight `DoPut` stream and write each record batch to the
    /// Paimon table backing `pcollection_id`.
    async fn write_flight_batches(
        &self,
        pcollection_id: &str,
        stream: PeekableFlightDataStream,
    ) -> Result<i64, Status> {
        let stream = stream
            .into_inner()
            .map(|item| item.map_err(|status| FlightError::from_external_error(Box::new(status))));
        let mut batches = FlightRecordBatchStream::new_from_flight_data(stream);

        let mut record_count = 0_i64;
        while let Some(batch) = batches.next().await {
            let batch = batch
                .map_err(|err| Status::internal(format!("Failed to decode Flight data: {err}")))?;
            record_count += batch.num_rows() as i64;

            self.store
                .write_row_batch(pcollection_id, batch)
                .await
                .map_err(|err| {
                    Status::internal(format!(
                        "Failed to write batch to Paimon table '{pcollection_id}': {err}"
                    ))
                })?;
        }

        debug!("Wrote {record_count} rows to pcollection '{pcollection_id}'");
        Ok(record_count)
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

    /*async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let pcollection_id = parse_table_name(&ticket.query)?;
        self.write_flight_batches(&pcollection_id, request.into_inner())
            .await
    }*/

    async fn do_put_fallback(
        &self,
        request: Request<PeekableFlightDataStream>,
        _message: Any,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>>,
        Status,
    > {
        let mut stream = request.into_inner();

        let reference = match stream.peek().await {
            Some(Ok(data)) => data
                .flight_descriptor
                .as_ref()
                .map(|descriptor| descriptor.path.join("."))
                .unwrap_or_default(),
            Some(Err(status)) => return Err(status.clone()),
            None => {
                return Err(Status::invalid_argument(
                    "DoPut stream is missing its FlightData descriptor",
                ));
            }
        };

        let pcollection_id = parse_table_name(&reference)?;
        let record_count = self.write_flight_batches(&pcollection_id, stream).await?;

        let result = DoPutUpdateResult { record_count };
        let output = futures::stream::iter(vec![Ok(PutResult {
            app_metadata: prost::Message::encode_to_vec(&result).into(),
        })]);
        Ok(Response::new(Box::pin(output)))
    }
}

fn parse_table_name(reference: &str) -> Result<String, Status> {
    let table = reference
        .trim()
        .rsplit('.')
        .next()
        .map(str::trim)
        .unwrap_or_default();

    if table.is_empty() {
        return Err(Status::invalid_argument(
            "Cannot derive a destination table name from the update statement",
        ));
    }

    Ok(table.to_string())
}
