use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use crate::error::BallistaError;
use crate::plan::Action;
use crate::protobuf;

use crate::arrow::datatypes::Schema;
use crate::arrow::flight::flight_data_to_batch;

use crate::arrow::record_batch::RecordBatch;
use flight::flight_service_client::FlightServiceClient;
use flight::Ticket;
use prost::Message;

pub async fn execute_action(
    host: &str,
    port: usize,
    action: Action,
) -> Result<Vec<RecordBatch>, BallistaError> {
    //TODO need to avoid connecting per request
    let mut client = FlightServiceClient::connect(format!("http://{}:{}", host, port))
        .await
        .map_err(|e| BallistaError::General(format!("{:?}", e)))?;

    let serialized_action: protobuf::Action = action.try_into()?;
    let mut buf: Vec<u8> = Vec::with_capacity(serialized_action.encoded_len());
    serialized_action
        .encode(&mut buf)
        .map_err(|e| BallistaError::General(format!("{:?}", e)))?;

    let request = tonic::Request::new(Ticket { ticket: buf });

    let mut stream = client
        .do_get(request)
        .await
        .map_err(|e| BallistaError::General(format!("{:?}", e)))?
        .into_inner();

    // the schema should be the first message returned, else client should error
    let flight_data = stream
        .message()
        .await
        .map_err(|e| BallistaError::General(format!("{:?}", e)))?
        .unwrap();

    // convert FlightData to a stream
    let schema = Arc::new(Schema::try_from(&flight_data)?);

    // all the remaining stream messages should be dictionary and record batches
    let mut batches = vec![];
    while let Some(flight_data) = stream
        .message()
        .await
        .map_err(|e| BallistaError::General(format!("{:?}", e)))?
    {
        // the unwrap is infallible and thus safe
        let record_batch = flight_data_to_batch(&flight_data, schema.clone())?.unwrap();
        batches.push(record_batch);
    }

    Ok(batches)
}
