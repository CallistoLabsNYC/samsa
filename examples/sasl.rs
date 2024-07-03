//! A simple client using SASLConfig::with_credentials
//!
//! This can authenticate a client side with mechanisms that only require an authid, authzid and
//! password. Currently this means 'SCRAM-SHA-2', 'SCRAM-SHA-1', 'PLAIN', and 'LOGIN', preferred
//! in that order.

use bytes::Bytes;
use rsasl::prelude::*;
use samsa::prelude::{
    do_sasl, protocol::{
        sasl_handshake::{request::SaslHandshakeRequest, response::SaslHandshakeResponse}, MetadataRequest, MetadataResponse, SaslAuthenticationRequest, SaslAuthenticationResponse
    }, BrokerAddress, TcpConnection, TlsConnection, TlsConnectionOptions
};
use std::io::Cursor;

#[tokio::main]
async fn main() -> Result<(), ()> {
    tracing_subscriber::fmt()
        // filter spans/events with level TRACE or higher.
        .with_max_level(tracing::Level::TRACE)
        .compact()
        // Display source code file paths
        .with_file(true)
        // Display source code line numbers
        .with_line_number(true)
        // Display the thread ID an event was recorded on
        .with_thread_ids(true)
        // Don't display the event's target (module path)
        .with_target(false)
        // Build the subscriber
        .init();

    let username = String::from("myuser");
    let password = String::from("pass1234");
    let topics = vec!["atopic"];

    let correlation_id = 1;
    let client_id = "rust";

    let options = vec![BrokerAddress {
        host: "piggy.callistolabs.cloud".to_owned(),
        port: 9092,
    }];

    let mut conn = TcpConnection::new_(options).await.unwrap();

    do_sasl(conn.clone(), correlation_id, client_id, username, password).await.unwrap();

    let metadata_request = MetadataRequest::new(correlation_id, client_id, &topics);
    conn.send_request_(&metadata_request).await.unwrap();
    let metadata_response = conn.receive_response_().await.unwrap();
    let metadata_response =
        MetadataResponse::try_from(metadata_response.freeze());

    Ok(())
}
