//! A simple client using SASLConfig::with_credentials
//!
//! This can authenticate a client side with mechanisms that only require an authid, authzid and
//! password. Currently this means 'SCRAM-SHA-2', 'SCRAM-SHA-1', 'PLAIN', and 'LOGIN', preferred
//! in that order.

use rsasl::prelude::*;
use samsa::prelude::{protocol::{sasl_handshake::{request::SaslHandshakeRequest, response::SaslHandshakeResponse}, SaslAuthenticationRequest, SaslAuthenticationResponse}, BrokerAddress, TcpConnection, TlsConnection, TlsConnectionOptions};
use std::io::Cursor;
use bytes::Bytes;

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


    let username = String::from("admin");
    let password = String::from("pass1234");
    let correlation_id = 1;
    let client_id = "rust";

    let options = vec![BrokerAddress {
            host: "piggy.callistolabs.cloud".to_owned(),
            port: 9092,
        }];
        

    // Construct a a config from only the credentials.
    // This takes the authzid, authid/username and password that are to be used.
    let config = SASLConfig::with_credentials(None, username, password).unwrap();

    // The config can now be sent to a protocol handling crate and used there. Below we simulate
    // a PLAIN authentication happening with the config:

    let sasl = rsasl::prelude::SASLClient::new(config);

    // There are often more than one Mechanisms offered by the server, `start_suggested` will
    // select the best ones from those available to both sides.
    let offered = [Mechname::parse(b"PLAIN").unwrap()];
    let mut session = sasl.start_suggested(&offered).unwrap();

    // Do the authentication steps.
    let mut out = Cursor::new(Vec::new());
    // PLAIN is client-first, and thus takes no input data on the first step.
    let input: Option<&[u8]> = None;
    // Actually generate the authentication data to send to a server
    let state = session.step(input, &mut out).unwrap();

    let mut conn = TcpConnection::new_(options).await.unwrap();

    match state {
        State::Running => panic!("PLAIN exchange took more than one step"),
        State::Finished(MessageSent::Yes) => {
            let buffer = out.into_inner();
            println!("Encoded bytes: {buffer:?}");
            println!("As string: {:?}", std::str::from_utf8(buffer.as_ref()));


            let handshake_request = SaslHandshakeRequest::new(correlation_id, client_id, "PLAIN".to_owned());
            conn.send_request_(&handshake_request).await.unwrap();
            let handshake_response = conn.receive_response_().await.unwrap();
            let handshake_response = SaslHandshakeResponse::try_from(handshake_response.freeze());

            let authentication_request = SaslAuthenticationRequest::new(correlation_id, client_id, Bytes::from(buffer));
            conn.send_request_(&authentication_request).await.unwrap();
            let authentication_response = conn.receive_response_().await.unwrap();
            let authentication_response = SaslAuthenticationResponse::try_from(authentication_response.freeze());

        }
        State::Finished(MessageSent::No) => {
            panic!("PLAIN exchange produced no output")
        }
    }

    Ok(())
}
