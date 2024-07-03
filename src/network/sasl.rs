use crate::prelude::{
    protocol::{
        SaslAuthenticationRequest, SaslAuthenticationResponse, SaslHandshakeRequest,
        SaslHandshakeResponse,
    },
    BrokerConnection, Result,
};
use bytes::Bytes;
use rsasl::prelude::*;
use std::io::Cursor;

pub async fn sasl_handshake(
    mut broker_conn: impl BrokerConnection,
    correlation_id: i32,
    client_id: &str,
    mechanism: String,
) -> Result<SaslHandshakeResponse> {
    let handshake_request = SaslHandshakeRequest::new(correlation_id, client_id, mechanism);
    broker_conn.send_request(&handshake_request).await?;
    let handshake_response = broker_conn.receive_response().await?;
    SaslHandshakeResponse::try_from(handshake_response.freeze())
}

pub async fn sasl_authentication(
    mut broker_conn: impl BrokerConnection,
    correlation_id: i32,
    client_id: &str,
    auth_bytes: Bytes,
) -> Result<SaslAuthenticationResponse> {
    let authentication_request =
        SaslAuthenticationRequest::new(correlation_id, client_id, auth_bytes);
    broker_conn.send_request(&authentication_request).await?;
    let authentication_response = broker_conn.receive_response().await?;
    SaslAuthenticationResponse::try_from(authentication_response.freeze())
}

pub async fn sasl(
    broker_conn: impl BrokerConnection + Copy,
    correlation_id: i32,
    client_id: &str,
    username: String,
    password: String,
) -> Result<()> {
    let mechanism = String::from("SCRAM-SHA-256");
    let handshake_response =
        sasl_handshake(broker_conn, correlation_id, client_id, mechanism).await?;

    let config = SASLConfig::with_credentials(None, username, password).unwrap();
    let sasl = rsasl::prelude::SASLClient::new(config);

    // There are often more than one Mechanisms offered by the server, `start_suggested` will
    // select the best ones from those available to both sides.
    let offered = [Mechname::parse(b"SCRAM-SHA-256").unwrap()];
    let mut session = sasl.start_suggested(&offered).unwrap();

    // Do the authentication steps.
    let mut out = Cursor::new(Vec::new());
    let input: Option<&[u8]> = None;
    let state = session.step(input, &mut out).unwrap();
    match state {
        State::Running => tracing::info!("SCRAM-SHA-256 exchange took more than one step"),
        State::Finished(MessageSent::Yes) => {
            tracing::info!("SCRAM-SHA-256 sent the message")
        }
        State::Finished(MessageSent::No) => {
            tracing::info!("SCRAM-SHA-256 exchange produced no output")
        }
    }

    let buffer = out.into_inner();
    println!("Encoded bytes: {buffer:?}");
    println!("As string: {:?}", std::str::from_utf8(buffer.as_ref()));

    let authentication_response =
        sasl_authentication(broker_conn, correlation_id, client_id, Bytes::from(buffer)).await?;

    let mut out = Cursor::new(Vec::new());
    let state = session
        .step(
            Some(&authentication_response.auth_bytes.to_vec()),
            &mut out,
        )
        .unwrap();
    match state {
        State::Running => tracing::info!("SCRAM-SHA-256 exchange took more than one step"),
        State::Finished(MessageSent::Yes) => {
            tracing::info!("SCRAM-SHA-256 sent the message")
        }
        State::Finished(MessageSent::No) => {
            tracing::info!("SCRAM-SHA-256 exchange produced no output")
        }
    }
    let buffer = out.into_inner();
    println!("Encoded bytes: {buffer:?}");
    println!("As string: {:?}", std::str::from_utf8(buffer.as_ref()));

    let authentication_response = sasl_authentication(broker_conn, correlation_id, client_id, Bytes::from(buffer)).await?;

    let mut out = Cursor::new(Vec::new());
    let state = session
        .step(
            Some(&authentication_response.auth_bytes.to_vec()),
            &mut out,
        )
        .unwrap();
    match state {
        State::Running => tracing::info!("SCRAM-SHA-256 exchange took more than one step"),
        State::Finished(MessageSent::Yes) => {
            tracing::info!("SCRAM-SHA-256 sent the message")
        }
        State::Finished(MessageSent::No) => {
            tracing::info!("SCRAM-SHA-256 exchange produced no output")
        }
    }
    let buffer = out.into_inner();
    println!("Encoded bytes: {buffer:?}");
    println!("As string: {:?}", std::str::from_utf8(buffer.as_ref()));

    Ok(())
}
