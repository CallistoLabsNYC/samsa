use crate::{
    prelude::{
        protocol::{
            SaslAuthenticationRequest, SaslAuthenticationResponse, SaslHandshakeRequest,
            SaslHandshakeResponse,
        },
        BrokerConnection, Error, KafkaCode, Result,
    },
    DEFAULT_CLIENT_ID, DEFAULT_CORRELATION_ID,
};
use bytes::Bytes;
use rsasl::prelude::*;
use std::io::Cursor;

#[derive(Clone, Debug)]
pub struct SaslConfig {
    pub username: String,
    pub password: String,
    pub correlation_id: i32,
    pub client_id: String,
}

impl SaslConfig {
    pub fn new(
        username: String,
        password: String,
        correlation_id: Option<i32>,
        client_id: Option<String>,
    ) -> Self {
        SaslConfig {
            username,
            password,
            correlation_id: correlation_id.unwrap_or(DEFAULT_CORRELATION_ID),
            client_id: client_id.unwrap_or(DEFAULT_CLIENT_ID.to_owned()),
        }
    }
}

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

pub async fn do_sasl(
    broker_conn: impl BrokerConnection + Clone,
    correlation_id: i32,
    client_id: &str,
    config: SaslConfig,
) -> Result<()> {
    let mechanism = String::from("SCRAM-SHA-256");
    let handshake_response =
        sasl_handshake(broker_conn.clone(), correlation_id, client_id, mechanism).await?;
    if handshake_response.error_code != KafkaCode::None {
        return Err(Error::KafkaError(handshake_response.error_code));
    }

    let config = SASLConfig::with_credentials(None, config.username, config.password).unwrap();
    let sasl = rsasl::prelude::SASLClient::new(config);
    let mechanisms = handshake_response
        .mechanisms
        .iter()
        .map(|mech| {
            tracing::debug!("{:?}", mech);
            Mechname::parse(mech).map_err(|e| {
                tracing::error!("{:?}", e);
                Error::InvalidSaslMechanism
            })
        })
        .collect::<Result<Vec<&Mechname>>>()?;
    tracing::debug!("mechanisms {:?}", mechanisms);

    let mut session = sasl.start_suggested(&mechanisms).unwrap();
    let selected_mechanism = session.get_mechname();
    tracing::debug!("Using {:?} for our SASL Mechanism", selected_mechanism);

    let mut data: Option<Vec<u8>> = None;

    // stepping the authentication exchange to completion
    while {
        let mut out = Cursor::new(Vec::new());
        // each call to step writes the generated auth data into the provided writer.
        // Normally this data would then have to be sent to the other party, but this goes
        // beyond the scope of this example
        let state = session
            .step(data.as_deref(), &mut out)
            .expect("step errored!");

        data = Some(out.into_inner());

        // returns `true` if step needs to be called again with another batch of data
        state.is_running()
    } {
        let authentication_response = sasl_authentication(
            broker_conn.clone(),
            correlation_id,
            client_id,
            Bytes::from(data.unwrap()),
        )
        .await?;
        data = Some(authentication_response.auth_bytes.to_vec());
    }

    Ok(())
}
