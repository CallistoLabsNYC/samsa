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
use std::{collections::HashSet, future::Future, io::Cursor};

/// SASL Credentials
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

pub async fn start_sasl_session(
    broker_conn: impl BrokerConnection + Clone,
    correlation_id: i32,
    client_id: &str,
    config: SaslConfig,
    mechanism: String,
) -> Result<(Option<Session>, Vec<String>)> {
    let config = SASLConfig::with_credentials(None, config.username, config.password).unwrap();
    let mut maybe_session = None;
    let mut maybe_next = vec![];

    let handshake_response = sasl_handshake(
        broker_conn.clone(),
        correlation_id,
        client_id,
        mechanism.clone(),
    )
    .await?;

    let suggested = parse_mechanisms(&handshake_response)?;

    match handshake_response.error_code {
        KafkaCode::None => {
            tracing::trace!("good handshake, starting a session");

            let sasl = rsasl::prelude::SASLClient::new(config.clone());
            maybe_session = Some(sasl.start_suggested(&suggested).map_err(|e| {
                tracing::error!("{:?}", e);
                Error::InvalidSaslMechanism
            })?);
        }
        KafkaCode::UnsupportedSaslMechanism => {
            tracing::debug!(
                "bad handshake: {:?} not supported; will try others",
                mechanism
            );
            tracing::debug!("supported mechanisms {:?}", suggested);
            assert!(!suggested.is_empty());

            maybe_next.extend(suggested.iter().map(|x| String::from(x.as_str())));
        }
        _ => {
            tracing::error!("handshake failed with: {:?}", handshake_response);
            return Err(Error::KafkaError(handshake_response.error_code));
        }
    }

    Ok((maybe_session, maybe_next))
}

fn parse_mechanisms(handshake_response: &SaslHandshakeResponse) -> Result<Vec<&Mechname>> {
    handshake_response
        .mechanisms
        .iter()
        .map(|mech| {
            tracing::debug!("{:?}", mech);
            Mechname::parse(mech.as_ref()).map_err(|e| {
                tracing::error!("{:?}", e);
                Error::InvalidSaslMechanism
            })
        })
        .collect::<Result<Vec<&Mechname>>>()
}

//
// We need the factory because kafka drops our connection
// if the given mechanism is unsupported.
// In this case we retry with another mechanism, suggested by kafka.
// rsasl supports this kind of workflow.
//
pub async fn do_sasl_v2<T, FT, FFT>(
    conn_factory: FFT,
    correlation_id: i32,
    client_id: &str,
    config: SaslConfig,
) -> Result<T>
where
    T: BrokerConnection + Clone,
    FT: Future<Output = Result<T>>,
    FFT: Fn() -> FT,
{
    let mut tried = HashSet::new();

    let mut maybe_session;
    // Start with the given mechanism.
    // Server will respond with a list of supported ones.
    let mut next_mech = Some(String::from("SCRAM-SHA-256"));

    let mut broker_conn;

    loop {
        tried.insert(next_mech.clone().unwrap());
        broker_conn = conn_factory().await?;

        let suggested;
        (maybe_session, suggested) = start_sasl_session(
            broker_conn.clone(),
            correlation_id,
            client_id,
            config.clone(),
            next_mech.clone().unwrap(),
        )
        .await?;

        next_mech = HashSet::from_iter(suggested)
            .difference(&tried)
            .map(|x| x.to_owned())
            .next();

        if maybe_session.is_some() || next_mech.is_none() {
            break;
        }
        tracing::debug!("will retry with mechanism {:?}", next_mech);
    }

    if maybe_session.is_none() {
        tracing::error!("failed to start a sasl session");
        return Err(Error::InvalidSaslMechanism);
    }

    let session = maybe_session.unwrap();
    tracing::info!("Using {:?} for our SASL Mechanism", session.get_mechname());

    match do_sasl_chit_chat(session, &broker_conn, correlation_id, client_id).await {
        Ok(_) => Ok(broker_conn),
        Err(e) => {
            tracing::error!("chit_chat failed: {:?}", e);
            Err(e)
        }
    }
}

async fn do_sasl_chit_chat<T>(
    mut session: Session,
    broker_conn: &T,
    correlation_id: i32,
    client_id: &str,
) -> Result<()>
where
    T: BrokerConnection + Clone,
{
    let mut data_in: Option<Vec<u8>> = None;

    tracing::trace!("start sasl chit-chat");

    // stepping the authentication exchange to completion
    loop {
        let mut out = Cursor::new(Vec::new());

        // each call to step writes the generated auth data into the provided writer.
        let state = session
            .step(data_in.as_deref(), &mut out)
            .expect("step errored!");

        let data_out = out.into_inner();

        tracing::trace!("outgoing: {:?}", data_out);

        let response = sasl_authentication(
            broker_conn.clone(),
            correlation_id,
            client_id,
            Bytes::from(data_out),
        )
        .await?;

        tracing::trace!("incoming: {:?}", response);

        match response.error_code {
            KafkaCode::None => {
                let auth_bytes = response.auth_bytes.to_vec();

                data_in = if !auth_bytes.is_empty() {
                    Some(auth_bytes)
                } else {
                    None
                };
            }
            KafkaCode::SaslAuthenticationFailed => {
                let msg = response
                    .error_message
                    .map(|x| String::from_utf8_lossy(&x).into_owned())
                    .unwrap_or("".to_owned());
                tracing::info!("auth failed: {:?}: {:?}", response.error_code, msg);
                return Err(Error::SaslAuthFailed(msg));
            }
            _ => return Err(Error::KafkaError(response.error_code)),
        }

        if data_in.is_none() && state.is_finished() {
            break;
        }
    }

    Ok(())
}
