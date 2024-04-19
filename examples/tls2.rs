use samsa::prelude::{protocol, ConnectionOptions, TlsBrokerOptions, TlsConnection};

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
    let options = ConnectionOptions {
        broker_options: vec![TlsBrokerOptions {
            key: "./etc/redpanda/certs/client.key".into(),
            cert: "./etc/redpanda/certs/client.crt".into(),
            host: "piggy.callistolabs.cloud".to_owned(),
            port: 9092,
        }],
        cafile: Some("./etc/redpanda/certs/root.crt".into()),
    };

    let conn = TlsConnection::new(options).await.unwrap();

    let metta = metadata(conn).await;

    println!("{:?}", metta);

    Ok(())
}

pub async fn metadata(mut conn: TlsConnection) -> protocol::MetadataResponse {
    let topics = vec![
        "microcks-services-updates".to_string(),
        "test-1-user-signedup".to_string(),
    ];
    let find_coordinator_request = protocol::MetadataRequest::new(1, "client_id", &topics);
    conn.send_request(&find_coordinator_request).await.unwrap();

    let find_coordinator_response = conn.receive_response().await.unwrap();

    protocol::MetadataResponse::try_from(find_coordinator_response.freeze()).unwrap()
}
