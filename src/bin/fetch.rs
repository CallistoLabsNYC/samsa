use std::collections::HashMap;

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

    let bootstrap_addrs = vec!["127.0.0.1:9092".to_string()];

    let src_topic = "shakespeare".to_string();

    let conn = samsa::prelude::BrokerConnection::new(bootstrap_addrs)
        .await
        .map_err(|err| tracing::error!("{:?}", err))?;
    let correlation_id = 1;
    let client_id = "rust";
    let max_wait_ms = 100;
    let min_bytes = 100;
    let max_bytes = 30000;
    let isolation_level = 0;

    let response = samsa::prelude::fetch(
        conn.clone(),
        correlation_id,
        client_id,
        max_wait_ms,
        min_bytes,
        max_bytes,
        max_bytes,
        isolation_level,
        &HashMap::from([(src_topic, vec![0])]),
        &HashMap::new(),
    )
    .await
    .map_err(|err| tracing::error!("{:?}", err))?;

    tracing::info!("{:?}", response);

    Ok(())
}
