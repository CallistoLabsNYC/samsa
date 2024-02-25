use std::collections::HashMap;

use nom::AsBytes;
use samsa::prelude::ConsumerBuilder;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), ()> {
    tracing_subscriber::fmt()
        // filter spans/events with level TRACE or higher.
        .with_max_level(tracing::Level::INFO)
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

    let counter = ConsumerBuilder::new(
        bootstrap_addrs,
        HashMap::from([(src_topic, vec![0, 1, 2, 3])]),
    )
    .await
    .map_err(|err| tracing::error!("{:?}", err))?
    .build()
    .into_flat_stream()
    .take(800000)
    .map(|record| {
        std::str::from_utf8(record.value.as_bytes())
            .unwrap()
            .replace(&['(', ')', ',', '\"', '.', ';', ':', '\''][..], "")
            .to_lowercase()
    })
    .fold(HashMap::new(), |mut counter, word| {
        if let Some(count) = counter.get(&word) {
            counter.insert(word, count + 1);
        } else {
            counter.insert(word, 1);
        }
        counter
    })
    .await;

    let mut hash_vec: Vec<(&String, &u32)> = counter.iter().collect();
    hash_vec.sort_by(|a, b| b.1.cmp(a.1));
    for (word, count) in hash_vec.iter().take(100) {
        tracing::info!("{word}: {count}");
    }

    Ok(())
}
