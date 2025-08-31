use samsa::prelude::{ClusterMetadata, TcpConnection};
use std::fmt::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .init();

    let addrs = vec![samsa::prelude::BrokerAddress {
        host: "localhost".to_owned(),
        port: 9092,
    }];

    let metadata =
        match ClusterMetadata::<TcpConnection>::new(addrs, 0, "cliend_id".to_owned(), vec![]).await
        {
            Ok(metadata) => metadata,
            Err(err) => panic!("Failed to retrieve cluster metadata: {}", err),
        };

    println!("{:#?}", metadata.topics);

    Ok(())
}
