use futures::StreamExt;
use samsa::prelude::protocol::produce::request::Attributes;
use samsa::prelude::{protocol, BrokerConnection, Compression, Error, KafkaCode, TcpConnection};
use std::collections::HashMap;
use tokio_stream::iter;

mod testsupport;
const CLIENT_ID: &str = "write and read 1M messages";
const CORRELATION_ID: i32 = 1;
const PARTITION_ID: i32 = 0;
const CHUNK_SIZE: usize = 50_000;

#[tokio::test]
async fn write_and_read_1m_messages() -> Result<(), Box<Error>> {
    let (skip, brokers, topic) = testsupport::get_brokers_and_topic()?;
    if skip {
        return Ok(());
    }
    let conn = TcpConnection::new(brokers.clone()).await?;
    testsupport::ensure_topic_creation(conn, &topic, CORRELATION_ID, CLIENT_ID).await?;

    let cluster_metadata = samsa::prelude::ClusterMetadata::<TcpConnection>::new(
        brokers.clone(),
        CLIENT_ID.to_string(),
        vec![topic.clone()],
    )
    .await?;
    let topic_partition = HashMap::from([(topic.to_string(), vec![PARTITION_ID])]);
    let (mut conn, _) =
        cluster_metadata.get_connections_for_topic_partitions(&topic_partition)?[0].to_owned();

    //
    // Test writing
    //
    let stream = iter(0..1000000);
    let mut stream = stream.ready_chunks(CHUNK_SIZE);

    while let Some(chunk) = stream.next().await {
        let mut produce_request = protocol::ProduceRequest::new(
            1,
            1000,
            CORRELATION_ID,
            CLIENT_ID,
            Attributes::new(Some(Compression::Gzip)),
        );

        for _ in chunk {
            let key = bytes::Bytes::from("testing testing...");
            let value = bytes::Bytes::from("123!");
            let header = protocol::Header::new(
                String::from("Header key"),
                bytes::Bytes::from("Header value"),
            );
            produce_request.add(&topic, PARTITION_ID, Some(key), Some(value), vec![header]);
        }

        conn.send_request(&produce_request).await?;
    }

    let bytes = conn.receive_response().await?.freeze();
    let produce_response = protocol::ProduceResponse::try_from(bytes)?;

    assert_eq!(produce_response.responses.len(), 1);
    assert_eq!(
        produce_response.responses[0].name,
        bytes::Bytes::from(topic.clone())
    );
    assert_eq!(
        produce_response.responses[0].partition_responses[0].error_code,
        KafkaCode::None
    );

    //
    // Test fetch (read)
    //
    let mut fetch_req = protocol::FetchRequest::new(CORRELATION_ID, CLIENT_ID, 1000, 10, 1000, 0);
    fetch_req.add(&topic, 0, 0, 1000);

    conn.send_request(&fetch_req).await?;
    let bytes = conn.receive_response().await?.freeze();
    let fetch_response = protocol::FetchResponse::try_from(bytes)?;

    dbg!(&fetch_response);

    Ok(())
}
