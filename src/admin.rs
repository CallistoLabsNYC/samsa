use crate::prelude::{protocol, Result, TcpBrokerConnection};
use std::collections::HashMap;

/// Create a topic in the cluster.
///
/// See this [protocol spec] for more information.
///
/// [protocol spec]: protocol::create_topics
pub async fn create_topics(
    conn: TcpBrokerConnection,
    correlation_id: i32,
    client_id: &str,
    topics_with_partition_count: HashMap<&str, i32>,
) -> Result<protocol::CreateTopicsResponse> {
    let mut create_topics =
        protocol::CreateTopicsRequest::new(correlation_id, client_id, 4000, false)?;

    for (topic_name, num_partitions) in topics_with_partition_count {
        create_topics.add(topic_name, num_partitions, 1);
    }

    conn.send_request_(&create_topics).await?;

    let create_topics_response = conn.receive_response_().await?;

    protocol::CreateTopicsResponse::try_from(create_topics_response.freeze())
}

/// Delete a topic in the cluster.
///
/// See this [protocol spec] for more information.
///
/// [protocol spec]: protocol::delete_topics
pub async fn delete_topics(
    conn: TcpBrokerConnection,
    correlation_id: i32,
    client_id: &str,
    topics: Vec<&str>,
) -> Result<protocol::DeleteTopicsResponse> {
    let mut delete_topics = protocol::DeleteTopicsRequest::new(correlation_id, client_id, 4000)?;

    for topic_name in topics {
        delete_topics.add(topic_name);
    }

    conn.send_request_(&delete_topics).await?;

    let delete_topics_response = conn.receive_response_().await?;

    protocol::DeleteTopicsResponse::try_from(delete_topics_response.freeze())
}
