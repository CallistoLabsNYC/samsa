use crate::prelude::BrokerConnection;
use crate::prelude::Result;
use crate::prelude::protocol;
use crate::protocol::create_topics::NewTopic;

/// Create a topic in the cluster.
///
/// See this [protocol spec] for more information.
///
/// [protocol spec]: protocol::create_topics
pub async fn create_topics(
    mut conn: impl BrokerConnection,
    correlation_id: i32,
    client_id: &str,
    topics: Vec<NewTopic>,
) -> Result<protocol::CreateTopicsResponse> {
    let mut create_topics =
        protocol::CreateTopicsRequest::new(correlation_id, client_id, 4000, false)?;

    for t in &topics {
        create_topics.add(&t.topic_name, t.num_partitions, t.replication_factor);
    }

    conn.send_request(&create_topics).await?;

    let create_topics_response = conn.receive_response().await?;

    protocol::CreateTopicsResponse::try_from(create_topics_response.freeze())
}

/// Delete a topic in the cluster.
///
/// See this [protocol spec] for more information.
///
/// [protocol spec]: protocol::delete_topics
pub async fn delete_topics(
    mut conn: impl BrokerConnection,
    correlation_id: i32,
    client_id: &str,
    topics: Vec<&str>,
) -> Result<protocol::DeleteTopicsResponse> {
    let mut delete_topics = protocol::DeleteTopicsRequest::new(correlation_id, client_id, 4000)?;

    for topic_name in topics {
        delete_topics.add(topic_name);
    }

    conn.send_request(&delete_topics).await?;

    let delete_topics_response = conn.receive_response().await?;

    protocol::DeleteTopicsResponse::try_from(delete_topics_response.freeze())
}
