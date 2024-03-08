mod builder;
mod partition;

use crate::error::Error::KafkaError;
use crate::error::{KafkaCode, Result};
pub use partition::Partition;

#[derive(Default)]
pub struct AdminAPI {
    client: reqwest::Client,
}

impl AdminAPI {
    pub async fn get_leader_id(self) -> Result<i32> {
        let pa = self.get_partition("redpanda", "controller", 0).await?;
        if pa.leader_id == -1 {
            return Err(KafkaError(KafkaCode::LeaderNotAvailable));
        };
        Ok(pa.leader_id)
    }

    pub async fn get_partition(
        self,
        namespace: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Partition> {
        let req = self.client.get(format!(
            "/v1/partitions/{}/{}/{}",
            namespace, topic, partition
        ));
        let partition: Partition = req.send().await?.json().await?;
        Ok(partition)
    }
}
