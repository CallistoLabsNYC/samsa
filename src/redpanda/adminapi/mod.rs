mod builder;
mod partition;

use crate::error::Error::KafkaError;
use crate::error::{KafkaCode, Result};
use crate::redpanda::adminapi::builder::Builder;
pub use partition::Partition;
use serde::Deserialize;

#[derive(Default)]
pub struct AdminAPI {
    client: reqwest::Client,
    urls: Vec<String>,
}

impl AdminAPI {
    pub fn builder() -> Builder {
        Builder::default()
    }

    async fn get_any<T>(self, path: &str) -> Result<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let req = self.client.get(format!("{}/{}", self.urls[0], path));
        let res = req.send().await?.json().await?;
        Ok(res)
    }

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
        self.get_any(&format!(
            "/v1/partitions/{}/{}/{}",
            namespace, topic, partition
        ))
        .await
    }
}
