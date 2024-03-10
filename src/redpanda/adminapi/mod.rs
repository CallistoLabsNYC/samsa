#![allow(dead_code)] // TODO remove

mod builder;
mod environment_variable;
mod node_config;
mod partition;
mod partition_transform_status;
mod transform_metadata;

use crate::error::Error::KafkaError;
use crate::error::{Error, KafkaCode, Result};
use crate::redpanda::adminapi::builder::Builder;
pub use environment_variable::EnvironmentVariable;
pub use node_config::NodeConfig;
pub use partition::Partition;
pub use partition_transform_status::PartitionTransformStatus;
use reqwest::Response;
use reqwest::{Body, Method};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
pub use transform_metadata::TransformMetadata;

#[derive(Clone, Default)]
pub struct AdminAPI {
    broker_id_to_urls: Arc<Mutex<HashMap<i32, String>>>,
    client: reqwest::Client,
    urls: Vec<String>,
}

impl AdminAPI {
    pub fn builder() -> Builder {
        Builder::default()
    }

    async fn broker_id_to_url(self, broker_id: i32) -> Result<String> {
        if let Ok(url) = self.clone().get_url_from_broker_id(broker_id.clone()) {
            return Ok(url);
        }
        self.clone().map_broker_ids_to_urls().await?;
        self.get_url_from_broker_id(broker_id)
    }

    async fn delete_any(self, path: &str) -> Result<Response> {
        let req = self.client.delete(format!("{}/{}", self.urls[0], path));
        let res = req.send().await?;
        Ok(res)
    }

    pub async fn each_broker<F>(self, f: impl Fn(AdminAPI) -> Result<()>) -> Result<()> {
        for url in self.urls {
            let url = url.clone();
            let aa = new_admin_for_single_host(url)?;
            f(aa)?;
        }
        Ok(())
    }

    pub async fn delete_wasm_transform(self, _name: &str) -> Result<()> {
        Ok(()) // TODO
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

    pub async fn get_node_config(self) -> Result<NodeConfig> {
        let node_config = self
            .send_one(Method::GET, "/v1/node_config", false)
            .await?
            .unwrap();
        Ok(node_config)
    }

    pub async fn get_partition(
        self,
        namespace: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Partition> {
        self.send_any(
            Method::GET,
            &format!("/v1/partitions/{}/{}/{}", namespace, topic, partition),
        )
        .await
    }

    fn get_url_from_broker_id(self, broker_id: i32) -> Result<String> {
        let locked = self
            .broker_id_to_urls
            .lock()
            .map_err(|err| Error::LockError(err.to_string()))?;
        if let Some(url) = locked.get(&broker_id) {
            return Ok(url.clone());
        }
        Err(KafkaError(KafkaCode::BrokerNotAvailable))
    }

    pub async fn list_wasm_transforms(self) -> Result<Vec<TransformMetadata>> {
        self.get_any("/v1/transform/").await
    }

    async fn map_broker_ids_to_urls(self) -> Result<()> {
        // TODO
        // self.each_broker(|aa| async {
        //     let nc = self.get_node_config().await.unwrap();
        //     let mut locked = self
        //         .broker_id_to_urls
        //         .lock()
        //         .map_err(|err| Error::LockError(err.to_string()))?;
        //     locked.insert(nc.node_id, aa.urls[0].clone());
        //     // Ok(())
        // });
        Ok(())
    }

    async fn send_any<T>(self, method: Method, path: &str) -> Result<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let req = self
            .client
            .request(method, format!("{}/{}", self.urls[0], path));
        let res = req.send().await?.json().await?;
        Ok(res)
    }

    async fn send_one<T>(self, method: Method, path: &str, _retryable: bool) -> Result<Option<T>>
    where
        T: for<'a> Deserialize<'a>,
    {
        if self.urls.len() != 1 {
            return Err(Error::ArgError(format!(
                "unable to issue a single-admin-endpoint request to {} admin endpoints",
                self.urls.len()
            )))?;
        }
        let url = format!("{}/{}", self.urls[0], path);
        let req = self.client.request(method, url);
        let res = req.send().await?.json().await?;
        Ok(res)
    }

    async fn send_one_with_body<B: Into<Body>, T>(
        self,
        method: Method,
        path: &str,
        body: B,
        _retryable: bool,
    ) -> Result<Option<T>>
    where
        T: for<'a> Deserialize<'a>,
    {
        if self.urls.len() != 1 {
            return Err(Error::ArgError(format!(
                "unable to issue a single-admin-endpoint request to {} admin endpoints",
                self.urls.len()
            )))?;
        }
        let url = format!("{}/{}", self.urls[0], path);
        let req = self.client.request(method, url).body(body);
        let res = req.send().await?.json().await?;
        Ok(res)
    }

    async fn send_to_leader<T>(self, method: Method, path: &str) -> Result<Option<T>>
    where
        T: for<'a> Deserialize<'a>,
    {
        // If there's only one broker, let's just send the request to it
        if self.urls.len() == 1 {
            return self.send_one(method, path, true).await;
        }

        let mut retries = 3;
        let leader_id: Option<i32> = None;
        let mut leader_url = String::new();
        while leader_id.is_none() || leader_url.is_empty() {
            match self.clone().get_leader_id().await {
                Err(KafkaError(KafkaCode::LeaderNotAvailable)) => {
                    retries = retries - 1;
                    if retries == 0 {
                        return Err(KafkaError(KafkaCode::LeaderNotAvailable));
                    }
                }
                Err(e) => return Err(e),
                Ok(leader_id) => {
                    // Got a leader id, check if it's resolvable
                    let res = self.clone().broker_id_to_url(leader_id).await;
                    if res.is_err()
                        && self
                            .broker_id_to_urls
                            .lock()
                            .map_err(|err| Error::LockError(err.to_string()))?
                            .is_empty()
                    {
                        // TODO return send_all
                    } else if res.is_err() {
                        break;
                    }
                    leader_url = res.unwrap();
                    retries = retries - 1;
                    if retries == 0 {
                        return Err(KafkaError(KafkaCode::LeaderNotAvailable));
                    }
                    // TODO sleep for stale leader backoff
                }
            };
        }

        let aa = new_admin_for_single_host(leader_url)?;
        aa.send_one(method, path, true).await
    }

    async fn send_to_leader_with_body<B: Into<Body>, T>(
        self,
        method: Method,
        path: &str,
        body: B,
    ) -> Result<Option<T>>
    where
        T: for<'a> Deserialize<'a>,
    {
        // If there's only one broker, let's just send the request to it
        if self.urls.len() == 1 {
            return self.send_one_with_body(method, path, body, true).await;
        }

        let mut retries = 3;
        let leader_id: Option<i32> = None;
        let mut leader_url = String::new();
        while leader_id.is_none() {
            match self.clone().get_leader_id().await {
                Err(KafkaError(KafkaCode::LeaderNotAvailable)) => {
                    retries = retries - 1;
                    if retries == 0 {
                        return Err(KafkaError(KafkaCode::LeaderNotAvailable));
                    }
                }
                Err(e) => return Err(e),
                Ok(leader_id) => {
                    // Got a leader id, check if it's resolvable
                    let res = self.clone().broker_id_to_url(leader_id).await;
                    if res.is_err()
                        && self
                            .broker_id_to_urls
                            .lock()
                            .map_err(|err| Error::LockError(err.to_string()))?
                            .is_empty()
                    {
                        // TODO return send_all
                    } else if res.is_err() {
                        break;
                    }
                    leader_url = res.unwrap();
                    retries = retries - 1;
                    if retries == 0 {
                        return Err(KafkaError(KafkaCode::LeaderNotAvailable));
                    }
                    // TODO sleep for stale leader backoff
                }
            };
        }

        let aa = new_admin_for_single_host(leader_url)?;
        aa.send_one_with_body(method, path, body, true).await
    }
}

fn new_admin_for_single_host(host: String) -> Result<AdminAPI> {
    Builder::new().urls(vec![host]).build()
}
