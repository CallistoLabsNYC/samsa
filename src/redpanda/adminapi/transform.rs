use crate::prelude::redpanda::adminapi::{EnvironmentVariable, PartitionTransformStatus};
use reqwest::Body;
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct TransformMetadata {
    pub name: String,
    pub input_topic: String,
    pub output_topics: String,
    pub status: Vec<PartitionTransformStatus>,
    pub environment: Vec<EnvironmentVariable>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Transform {
    pub metadata: TransformMetadata,
    pub contents: Vec<u8>,
}

impl Into<Body> for Transform {
    fn into(self) -> Body {
        let mut bytes = serde_json::to_vec(&self.metadata).unwrap();
        let mut contents = self.contents.clone();
        bytes.append(&mut contents);
        Body::from(bytes)
    }
}
