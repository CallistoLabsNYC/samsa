use crate::prelude::redpanda::adminapi::{EnvironmentVariable, PartitionTransformStatus};
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct TransformMetadata {
    pub name: String,
    pub input_topic: String,
    pub output_topics: String,
    pub status: Vec<PartitionTransformStatus>,
    pub environment: Vec<EnvironmentVariable>,
}
