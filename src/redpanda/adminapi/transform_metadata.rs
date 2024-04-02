use crate::prelude::redpanda::adminapi::EnvironmentVariable;
use crate::redpanda::adminapi::PartitionTransformStatus;
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct TransformMetadataIn {
    pub name: String,
    pub input_topic: String,
    pub output_topics: Vec<String>,
    pub environment: Vec<EnvironmentVariable>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct TransformMetadataOut {
    pub name: String,
    pub input_topic: String,
    pub output_topics: Vec<String>,
    pub status: Vec<PartitionTransformStatus>,
}
