use crate::prelude::redpanda::adminapi::EnvironmentVariable;
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct TransformMetadata {
    pub name: String,
    pub input_topic: String,
    pub output_topics: Vec<String>,
    pub environment: Vec<EnvironmentVariable>,
}
