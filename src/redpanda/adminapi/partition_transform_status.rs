use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct PartitionTransformStatus {
    pub node_id: i32,
    pub partition: i32,
    pub status: String,
    pub lag: i32,
}
