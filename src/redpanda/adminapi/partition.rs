use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Partition {
    pub leader_id: i32,
    pub namespace: String,
    pub partition_id: i32,
    pub raft_group_id: i32,
    // TODO replicas
    pub status: String,
    pub topic: String,
}
