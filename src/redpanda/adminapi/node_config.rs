use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct NodeConfig {
    pub node_id: i32,
    // TODO add the rest of the fields
}
