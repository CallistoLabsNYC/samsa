use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct EnvironmentVariable {
    pub key: String,
    pub value: String,
}
