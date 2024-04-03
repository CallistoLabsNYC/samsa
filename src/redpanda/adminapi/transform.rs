use crate::redpanda::adminapi::TransformMetadataIn;
use reqwest::Body;
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Transform {
    pub metadata: TransformMetadataIn,
    pub contents: Vec<u8>,
}

impl From<Transform> for Body {
    fn from(val: Transform) -> Self {
        let mut bytes = serde_json::to_vec(&val.metadata).unwrap();
        let mut contents = val.contents.clone();
        bytes.append(&mut contents);
        Body::from(bytes)
    }
}
