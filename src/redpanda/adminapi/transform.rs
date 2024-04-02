use crate::redpanda::adminapi::TransformMetadata;
use reqwest::Body;
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Transform {
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
