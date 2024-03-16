use super::AdminAPI;
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Default)]
pub struct Builder {
    urls: Vec<String>,
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(self) -> Result<AdminAPI> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|err| Error::ArgError(err.to_string()))?;
        Ok(AdminAPI {
            broker_id_to_urls: Arc::new(Mutex::new(HashMap::new())),
            client,
            urls: self.urls,
        })
    }

    pub fn urls(&mut self, urls: Vec<String>) -> Self {
        self.urls = urls;
        self.clone()
    }
}
