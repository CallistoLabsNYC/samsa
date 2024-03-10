use super::AdminAPI;
use crate::error::{Error, Result};

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
            client,
            urls: self.urls,
        })
    }

    pub fn urls(&mut self, urls: Vec<String>) -> Self {
        self.urls = urls;
        self.clone()
    }
}
