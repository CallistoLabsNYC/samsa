//! Encoding and creation for DescribeConfigs requests.
//!
//! This implements a minimal version to query configs for brokers or topics.
//!
//! References: Kafka protocol DescribeConfigs (API key 32) v1-compatible subset.

use crate::encode::ToByte;
use crate::error::Result;
use crate::protocol::HeaderRequest;

const API_KEY_DESCRIBE_CONFIGS: i16 = 32; // Kafka DescribeConfigs
const API_VERSION: i16 = 1; // minimal widely supported fields

type BrokerID<'a> = &'a str;
type TopicName<'a> = &'a str;

#[derive(Debug, Clone)]
pub enum ConfigResource<'a> {
    Broker {
        resource_name: BrokerID<'a>,
        config_names: Vec<&'a str>,
    },
    Topic {
        resource_name: TopicName<'a>,
        config_names: Vec<&'a str>,
    },
}

impl<'a> ConfigResource<'a> {
    pub fn resource_name(&self) -> &'a str {
        match self {
            ConfigResource::Broker { resource_name, .. } => resource_name,
            ConfigResource::Topic { resource_name, .. } => resource_name,
        }
    }

    // Values per Kafka protocol
    pub fn resource_type(&self) -> i8 {
        match self {
            ConfigResource::Broker { .. } => 4,
            ConfigResource::Topic { .. } => 2,
        }
    }

    pub fn config_names(&self) -> Vec<&'a str> {
        match self {
            ConfigResource::Broker { config_names, .. } => config_names.clone(),
            ConfigResource::Topic { config_names, .. } => config_names.clone(),
        }
    }
}

#[derive(Debug)]
pub struct DescribeConfigsRequest<'a> {
    pub header: HeaderRequest<'a>,
    pub resources: Vec<Resource<'a>>, // array of resources
    pub include_synonyms: bool,
}

#[derive(Debug)]
pub struct Resource<'a> {
    pub resource_type: i8,
    pub resource_name: String,
    pub config_names: Vec<&'a str>, // empty means all
}

impl<'a> DescribeConfigsRequest<'a> {
    pub fn new(correlation_id: i32, client_id: &'a str) -> Self {
        let header = HeaderRequest::new(
            API_KEY_DESCRIBE_CONFIGS,
            API_VERSION,
            correlation_id,
            client_id,
        );
        Self {
            header,
            resources: vec![],
            include_synonyms: false,
        }
    }

    pub fn add_resource(&mut self, resource: ConfigResource<'a>) {
        self.resources.push(Resource {
            resource_type: resource.resource_type(),
            resource_name: resource.resource_name().to_string(),
            config_names: resource.config_names(),
        });
    }
}

impl ToByte for DescribeConfigsRequest<'_> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> Result<()> {
        self.header.encode(buffer)?;
        self.resources.encode(buffer)?;
        self.include_synonyms.encode(buffer)?;
        Ok(())
    }
}

impl ToByte for Resource<'_> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> Result<()> {
        self.resource_type.encode(buffer)?;
        self.resource_name.encode(buffer)?;
        crate::encode::AsStrings(&self.config_names).encode(buffer)?;
        Ok(())
    }
}
