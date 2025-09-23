//! Parsing and processing for DescribeConfigs responses.
//!
//! Minimal v1-compatible subset.

use crate::error::Error;
use crate::error::KafkaCode;
use crate::error::Result;
use crate::parser::parse_array;
use crate::parser::{self};
use crate::protocol::HeaderResponse;
use crate::protocol::parse_header_response;
use bytes::Bytes;
use nom::IResult;
use nom::number::complete::be_i8;
use nombytes::NomBytes;
#[cfg(feature = "serde")]
use serde::de::DeserializeOwned;
#[cfg(feature = "serde")]
use serde_json;

#[derive(Debug, PartialEq)]
pub struct DescribeConfigsResponse {
    pub header: HeaderResponse,
    pub throttle_time_ms: i32,
    pub resources: Vec<Resource>,
}

impl DescribeConfigsResponse {
    #[cfg(feature = "serde")]
    /// Try to fetch and parse a config value for a given resource name and key.
    /// Returns None if the resource/key/value is not present or parsing fails.
    pub fn get_value<T: DeserializeOwned>(&self, resource_name: &str, key: &str) -> Option<T> {
        let resource = self
            .resources
            .iter()
            .find(|r| r.resource_name.as_ref() == resource_name.as_bytes())?;
        let cfg = resource
            .configs
            .iter()
            .find(|c| c.name.as_ref() == key.as_bytes())?;
        let raw = cfg.value.as_ref()?;
        // First try to parse as JSON literal (numbers/bools/objects/arrays)
        if let Ok(v) = serde_json::from_slice::<T>(raw.as_ref()) {
            return Some(v);
        }
        // Fallback: treat raw as UTF-8 string and deserialize from a JSON string
        let s = std::str::from_utf8(raw.as_ref()).ok()?;
        let v = serde_json::Value::String(s.to_string());
        serde_json::from_value(v).ok()
    }
}

#[derive(Debug, PartialEq)]
pub struct Resource {
    pub error_code: KafkaCode,
    pub error_message: Option<Bytes>,
    pub resource_type: i8,
    pub resource_name: Bytes,
    pub configs: Vec<Config>,
}

#[derive(Debug, PartialEq)]
pub struct Config {
    pub name: Bytes,
    pub value: Option<Bytes>,
    pub read_only: bool,
    pub is_default: bool,
    pub is_sensitive: bool,
    pub synonyms: Vec<ConfigSynonym>,
}

#[derive(Debug, PartialEq)]
pub struct ConfigSynonym {
    pub name: Bytes,
    pub value: Option<Bytes>,
    pub source: i8,
}

impl TryFrom<Bytes> for DescribeConfigsResponse {
    type Error = Error;
    fn try_from(s: Bytes) -> Result<Self> {
        let (_, resp) =
            parse_describe_configs_response(NomBytes::new(s.clone())).map_err(|err| {
                tracing::error!("ERROR: Failed parsing DescribeConfigsResponse {:?}", err);
                tracing::error!("ERROR: DescribeConfigsResponse Bytes {:?}", s);
                Error::ParsingError(s)
            })?;
        Ok(resp)
    }
}

pub fn parse_describe_configs_response(s: NomBytes) -> IResult<NomBytes, DescribeConfigsResponse> {
    let (s, header) = parse_header_response(s)?;
    let (s, throttle_time_ms) = nom::number::complete::be_i32(s)?;
    let (s, resources) = parse_array(parse_resource)(s)?;
    Ok((
        s,
        DescribeConfigsResponse {
            header,
            throttle_time_ms,
            resources,
        },
    ))
}

fn parse_resource(s: NomBytes) -> IResult<NomBytes, Resource> {
    let (s, error_code) = parser::parse_kafka_code(s)?;
    let (s, error_message) = parser::parse_nullable_string(s)?;
    let (s, resource_type) = be_i8(s)?;
    let (s, resource_name) = parser::parse_string(s)?;
    let (s, configs) = parse_array(parse_config)(s)?;
    Ok((
        s,
        Resource {
            error_code,
            error_message,
            resource_type,
            resource_name,
            configs,
        },
    ))
}

fn parse_config(s: NomBytes) -> IResult<NomBytes, Config> {
    let (s, name) = parser::parse_string(s)?;
    let (s, value) = parser::parse_nullable_string(s)?;
    let (s, read_only) = parser::parse_boolean(s)?;
    let (s, is_default) = parser::parse_boolean(s)?;
    let (s, is_sensitive) = parser::parse_boolean(s)?;
    // v1 adds synonyms array (may be empty when include_synonyms=false)
    let (s, synonyms) = parse_array(parse_config_synonym)(s)?;
    Ok((
        s,
        Config {
            name,
            value,
            read_only,
            is_default,
            is_sensitive,
            synonyms,
        },
    ))
}

fn parse_config_synonym(s: NomBytes) -> IResult<NomBytes, ConfigSynonym> {
    let (s, name) = parser::parse_string(s)?;
    let (s, value) = parser::parse_nullable_string(s)?;
    let (s, source) = be_i8(s)?;
    Ok((
        s,
        ConfigSynonym {
            name,
            value,
            source,
        },
    ))
}
