use std::collections::HashMap;
use std::convert::AsRef;
use std::fs::File;
use std::path::Path;

use base64::prelude::*;
use serde::Deserialize;
use url::Url;

#[derive(Debug, Deserialize)]
pub struct Configuration {
    pub gauges: HashMap<String, Gauge>,
}

impl Configuration {
    #[cfg(test)]
    fn from_file<S: Into<String> + AsRef<Path>>(location: S) -> Self {
        serde_yaml::from_reader(File::open(location).expect("Failed to open manifest"))
            .expect("Failed to deserialize")
    }

    pub fn from_base64<S: AsRef<[u8]>>(buffer: S) -> Result<Self, anyhow::Error> {
        let b64 = BASE64_STANDARD.decode(buffer)?;
        Ok(serde_yaml::from_slice(&b64)?)
    }
}

#[derive(Debug, Deserialize)]
pub struct Gauge {
    pub url: Url,
    #[serde(rename = "metric")]
    pub name: String,
    #[serde(rename = "type")]
    pub measurement_type: Measurement,
    pub query: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Measurement {
    Count,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_deser() {
        let _conf: Configuration = Configuration::from_file("manifest.yml");
    }

    #[test]
    fn test_config_b64() {
        let b = b"LS0tCiMgVGhpcyBpcyBhbiBleGFtcGxlIG1hbmlmZXN0IGZpdWxlIHdoaWNoIGNvbmZpZ3VyZXMgdGhlIGxhbWJkYQpnYXVnZXM6CiAgIyBFYWNoIGdhdWdlIHNob3VsZCBoYXZlIGEgZGlzdGluY3QgbmFtZSBmb3IgbWFuYWdpbmcgaW5zaWRlIG9mIHRoZSBsYW1iZGEKICBpbXBvcnRhbnRfbWV0cmljOgogICAgIyBUaGVuIGRlZmluZSBhIG1ldHJpYyBuYW1lIHRvIGV4cG9ydCB0byBjbG91ZHdhdGNoCiAgICBtZXRyaWM6ICdsYXN0XzEwX3VuaXEnCiAgICB1cmw6ICdzMzovL2V4YW1wbGUtYnVja2V0L2RhdGFiYXNlcy9kcy1wYXJ0aXRpb25lZC1kZWx0YS10YWJsZS8nCiAgICAjIEN1cnJlbnRseSBvbmx5IGEgcXVlcnkgaGFuZGxlciB0eXBlIG9mIGBjb3VudGAgaXMgc3VwcG9ydGVkCiAgICB0eXBlOiBjb3VudAogICAgIyBUaGUgZXhhbXBsZSBEYXRhZnVzaW9uIFNRTCBxdWVyeSBiZWxvdyBxdWVyaWVzIHRoZSBzb3VyY2UgdGFibGUsIHdoaWNoIGlzIGRlZmluZWQgYnkKICAgICMgdGhlIFVSTCBhYm92ZSwgdG8gZmluZCBhbGwgdGhlIGRpc3RpbmN0IHV1aWRzIGluIHRoZSBsYXN0IDEwIG1pbnV0ZXMgb2YgdGhlIGN1cnJlbnQKICAgICMgYGRzYCBwYXJ0aXRpb24uCiAgICBxdWVyeTogfAogICAgICBTRUxFQ1QgRElTVElOQ1QgdXVpZCBBUyB0b3RhbCBGUk9NIHNvdXJjZQogICAgICAgIFdIRVJFIGRzID0gQVJST1dfQ0FTVChDVVJSRU5UX0RBVEUoKSAsICdVdGY4JykKICAgICAgICBBTkQgY3JlYXRlZF9hdCA+PSAoQVJST1dfQ0FTVChBUlJPV19DQVNUKE5PVygpLCAnVGltZXN0YW1wKFNlY29uZCwgTm9uZSknKSwgJ0ludDY0JykgLSAoNjAgKiAxMCkpCg==";
        let conf: Configuration = Configuration::from_base64(&b).expect("Failed to deserialize");

        assert_eq!(conf.gauges.len(), 1);
    }
}
