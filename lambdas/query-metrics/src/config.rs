use serde::Deserialize;
use std::collections::HashMap;
use std::convert::AsRef;
use std::fs::File;
use std::path::Path;

use url::Url;
#[derive(Debug, Deserialize)]
pub struct Configuration {
    pub gauges: HashMap<String, Gauge>,
}

impl Configuration {
    pub fn from_file<S: Into<String> + AsRef<Path>>(location: S) -> Self {
        serde_yaml::from_reader(File::open(location).expect("Failed to open manifest"))
            .expect("Failed to deserialize")
    }
}

#[derive(Debug, Deserialize)]
pub struct Gauge {
    pub url: Url,
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
}
