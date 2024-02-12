use std::collections::HashMap;
use std::convert::AsRef;
use std::fs::File;
use std::path::Path;

use base64::prelude::*;
use serde::Deserialize;
use url::Url;

#[derive(Debug, Deserialize)]
pub struct Configuration {
    pub gauges: HashMap<String, Vec<Gauge>>,
}

impl Configuration {
    pub fn from_file<S: Into<String> + AsRef<Path>>(location: S) -> Self {
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
    DimensionalCount,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_deser() {
        let _conf: Configuration = Configuration::from_file("manifest.yml");
    }

    #[test]
    fn test_config_b64() -> anyhow::Result<()> {
        use std::io::Read;
        let mut manifest = File::open("manifest.yml")?;
        let mut buf = vec![];
        let count = manifest.read_to_end(&mut buf)?;
        let b = BASE64_STANDARD.encode(buf);
        let conf: Configuration = Configuration::from_base64(&b).expect("Failed to deserialize");

        assert_eq!(conf.gauges.len(), 1);
        Ok(())
    }
}
