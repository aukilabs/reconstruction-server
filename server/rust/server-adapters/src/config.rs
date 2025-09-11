use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub api_key: Option<String>,
    pub port: String,
    pub log_format: Option<String>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            api_key: None,
            port: ":8080".into(),
            log_format: None,
        }
    }
}
