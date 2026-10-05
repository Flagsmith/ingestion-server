use std::env;

use crate::sink::KafkaAuth;

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: String,
    pub kafka_bootstrap_servers: String,
    pub kafka_topic: String,
    pub kafka_external_topic: String,
    pub kafka_auth: KafkaAuth,
    pub database_url: String,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let kafka_auth = parse_auth(
            &env::var("KAFKA_AUTH").unwrap_or_else(|_| "scram".to_string()),
            env::var("KAFKA_USERNAME").ok(),
            env::var("KAFKA_PASSWORD").ok(),
        )?;

        Ok(Config {
            listen_addr: env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string()),
            kafka_bootstrap_servers: required("KAFKA_BOOTSTRAP_SERVERS")?,
            kafka_topic: required("KAFKA_TOPIC")?,
            kafka_external_topic: required("KAFKA_EXTERNAL_TOPIC")?,
            kafka_auth,
            database_url: required("DATABASE_URL")?,
        })
    }
}

fn required(name: &'static str) -> Result<String, ConfigError> {
    env::var(name).map_err(|_| ConfigError::MissingRequired(name))
}

fn parse_auth(
    mode: &str,
    username: Option<String>,
    password: Option<String>,
) -> Result<KafkaAuth, ConfigError> {
    match mode {
        "scram" => Ok(KafkaAuth::Scram {
            username: username.ok_or(ConfigError::MissingRequired("KAFKA_USERNAME"))?,
            password: password.ok_or(ConfigError::MissingRequired("KAFKA_PASSWORD"))?,
        }),
        "none" => Ok(KafkaAuth::None),
        other => Err(ConfigError::Invalid("KAFKA_AUTH", other.to_string())),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Missing required environment variable: {0}")]
    MissingRequired(&'static str),
    #[error("Invalid value for {0}: {1:?} (expected scram or none)")]
    Invalid(&'static str, String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_auth_builds_scram_from_credentials() {
        let auth = parse_auth("scram", Some("u".to_string()), Some("p".to_string())).unwrap();
        assert_eq!(
            auth,
            KafkaAuth::Scram {
                username: "u".to_string(),
                password: "p".to_string(),
            }
        );
    }

    #[test]
    fn parse_auth_requires_both_scram_credentials() {
        assert!(matches!(
            parse_auth("scram", Some("u".to_string()), None),
            Err(ConfigError::MissingRequired("KAFKA_PASSWORD"))
        ));
        assert!(matches!(
            parse_auth("scram", None, Some("p".to_string())),
            Err(ConfigError::MissingRequired("KAFKA_USERNAME"))
        ));
    }

    #[test]
    fn parse_auth_accepts_none_without_credentials() {
        assert_eq!(parse_auth("none", None, None).unwrap(), KafkaAuth::None);
    }

    #[test]
    fn parse_auth_rejects_unknown_value() {
        assert!(matches!(
            parse_auth("iam", None, None),
            Err(ConfigError::Invalid("KAFKA_AUTH", v)) if v == "iam"
        ));
    }
}
