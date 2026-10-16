use anyhow::{Context, Error, Result};
use configparser::ini::{Ini, IniDefault};

pub struct Secrets {
    pub local_database_password: String,
    pub rabbitmq_password: String,
    pub secret_key: String,
    pub shared_secret: String,
    pub avatar_salt: String,
}

impl Secrets {
    pub fn load(secrets_file: String) -> Result<Secrets> {
        let mut defaults = IniDefault::default();
        defaults.enable_inline_comments = false;
        let mut secrets_config = Ini::new_from_defaults(defaults);

        secrets_config.load(secrets_file).map_err(Error::msg)?;
        Ok(Secrets {
            local_database_password: secrets_config
                .get("secrets", "local_database_password")
                .with_context(|| "missing local_database_password")?,
            rabbitmq_password: secrets_config
                .get("secrets", "rabbitmq_password")
                .with_context(|| "missing rabbitmq_password")?,
            secret_key: secrets_config
                .get("secrets", "secret_key")
                .with_context(|| "missing secret_key")?,
            shared_secret: secrets_config
                .get("secrets", "shared_secret")
                .with_context(|| "missing shared_secret")?,
            avatar_salt: secrets_config
                .get("secrets", "avatar_salt")
                .with_context(|| "missing avatar_salt")?,
        })
    }
}
