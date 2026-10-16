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
    pub fn load(secrets_file: &str) -> Result<Secrets> {
        let content = std::fs::read_to_string(secrets_file)
            .with_context(|| format!("failed to read {secrets_file}"))?;

        Self::parse(content)
    }

    fn parse(content: String) -> Result<Secrets> {
        let mut defaults = IniDefault::default();
        defaults.enable_inline_comments = false;
        let mut secrets_config = Ini::new_from_defaults(defaults);

        secrets_config.read(content).map_err(Error::msg)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secret_with_comment_characters() {
        // Zulip's secret_key is drawn from an alphabet that includes `#`.
        let content = String::from(
            "[secrets]\n\
             local_database_password = password\n\
             rabbitmq_password = password\n\
             secret_key = abc#def;ghi!@$%^&*(-_=+)\n\
             shared_secret = shared\n\
             avatar_salt = salt\n",
        );
        let secrets = Secrets::parse(content).unwrap();

        assert_eq!(secrets.secret_key, "abc#def;ghi!@$%^&*(-_=+)");
    }

    #[test]
    fn test_full_line_comments() {
        let content = String::from(
            "# A comment\n\
             ; Another comment\n\
             [secrets]\n\
             local_database_password = password\n\
             rabbitmq_password = password\n\
             # secret_key = commented out\n\
             secret_key = key\n\
             shared_secret = shared\n\
             avatar_salt = salt\n",
        );
        let secrets = Secrets::parse(content).unwrap();

        assert_eq!(secrets.secret_key, "key");
    }
}
