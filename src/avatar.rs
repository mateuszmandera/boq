use serde::Deserialize;
use url::Url;

use crate::avatar_hash::user_avatar_base_path_from_ids;
use crate::types::{RealmId, UserId};
use crate::upload::{MEDIUM_AVATAR_SIZE, get_avatar_url};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum AvatarSource {
    #[serde(rename = "G")]
    Gravatar,
    #[serde(rename = "U")]
    User,
    #[serde(rename = "J")]
    Jdenticon,
}

pub struct AvatarSettings {
    pub enable_gravatar: bool,
    pub default_avatar_uri: String,
    pub avatar_salt: String,
}

/// Most of the parameters to this function map to fields by the same name in
/// `UserProfile` (`avatar_source`, `realm_id`, `email`, etc.).
///
/// Then there are these:
///
/// `medium` - This means we want a medium-sized avatar. This can affect the `s`
/// parameter for gravatar avatars, or it can give us something like
/// `foo-medium.png` for user-uploaded avatars.
///
/// `client_gravatar` - If the client can compute their own gravatars, this will
/// be set to `true`, and we'll avoid computing them on the server (mostly to
/// save bandwidth).
pub fn get_avatar_field(
    user_id: UserId,
    realm_id: RealmId,
    email: &str,
    avatar_source: AvatarSource,
    avatar_version: i32,
    medium: bool,
    client_gravatar: bool,
    settings: &AvatarSettings,
) -> Option<String> {
    // TODO/boq: Cross-realm bots have hard-coded avatars. We need to port the
    // relevant chunk of logic from the python implementation to here.

    // If our client knows how to calculate gravatar hashes, we will return
    // `None` and let the client compute the gravatar url.
    if settings.enable_gravatar && client_gravatar && avatar_source == AvatarSource::Gravatar {
        return None;
    }

    // If we get this far, we'll compute an avatar URL based on the avatar
    // source, and then we'll add version info to try to avoid stale caches.

    match avatar_source {
        AvatarSource::User | AvatarSource::Jdenticon => {
            let hash_key =
                user_avatar_base_path_from_ids(user_id, avatar_version, realm_id, settings);
            Some(get_avatar_url(hash_key.as_str(), medium))
        }
        AvatarSource::Gravatar => Some(get_gravatar_url(email, avatar_version, medium, settings)),
    }
}

fn get_gravatar_url(
    email: &str,
    avatar_version: i32,
    medium: bool,
    settings: &AvatarSettings,
) -> String {
    // TODO/boq: Support `GRAVATAR_REALM_OVERRIDE`, which lets a deployment
    // disable gravatar for individual realms; we only have the global setting.
    let url = if settings.enable_gravatar {
        // gravatar_rs defaults to www.gravatar.com, but the web app only
        // recognizes secure.gravatar.com as a gravatar URL.
        let mut generator = gravatar_rs::Generator::default().set_base_url("secure.gravatar.com");
        generator.default_image = Some("identicon".to_string());
        generator.image_size = medium.then_some(MEDIUM_AVATAR_SIZE);
        generator.generate(email)
    } else {
        settings.default_avatar_uri.to_string()
    };

    // default_avatar_uri is configurable and may be a site-relative path, which
    // Url::parse rejects.
    match Url::parse(&url) {
        Ok(mut parsed) => {
            parsed
                .query_pairs_mut()
                .append_pair("version", &avatar_version.to_string());
            parsed.to_string()
        }
        Err(_) => {
            let separator = if url.contains('?') { "&" } else { "?" };
            format!("{url}{separator}version={avatar_version}")
        }
    }
}
