use serde::Deserialize;
use url::Url;

use crate::avatar_hash::user_avatar_path_from_ids;
use crate::types::{RealmId, UserId};
use crate::upload::{MEDIUM_AVATAR_SIZE, get_avatar_url};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum AvatarSource {
    #[serde(rename = "G")]
    Gravatar,
    #[serde(rename = "U")]
    User,
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
    // If our client knows how to calculate gravatar hashes, we will return
    // `None` and let the client compute the gravatar url.
    if settings.enable_gravatar && client_gravatar && avatar_source == AvatarSource::Gravatar {
        return None;
    }

    // If we get this far, we'll compute an avatar URL that may be either
    // user-uploaded or a gravatar, and then we'll add version info to try to
    // avoid stale caches.

    let url = get_unversioned_avatar_url(
        user_id,
        avatar_source,
        realm_id,
        Some(email),
        medium,
        settings,
    );
    let mut url = Url::parse(&url).unwrap();
    url.query_pairs_mut()
        .append_pair("version", &avatar_version.to_string());
    Some(url.to_string())
}

fn get_unversioned_gravatar_url(email: &str, medium: bool, settings: &AvatarSettings) -> String {
    if settings.enable_gravatar {
        let mut generator = gravatar_rs::Generator::default();
        generator.default_image = Some("identicon".to_string());
        generator.image_size = medium.then_some(MEDIUM_AVATAR_SIZE);
        generator.generate(email)
    } else {
        settings.default_avatar_uri.to_string()
    }
}

fn get_unversioned_avatar_url(
    user_profile_id: UserId,
    avatar_source: AvatarSource,
    realm_id: RealmId,
    email: Option<&str>,
    medium: bool,
    settings: &AvatarSettings,
) -> String {
    match avatar_source {
        AvatarSource::User => {
            let hash_key = user_avatar_path_from_ids(user_profile_id, realm_id, settings);
            get_avatar_url(&hash_key, medium)
        }
        AvatarSource::Gravatar => get_unversioned_gravatar_url(email.unwrap(), medium, settings),
    }
}
