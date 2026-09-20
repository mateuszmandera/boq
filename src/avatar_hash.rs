use sha2::{Digest, Sha256};

use crate::{
    avatar::AvatarSettings,
    types::{RealmId, UserId},
};

fn user_avatar_hash(uid: &str, avatar_version_str: &str, avatar_salt: &str) -> String {
    // WARNING: If this method is changed, you may need to do a migration
    // similar to zerver/migrations/0060_move_avatars_to_be_uid_based.py .
    //
    // The salt prevents unauthenticated clients from enumerating the
    // avatars of all users.

    let user_key = format!("{uid}:{avatar_version_str}:{avatar_salt}");

    let mut hasher = Sha256::new();
    hasher.update(user_key.as_bytes());

    hex::encode(hasher.finalize())[..40].to_string()
}

pub fn user_avatar_base_path_from_ids(
    user_profile_id: UserId,
    avatar_version: i32,
    realm_id: RealmId,
    settings: &AvatarSettings,
) -> String {
    let user_id_hash = user_avatar_hash(
        &user_profile_id.to_string(),
        &avatar_version.to_string(),
        &settings.avatar_salt,
    );
    format!("{realm_id}/{user_id_hash}")
}
