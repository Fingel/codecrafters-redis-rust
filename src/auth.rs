use sha2::{Digest, Sha256};

use crate::{
    Db,
    parser::{RArray, RSimpleString, RString, RedisValueRef},
};

#[derive(Debug, Clone, Default)]
pub struct User {
    password: String,
}

pub fn aclwhoami(_db: &Db) -> RedisValueRef {
    RString("default")
}

pub fn aclgetuser(db: &Db, user: String) -> RedisValueRef {
    let db_guard = db.users.lock().unwrap();
    let password = db_guard.get(&user).map(|user| user.password.clone());
    let passwords = match password {
        Some(password) => vec![RString(password)],
        None => vec![],
    };
    let flags = if passwords.is_empty() {
        vec![RString("nopass")]
    } else {
        vec![]
    };

    RArray(vec![
        RString("flags"),
        RArray(flags),
        RString("passwords"),
        RArray(passwords),
    ])
}

pub fn aclsetuser(db: &Db, username: String, password: String) -> RedisValueRef {
    let mut db_guard = db.users.lock().unwrap();
    let digest = Sha256::digest(password.as_bytes());
    // format bytes as hex string
    let password_hash = format!("{:x}", digest);
    let lower_hash = password_hash.to_lowercase();

    // Entry API for the win
    db_guard
        .entry(username.clone())
        .and_modify(|user| user.password = lower_hash.clone())
        .or_insert_with(|| User {
            password: lower_hash,
        });

    RSimpleString("OK")
}
