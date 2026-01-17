use sha2::{Digest, Sha256};

use crate::{
    Db,
    interpreter::RedisCommand,
    parser::{RArray, RError, RSimpleString, RString, RedisValueRef},
};

#[derive(Debug, Clone, Default)]
pub struct User {
    password: String,
}

pub fn aclwhoami(_db: &Db) -> RedisValueRef {
    RString("default")
}

fn password_hash(password: &str) -> String {
    let digest = Sha256::digest(password.as_bytes());
    // format bytes as hex string
    let hash = format!("{:x}", digest);
    hash.to_lowercase()
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
    let password_hash = password_hash(&password);
    // Entry API for the win
    db_guard
        .entry(username.clone())
        .and_modify(|user| user.password = password_hash.clone())
        .or_insert_with(|| User {
            password: password_hash,
        });

    RSimpleString("OK")
}

pub fn auth(db: &Db, username: String, password: String) -> RedisValueRef {
    let db_guard = db.users.lock().unwrap();
    let user_password = db_guard.get(&username).map(|user| user.password.clone());
    match user_password {
        Some(user_password) => {
            let password_hash = password_hash(&password);
            if user_password == password_hash {
                RSimpleString("OK")
            } else {
                RError("WRONGPASS invalid username-password pair or user is disabled.")
            }
        }
        None => RSimpleString("OK"),
    }
}

pub fn check_auth(db: &Db, command: &RedisCommand) -> bool {
    // Need to handle case of default user with no password set
    match command {
        RedisCommand::Auth(username, password) => {
            let result = auth(db, username.clone(), password.clone());
            result == RSimpleString("OK")
        }
        _ => db.users.lock().unwrap().is_empty(),
    }
}
