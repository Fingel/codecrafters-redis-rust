use crate::{
    Db,
    parser::{RString, RedisValueRef},
};

pub fn acl(_db: &Db, command: String) -> RedisValueRef {
    if command == "WHOAMI" {
        RString("default")
    } else {
        RString("OK")
    }
}
