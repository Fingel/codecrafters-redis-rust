use crate::{
    Db,
    parser::{RArray, RString, RedisValueRef},
};

pub fn aclwhoami(_db: &Db) -> RedisValueRef {
    RString("default")
}

pub fn aclgetuser(_db: &Db, _user: String) -> RedisValueRef {
    RArray(vec![RString("flags"), RArray(vec![])])
}
