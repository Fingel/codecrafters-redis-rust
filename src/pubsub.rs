use crate::{
    Db,
    parser::{RArray, RInt, RString, RedisValueRef},
};

pub fn subscribe(_db: &Db, channel: String) -> RedisValueRef {
    RArray(vec![RString("subscribe"), RString(channel), RInt(1)])
}
