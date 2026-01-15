use crate::{
    Db,
    parser::{RInt, RedisValueRef},
};

pub fn geoadd(_db: &Db, _set: String, _lng: f64, _lat: f64, _member: String) -> RedisValueRef {
    RInt(1)
}
