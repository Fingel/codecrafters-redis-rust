use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::parser::RedisValueRef;
use bytes::Bytes;
use tokio::sync::RwLock;

pub mod interpreter;
pub mod parser;

// Storage Type
#[derive(Debug, Clone, PartialEq)]
pub enum RedisValue {
    String(Bytes),
    List(Vec<Bytes>), // TODO: use a VecDeque for better performance on front operations
}

/// Convert from storage format to wire protocol format
impl From<&RedisValue> for RedisValueRef {
    fn from(value: &RedisValue) -> Self {
        match value {
            RedisValue::String(s) => RedisValueRef::String(s.clone()),
            RedisValue::List(items) => RedisValueRef::Array(
                items
                    .iter()
                    .map(|item| RedisValueRef::String(item.clone()))
                    .collect(),
            ),
        }
    }
}

/// Convert from wire protocol format to storage format
impl TryFrom<RedisValueRef> for RedisValue {
    type Error = String;

    fn try_from(value: RedisValueRef) -> Result<Self, Self::Error> {
        match value {
            RedisValueRef::String(s) => Ok(RedisValue::String(s)),
            RedisValueRef::Array(items) => {
                // Convert array of RedisValueRef to List of Bytes
                let mut bytes_vec = Vec::new();
                for item in items {
                    match item {
                        RedisValueRef::String(s) => bytes_vec.push(s),
                        _ => return Err("List can only contain strings".to_string()),
                    }
                }
                Ok(RedisValue::List(bytes_vec))
            }
            _ => Err("Cannot convert this RedisValueRef to storage format".to_string()),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RedisDb {
    pub dict: HashMap<String, RedisValue>,
    pub ttl: HashMap<String, u64>,
}

impl RedisDb {
    pub fn new() -> Self {
        RedisDb {
            dict: HashMap::new(),
            ttl: HashMap::new(),
        }
    }
}

pub type Db = Arc<RwLock<RedisDb>>;

pub fn ping() -> RedisValueRef {
    RedisValueRef::SimpleString(Bytes::from("PONG"))
}

pub fn echo(arg: Bytes) -> RedisValueRef {
    RedisValueRef::String(arg)
}

pub async fn set(db: &Db, key: Bytes, value: Bytes) -> RedisValueRef {
    let mut db = db.write().await;
    db.dict.insert(
        String::from_utf8_lossy(&key).to_string(),
        RedisValue::String(value),
    );
    RedisValueRef::SimpleString(Bytes::from("OK"))
}

pub async fn set_ex(db: &Db, key: Bytes, value: Bytes, ttl: u64) -> RedisValueRef {
    let key_string = String::from_utf8_lossy(&key).to_string();
    let expiry = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64)
        .saturating_add(ttl);

    let mut db = db.write().await;
    db.dict
        .insert(key_string.clone(), RedisValue::String(value));
    db.ttl.insert(key_string, expiry);
    RedisValueRef::SimpleString(Bytes::from("OK"))
}

pub async fn get(db: &Db, key: Bytes) -> RedisValueRef {
    let key_string = String::from_utf8_lossy(&key).to_string();
    // Check if expired with read lock
    let is_expired = {
        let db_r = db.read().await;
        if let Some(expiry) = db_r.ttl.get(&key_string) {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            *expiry < now
        } else {
            false
        }
    };

    // If expired, remove both entries using write lock
    if is_expired {
        let mut db_w = db.write().await;
        db_w.dict.remove(&key_string);
        db_w.ttl.remove(&key_string);
        return RedisValueRef::NullBulkString;
    }

    // If not expired, return value using read lock
    let db_r = db.read().await;
    match db_r.dict.get(&key_string) {
        Some(value) => value.into(),
        None => RedisValueRef::NullBulkString,
    }
}

pub async fn rpush(db: &Db, key: Bytes, value: Vec<Bytes>) -> RedisValueRef {
    let key_string = String::from_utf8_lossy(&key).to_string();
    let mut db = db.write().await;
    match db.dict.get_mut(&key_string) {
        Some(RedisValue::List(list)) => {
            list.extend(value);
            RedisValueRef::Int(list.len() as i64)
        }
        Some(RedisValue::String(_)) => RedisValueRef::Error(Bytes::from(
            "Attempted to push to an array of the wrong type",
        )),
        None => {
            let num_items = value.len() as i64;
            db.dict.insert(key_string, RedisValue::List(value));
            RedisValueRef::Int(num_items)
        }
    }
}

pub async fn lpush(db: &Db, key: Bytes, value: Vec<Bytes>) -> RedisValueRef {
    let key_string = String::from_utf8_lossy(&key).to_string();
    let mut db = db.write().await;
    match db.dict.get_mut(&key_string) {
        Some(RedisValue::List(list)) => {
            let mut reversed = value.clone();
            reversed.reverse();
            list.splice(0..0, reversed);
            RedisValueRef::Int(list.len() as i64)
        }
        Some(RedisValue::String(_)) => RedisValueRef::Error(Bytes::from(
            "Attempted to push to an array of the wrong type",
        )),
        None => {
            let num_items = value.len() as i64;
            db.dict.insert(key_string, RedisValue::List(value));
            RedisValueRef::Int(num_items)
        }
    }
}

pub async fn lrange(db: &Db, key: Bytes, start: i64, stop: i64) -> RedisValueRef {
    let key_string = String::from_utf8_lossy(&key).to_string();
    let db_r = db.read().await;
    let bytes: Vec<Bytes> = match db_r.dict.get(&key_string) {
        Some(RedisValue::List(list)) => {
            let list_len = list.len() as i64;
            let start = if start < 0 && start.abs() >= list_len {
                0
            } else if start < 0 {
                start + list_len
            } else {
                start.min(list_len - 1)
            };

            let stop = if stop < 0 && stop.abs() >= list_len {
                0
            } else if stop < 0 {
                stop + list_len
            } else {
                stop.min(list_len - 1)
            };

            if start >= list_len || start > stop {
                vec![]
            } else {
                list[start as usize..=stop as usize].to_vec()
            }
        }
        _ => vec![],
    };
    let refs = bytes.into_iter().map(RedisValueRef::String).collect();
    RedisValueRef::Array(refs)
}
