use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::parser::RedisValueRef;
use crate::streams::StreamCollection;
use bytes::Bytes;
use dashmap::DashMap;

pub mod interpreter;
pub mod lists;
pub mod parser;
pub mod replication;
pub mod streams;

// Storage Type
#[derive(Debug, Clone, PartialEq)]
pub enum RedisValue {
    String(Bytes),
    List(VecDeque<Bytes>),
    Stream(StreamCollection),
}

/// Convert from storage format to wire protocol format
impl From<&RedisValue> for RedisValueRef {
    fn from(value: &RedisValue) -> Self {
        match value {
            RedisValue::String(s) => RedisValueRef::String(s.clone()),
            RedisValue::List(items) => items
                .iter()
                .map(|item| RedisValueRef::String(item.clone()))
                .collect::<Vec<RedisValueRef>>()
                .into(),
            RedisValue::Stream(stream_collection) => stream_collection
                .all()
                .into_iter()
                .map(|e| e.into())
                .collect::<Vec<RedisValueRef>>()
                .into(),
        }
    }
}

pub fn ref_error(msg: &str) -> RedisValueRef {
    RedisValueRef::Error(Bytes::from(msg.to_string()))
}

#[derive(Debug, Clone, Default)]
pub struct RedisDb {
    pub dict: DashMap<String, RedisValue>,
    pub ttl: DashMap<String, u64>,
    pub waiters: Arc<Mutex<HashMap<String, VecDeque<tokio::sync::oneshot::Sender<Bytes>>>>>,
    pub stream_waiters:
        Arc<Mutex<HashMap<String, VecDeque<tokio::sync::oneshot::Sender<RedisValueRef>>>>>,
    pub replica_of: Option<(String, u16)>,
    pub replication_id: String,
    pub replication_offset: u64,
}

impl RedisDb {
    pub fn new(replica_of: Option<(String, u16)>) -> Self {
        RedisDb {
            dict: DashMap::new(),
            ttl: DashMap::new(),
            waiters: Arc::new(Mutex::new(HashMap::new())),
            stream_waiters: Arc::new(Mutex::new(HashMap::new())),
            replica_of,
            replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            replication_offset: 0,
        }
    }

    fn is_expired(&self, key: &str) -> bool {
        if let Some(expiry) = self.ttl.get(key) {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            *expiry < now
        } else {
            false
        }
    }

    fn remove_if_expired(&self, key: &str) -> bool {
        if self.is_expired(key) {
            self.dict.remove(key);
            self.ttl.remove(key);
            true
        } else {
            false
        }
    }

    pub fn get_if_valid(
        &self,
        key: &str,
    ) -> Option<dashmap::mapref::one::Ref<'_, String, RedisValue>> {
        if self.remove_if_expired(key) {
            None
        } else {
            self.dict.get(key)
        }
    }

    pub fn get_mut_if_valid(
        &self,
        key: &str,
    ) -> Option<dashmap::mapref::one::RefMut<'_, String, RedisValue>> {
        if self.remove_if_expired(key) {
            None
        } else {
            self.dict.get_mut(key)
        }
    }
}

pub type Db = Arc<RedisDb>;

pub fn ping() -> RedisValueRef {
    RedisValueRef::SimpleString(Bytes::from("PONG"))
}

pub fn echo(arg: String) -> RedisValueRef {
    arg.into()
}

pub async fn set(db: &Db, key: String, value: String) -> RedisValueRef {
    db.dict.insert(key, RedisValue::String(Bytes::from(value)));
    RedisValueRef::SimpleString(Bytes::from("OK"))
}

pub async fn set_ex(db: &Db, key: String, value: String, ttl: u64) -> RedisValueRef {
    let expiry = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64)
        .saturating_add(ttl);

    db.dict
        .insert(key.clone(), RedisValue::String(Bytes::from(value)));
    db.ttl.insert(key, expiry);
    RedisValueRef::SimpleString(Bytes::from("OK"))
}

pub async fn get(db: &Db, key: String) -> RedisValueRef {
    match db.get_if_valid(&key) {
        Some(value) => RedisValueRef::from(&*value),
        None => RedisValueRef::NullBulkString,
    }
}

pub async fn _type(db: &Db, key: String) -> RedisValueRef {
    let result = match db.get_if_valid(&key) {
        Some(entry) => match *entry {
            RedisValue::String(_) => "string",
            RedisValue::List(_) => "list",
            RedisValue::Stream(_) => "stream",
        },
        None => "none",
    };
    RedisValueRef::SimpleString(Bytes::from(result))
}

pub async fn incr(db: &Db, key: String) -> RedisValueRef {
    let result = match db.get_if_valid(&key) {
        Some(entry) => match &*entry {
            RedisValue::String(value) => {
                let new_value = String::from_utf8_lossy(value).to_string();
                let new_value = match new_value.parse::<i64>() {
                    Ok(num) => num,
                    Err(_) => {
                        return RedisValueRef::Error(
                            "ERR value is not an integer or out of range".into(),
                        );
                    }
                };
                new_value + 1
            }
            _ => {
                return RedisValueRef::Error(Bytes::from("WRONGTYPE"));
            }
        },
        None => 1,
    };
    db.dict
        .insert(key, RedisValue::String(Bytes::from(result.to_string())));
    RedisValueRef::Int(result)
}

pub async fn info(db: &Db, _section: String) -> RedisValueRef {
    let role = if db.replica_of.is_some() {
        "slave"
    } else {
        "master"
    };
    let info = format!(
        "# Replication\n\
        role:{}\n\
        master_replid:{}\n\
        master_repl_offset:{}\n",
        role, db.replication_id, db.replication_offset
    );
    info.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn setup() -> Arc<RedisDb> {
        Arc::new(RedisDb::new(None))
    }

    #[tokio::test]
    async fn test_set_get() {
        let db = setup();
        let key = "key".to_string();
        let value = "value".to_string();

        let result = set(&db, key.clone(), value.clone()).await;
        assert_eq!(result, RedisValueRef::SimpleString(Bytes::from("OK")));

        let result = get(&db, key).await;
        assert_eq!(result, "value".into());
    }

    #[tokio::test]
    async fn test_get_set_expired() {
        let db = setup();
        let key = "key".to_string();
        let value = "value".to_string();
        let result = set_ex(&db, key.clone(), value.clone(), 1).await;
        assert_eq!(result, RedisValueRef::SimpleString(Bytes::from("OK")));
        tokio::time::sleep(Duration::from_millis(10)).await;
        let result = get(&db, key).await;
        assert_eq!(result, RedisValueRef::NullBulkString);
    }

    #[tokio::test]
    async fn test_get_set_not_expired() {
        let db = setup();
        let key = "key".to_string();
        let value = "value".to_string();
        let result = set_ex(&db, key.clone(), value.clone(), 1000000).await;
        assert_eq!(result, RedisValueRef::SimpleString(Bytes::from("OK")));

        let result = get(&db, key).await;
        assert_eq!(result, "value".into());
    }

    #[tokio::test]
    async fn test_type() {
        let db = setup();
        let key = "test_key".to_string();
        let value = "test_value".to_string();

        let result = set(&db, key.clone(), value.clone()).await;
        assert_eq!(result, RedisValueRef::SimpleString(Bytes::from("OK")));

        let result = _type(&db, key).await;
        assert_eq!(result, RedisValueRef::SimpleString(Bytes::from("string")));

        let result = _type(&db, "notexist".to_string()).await;
        assert_eq!(result, RedisValueRef::SimpleString(Bytes::from("none")));
    }

    #[tokio::test]
    async fn test_incr() {
        let db = setup();
        let key = "test_key".to_string();
        let result = incr(&db, key.clone()).await;
        assert_eq!(result, RedisValueRef::Int(1));

        let result = incr(&db, key.clone()).await;
        assert_eq!(result, RedisValueRef::Int(2));
    }

    #[tokio::test]
    async fn test_incr_not_number() {
        let db = setup();
        let key = "test_key".to_string();
        set(&db, key.clone(), "stringlol".to_string()).await;

        let result = incr(&db, key.clone()).await;
        assert!(matches!(result, RedisValueRef::Error(_)));
    }

    #[test]
    fn test_redis_value_trait_conversions() {
        // Test From<&RedisValue> for RedisValueRef
        let stored_string = RedisValue::String(Bytes::from("hello"));
        let protocol: RedisValueRef = (&stored_string).into();
        assert_eq!(protocol, "hello".into());

        // Test with list
        let stored_list =
            RedisValue::List(VecDeque::from(vec![Bytes::from("a"), Bytes::from("b")]));
        let protocol: RedisValueRef = (&stored_list).into();
        match protocol {
            RedisValueRef::Array(items) => assert_eq!(items.len(), 2),
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_expect_string_helper() {
        // Test successful extraction
        let value: RedisValueRef = "hello".into();
        let result = value.expect_string();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Bytes::from("hello"));

        // Test error on non-string
        let value = RedisValueRef::Int(42);
        let result = value.expect_string();
        assert!(result.is_err());
        match result {
            Err(RedisValueRef::Error(_)) => {} // Expected
            _ => panic!("Expected error response"),
        }
    }
}
