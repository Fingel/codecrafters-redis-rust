use std::collections::HashMap; // TODO: try changing this to DashMap
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::parser::RedisValueRef;
use bytes::Bytes;
use tokio::sync::RwLock; // Todo: check performance of using regular RwLock

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

pub async fn llen(db: &Db, key: Bytes) -> RedisValueRef {
    let key_string = String::from_utf8_lossy(&key).to_string();
    let db_r = db.read().await;
    match db_r.dict.get(&key_string) {
        Some(RedisValue::List(list)) => RedisValueRef::Int(list.len() as i64),
        _ => RedisValueRef::Int(0),
    }
}

pub async fn lpop(db: &Db, key: Bytes, num_elements: Option<u64>) -> RedisValueRef {
    let key_string = String::from_utf8_lossy(&key).to_string();
    let mut db_w = db.write().await;
    match db_w.dict.get_mut(&key_string) {
        Some(RedisValue::List(list)) if !list.is_empty() => {
            // VecDeque should help here
            let num_elements = (num_elements.unwrap_or(1) as usize).min(list.len());
            let ret: Vec<Bytes> = list.drain(0..num_elements).collect();
            if ret.len() == 1 {
                RedisValueRef::String(ret[0].clone())
            } else {
                RedisValueRef::Array(ret.into_iter().map(RedisValueRef::String).collect())
            }
        }
        _ => RedisValueRef::NullBulkString,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn setup() -> Arc<RwLock<RedisDb>> {
        Arc::new(RwLock::new(RedisDb::new()))
    }

    #[tokio::test]
    async fn test_set_get() {
        let db = setup();
        let key = Bytes::from("key");
        let value = Bytes::from("value");

        let result = set(&db, key.clone(), value.clone()).await;
        assert_eq!(result, RedisValueRef::SimpleString(Bytes::from("OK")));

        let result = get(&db, key).await;
        assert_eq!(result, RedisValueRef::String(Bytes::from("value")));
    }

    #[tokio::test]
    async fn test_get_set_expired() {
        let db = setup();
        let key = Bytes::from("key");
        let value = Bytes::from("value");
        let result = set_ex(&db, key.clone(), value.clone(), 1).await;
        assert_eq!(result, RedisValueRef::SimpleString(Bytes::from("OK")));
        tokio::time::sleep(Duration::from_millis(10)).await;
        let result = get(&db, key).await;
        assert_eq!(result, RedisValueRef::NullBulkString);
    }

    #[tokio::test]
    async fn test_get_set_not_expired() {
        let db = setup();
        let key = Bytes::from("key");
        let value = Bytes::from("value");
        let result = set_ex(&db, key.clone(), value.clone(), 1000000).await;
        assert_eq!(result, RedisValueRef::SimpleString(Bytes::from("OK")));

        let result = get(&db, key).await;
        assert_eq!(result, RedisValueRef::String(Bytes::from("value")));
    }

    #[tokio::test]
    async fn test_rpush_new_list() {
        let db = setup();
        let key = Bytes::from("key");
        let value = vec![Bytes::from("value1")];

        let result = rpush(&db, key.clone(), value).await;
        assert_eq!(result, RedisValueRef::Int(1));

        // Push another item
        let value2 = vec![Bytes::from("value2")];
        let result = rpush(&db, key.clone(), value2).await;
        assert_eq!(result, RedisValueRef::Int(2));

        // Get should return the list as an array
        let result = get(&db, key).await;
        assert_eq!(
            result,
            RedisValueRef::Array(vec![
                RedisValueRef::String(Bytes::from("value1")),
                RedisValueRef::String(Bytes::from("value2")),
            ])
        );
    }

    #[tokio::test]
    async fn test_rpush_wrong_type() {
        let db = setup();
        let key = Bytes::from("key");
        let value = Bytes::from("string_value");

        // Set a string value
        let result = set(&db, key.clone(), value).await;
        assert_eq!(result, RedisValueRef::SimpleString(Bytes::from("OK")));

        // Try to rpush to a string key - should fail
        let list_value = vec![Bytes::from("list_item")];
        let result = rpush(&db, key, list_value).await;
        match result {
            RedisValueRef::Error(_) => {} // Expected
            _ => panic!("Expected error when rpush on string key"),
        }
    }

    #[tokio::test]
    async fn test_rpush_multiple() {
        let db = setup();
        let key = Bytes::from("key");
        let value = vec![Bytes::from("value1"), Bytes::from("value2")];

        let result = rpush(&db, key.clone(), value).await;
        assert_eq!(result, RedisValueRef::Int(2));

        // Get should return the list as an array
        let result = get(&db, key).await;
        assert_eq!(
            result,
            RedisValueRef::Array(vec![
                RedisValueRef::String(Bytes::from("value1")),
                RedisValueRef::String(Bytes::from("value2")),
            ])
        );
    }

    #[tokio::test]
    async fn test_lpush() {
        let db = setup();
        let key = Bytes::from("key");
        let value = vec![Bytes::from("c")];

        let result = lpush(&db, key.clone(), value).await;
        assert_eq!(result, RedisValueRef::Int(1));

        let value = vec![Bytes::from("b"), Bytes::from("a")];
        let result = lpush(&db, key.clone(), value).await;
        assert_eq!(result, RedisValueRef::Int(3));

        let result = lrange(&db, key, 0, -1).await;
        assert_eq!(
            result,
            RedisValueRef::Array(vec![
                RedisValueRef::String(Bytes::from("a")),
                RedisValueRef::String(Bytes::from("b")),
                RedisValueRef::String(Bytes::from("c")),
            ])
        );
    }

    #[tokio::test]
    async fn test_lrange() {
        let db = setup();
        let key = Bytes::from("key");
        let value = vec![
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];
        let result = rpush(&db, key.clone(), value).await;
        assert_eq!(result, RedisValueRef::Int(3));

        // Get should return the first two items
        let result = lrange(&db, key, 0, 1).await;
        assert_eq!(
            result,
            RedisValueRef::Array(vec![
                RedisValueRef::String(Bytes::from("value1")),
                RedisValueRef::String(Bytes::from("value2")),
            ])
        );
    }

    #[tokio::test]
    async fn test_lrange_large_upper() {
        let db = setup();
        let key = Bytes::from("key");
        let value = vec![
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];
        let result = rpush(&db, key.clone(), value).await;
        assert_eq!(result, RedisValueRef::Int(3));

        // Get should return all
        let result = lrange(&db, key, 0, 10).await;
        assert_eq!(
            result,
            RedisValueRef::Array(vec![
                RedisValueRef::String(Bytes::from("value1")),
                RedisValueRef::String(Bytes::from("value2")),
                RedisValueRef::String(Bytes::from("value3")),
            ])
        );
    }

    #[tokio::test]
    async fn test_lrange_negative() {
        let db = setup();
        let key = Bytes::from("key");
        let value = vec![
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
            Bytes::from("d"),
            Bytes::from("e"),
        ];
        let result = rpush(&db, key.clone(), value).await;
        assert_eq!(result, RedisValueRef::Int(5));

        // Matches example test on #RI1
        let result = lrange(&db, key, 2, -1).await;
        assert_eq!(
            result,
            RedisValueRef::Array(vec![
                RedisValueRef::String(Bytes::from("c")),
                RedisValueRef::String(Bytes::from("d")),
                RedisValueRef::String(Bytes::from("e")),
            ])
        );
    }

    #[tokio::test]
    async fn test_llen() {
        let db = setup();
        let key = Bytes::from("key");
        let value = vec![
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
            Bytes::from("d"),
        ];
        let result = rpush(&db, key.clone(), value).await;
        assert_eq!(result, RedisValueRef::Int(4));

        // Matches example test on #FV6
        let result = llen(&db, key).await;
        assert_eq!(result, RedisValueRef::Int(4));

        // Non-existent key
        let result = llen(&db, Bytes::from("nonexistent")).await;
        assert_eq!(result, RedisValueRef::Int(0));
    }

    #[tokio::test]
    async fn test_lpop() {
        let db = setup();
        let key = Bytes::from("key");
        let value = vec![Bytes::from("a")];
        let result = rpush(&db, key.clone(), value).await;
        assert_eq!(result, RedisValueRef::Int(1));

        // Matches example test on #EF1
        let result = lpop(&db, key.clone(), None).await;
        assert_eq!(result, RedisValueRef::String(Bytes::from("a")));

        // Should now be empty
        let result = lpop(&db, key, None).await;
        assert_eq!(result, RedisValueRef::NullBulkString);

        // Non-existent key
        let result = lpop(&db, Bytes::from("nonexistent"), None).await;
        assert_eq!(result, RedisValueRef::NullBulkString);
    }

    #[tokio::test]
    async fn test_lpop_multiple() {
        let db = setup();
        let key = Bytes::from("key");
        let value = vec![
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
            Bytes::from("d"),
        ];
        let result = rpush(&db, key.clone(), value).await;
        assert_eq!(result, RedisValueRef::Int(4));

        // Matches example test on #JP1
        let result = lpop(&db, key.clone(), Some(2)).await;
        assert_eq!(
            result,
            RedisValueRef::Array(vec![
                RedisValueRef::String(Bytes::from("a")),
                RedisValueRef::String(Bytes::from("b"))
            ])
        );

        // List should now only contain c and d
        let result = lrange(&db, key.clone(), 0, -1).await;
        assert_eq!(
            result,
            RedisValueRef::Array(vec![
                RedisValueRef::String(Bytes::from("c")),
                RedisValueRef::String(Bytes::from("d"))
            ])
        );

        // Should remove all elements
        let result = lpop(&db, key.clone(), Some(100)).await;
        assert_eq!(
            result,
            RedisValueRef::Array(vec![
                RedisValueRef::String(Bytes::from("c")),
                RedisValueRef::String(Bytes::from("d"))
            ])
        );
    }

    #[test]
    fn test_redis_value_trait_conversions() {
        use std::convert::TryInto;

        // Test From<&RedisValue> for RedisValueRef
        let stored_string = RedisValue::String(Bytes::from("hello"));
        let protocol: RedisValueRef = (&stored_string).into();
        assert_eq!(protocol, RedisValueRef::String(Bytes::from("hello")));

        // Test with list
        let stored_list = RedisValue::List(vec![Bytes::from("a"), Bytes::from("b")]);
        let protocol: RedisValueRef = (&stored_list).into();
        match protocol {
            RedisValueRef::Array(items) => assert_eq!(items.len(), 2),
            _ => panic!("Expected array"),
        }

        // Test TryFrom<RedisValueRef> for RedisValue
        let protocol_string = RedisValueRef::String(Bytes::from("world"));
        let stored: Result<RedisValue, _> = protocol_string.try_into();
        assert!(stored.is_ok());
        assert_eq!(stored.unwrap(), RedisValue::String(Bytes::from("world")));

        // Test that invalid conversions fail
        let protocol_error = RedisValueRef::Error(Bytes::from("ERR"));
        let stored: Result<RedisValue, _> = protocol_error.try_into();
        assert!(stored.is_err());
    }

    #[test]
    fn test_expect_string_helper() {
        // Test successful extraction
        let value = RedisValueRef::String(Bytes::from("hello"));
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
