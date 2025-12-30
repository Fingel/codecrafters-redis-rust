use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::interpreter::{RedisCommand, RedisInterpreter};
use crate::parser::{RedisValueRef, RespParser};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio_util::codec::Decoder;

pub mod interpreter;
pub mod parser;

// Storage Type
#[derive(Debug, Clone, PartialEq)]
enum RedisValue {
    String(Bytes),
    List(Vec<Bytes>),
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

#[derive(Debug, Clone)]
struct RedisDb {
    dict: HashMap<String, RedisValue>,
    ttl: HashMap<String, u64>,
}

impl RedisDb {
    pub fn new() -> Self {
        RedisDb {
            dict: HashMap::new(),
            ttl: HashMap::new(),
        }
    }
}

type Db = Arc<RwLock<RedisDb>>;

async fn process(stream: TcpStream, db: Db) {
    tokio::spawn(async move {
        let mut transport = RespParser.framed(stream);
        let interpreter = RedisInterpreter::new();
        while let Some(redis_value) = transport.next().await {
            match redis_value {
                Ok(value) => match interpreter.interpret(value) {
                    Ok(command) => {
                        let result = handle_command(&db, command).await;
                        transport.send(result).await.unwrap();
                    }
                    Err(err) => {
                        eprintln!("Command interpretation error: {}", err);
                        let resp = RedisValueRef::Error(Bytes::from(format!("ERR {}", err)));
                        transport.send(resp).await.unwrap();
                    }
                },
                Err(err) => {
                    eprintln!("Error processing Redis command: {:?}", err);
                }
            }
        }
    });
}

async fn handle_command(db: &Db, command: RedisCommand) -> RedisValueRef {
    match command {
        RedisCommand::Ping => ping(),
        RedisCommand::Echo(arg) => echo(arg),
        RedisCommand::Set(key, value) => set(db, key, value).await,
        RedisCommand::SetEx(key, value, ttl) => set_ex(db, key, value, ttl).await,
        RedisCommand::Get(key) => get(db, key).await,
        RedisCommand::Rpush(key, value) => rpush(db, key, value).await,
    }
}

fn ping() -> RedisValueRef {
    RedisValueRef::SimpleString(Bytes::from("PONG"))
}

fn echo(arg: Bytes) -> RedisValueRef {
    RedisValueRef::String(arg)
}

async fn set(db: &Db, key: Bytes, value: Bytes) -> RedisValueRef {
    let mut db = db.write().await;
    db.dict.insert(
        String::from_utf8_lossy(&key).to_string(),
        RedisValue::String(value),
    );
    RedisValueRef::SimpleString(Bytes::from("OK"))
}

async fn set_ex(db: &Db, key: Bytes, value: Bytes, ttl: u64) -> RedisValueRef {
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

async fn get(db: &Db, key: Bytes) -> RedisValueRef {
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

async fn rpush(db: &Db, key: Bytes, value: Vec<Bytes>) -> RedisValueRef {
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
            db.dict.insert(key_string, RedisValue::List(value));
            RedisValueRef::Int(1)
        }
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = Arc::new(RwLock::new(RedisDb::new()));

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((stream, _)) => {
                println!("Accepted new connection");
                process(stream, db.clone()).await;
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
                break;
            }
        }
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
