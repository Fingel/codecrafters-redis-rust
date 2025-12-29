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

#[derive(Debug, Clone)]
struct RedisDb {
    dict: HashMap<String, Bytes>,
    ttl: HashMap<String, u64>,
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
    }
}

fn ping() -> RedisValueRef {
    RedisValueRef::SimpleString(Bytes::from("PONG"))
}

fn echo(arg: RedisValueRef) -> RedisValueRef {
    arg
}

async fn set(db: &Db, key: RedisValueRef, value: RedisValueRef) -> RedisValueRef {
    match value {
        RedisValueRef::String(s) => {
            let mut db = db.write().await;
            db.dict.insert(key.to_string(), s);
            RedisValueRef::SimpleString(Bytes::from("OK"))
        }
        _ => RedisValueRef::Error(Bytes::from("ERR value must be a string")),
    }
}

async fn set_ex(db: &Db, key: RedisValueRef, value: RedisValueRef, ttl: u64) -> RedisValueRef {
    let expiry = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64)
        .saturating_add(ttl);
    match value {
        RedisValueRef::String(s) => {
            let mut db = db.write().await;
            db.dict.insert(key.to_string(), s);
            db.ttl.insert(key.to_string(), expiry);
            RedisValueRef::SimpleString(Bytes::from("OK"))
        }
        _ => RedisValueRef::Error(Bytes::from("ERR value must be a string")),
    }
}

async fn get(db: &Db, key: RedisValueRef) -> RedisValueRef {
    let key_string = key.to_string();
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
        Some(value) => RedisValueRef::String(value.clone()),
        None => RedisValueRef::NullBulkString,
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = Arc::new(RwLock::new(RedisDb {
        dict: HashMap::new(),
        ttl: HashMap::new(),
    }));

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
        Arc::new(RwLock::new(RedisDb {
            dict: HashMap::new(),
            ttl: HashMap::new(),
        }))
    }

    #[tokio::test]
    async fn test_set_get() {
        let db = setup();
        let key = RedisValueRef::String(Bytes::from("key"));
        let value = RedisValueRef::String(Bytes::from("value"));

        let result = set(&db, key.clone(), value.clone()).await;
        assert_eq!(result, RedisValueRef::SimpleString(Bytes::from("OK")));

        let result = get(&db, key).await;
        assert_eq!(result, RedisValueRef::String(Bytes::from("value")));
    }

    #[tokio::test]
    async fn test_get_set_expired() {
        let db = setup();
        let key = RedisValueRef::String(Bytes::from("key"));
        let value = RedisValueRef::String(Bytes::from("value"));
        let result = set_ex(&db, key.clone(), value.clone(), 1).await;
        assert_eq!(result, RedisValueRef::SimpleString(Bytes::from("OK")));
        tokio::time::sleep(Duration::from_millis(10)).await;
        let result = get(&db, key).await;
        assert_eq!(result, RedisValueRef::NullBulkString);
    }

    #[tokio::test]
    async fn test_get_set_not_expired() {
        let db = setup();
        let key = RedisValueRef::String(Bytes::from("key"));
        let value = RedisValueRef::String(Bytes::from("value"));
        let result = set_ex(&db, key.clone(), value.clone(), 1000000).await;
        assert_eq!(result, RedisValueRef::SimpleString(Bytes::from("OK")));

        let result = get(&db, key).await;
        assert_eq!(result, RedisValueRef::String(Bytes::from("value")));
    }
}
