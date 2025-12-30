use std::sync::Arc;

use bytes::Bytes;
use codecrafters_redis::{Db, RedisDb, echo, get, lpush, lrange, ping, rpush, set, set_ex};
use codecrafters_redis::{
    interpreter::{RedisCommand, RedisInterpreter},
    parser::{RedisValueRef, RespParser},
};
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio_util::codec::Decoder;

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
        RedisCommand::Lpush(key, value) => lpush(db, key, value).await,
        RedisCommand::Lrange(key, start, stop) => lrange(db, key, start, stop).await,
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
    use codecrafters_redis::RedisValue;

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
