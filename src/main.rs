use std::collections::HashMap;
use std::sync::Arc;

use crate::interpreter::{RedisCommand, RedisInterpreter};
use crate::parser::{RedisValueRef, RespParser};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_util::codec::Decoder;

pub mod interpreter;
pub mod parser;

type Db = Arc<Mutex<HashMap<String, Bytes>>>;

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
            let mut db = db.lock().await;
            db.insert(key.to_string(), s);
            RedisValueRef::SimpleString(Bytes::from("OK"))
        }
        _ => RedisValueRef::Error(Bytes::from("ERR value must be a string")),
    }
}

async fn get(db: &Db, key: RedisValueRef) -> RedisValueRef {
    let db = db.lock().await;
    match db.get(&key.to_string()) {
        Some(value) => RedisValueRef::String(value.clone()),
        None => RedisValueRef::NullBulkString,
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = Arc::new(Mutex::new(HashMap::<String, Bytes>::new()));

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

    #[tokio::test]
    async fn test_set_get() {
        let db = Arc::new(Mutex::new(HashMap::<String, Bytes>::new()));

        let key = RedisValueRef::String(Bytes::from("key"));
        let value = RedisValueRef::String(Bytes::from("value"));

        let result = set(&db, key.clone(), value.clone()).await;
        assert_eq!(result, RedisValueRef::SimpleString(Bytes::from("OK")));

        let result = get(&db, key).await;
        assert_eq!(result, RedisValueRef::String(Bytes::from("value")));
    }
}
