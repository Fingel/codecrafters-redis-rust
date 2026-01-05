use std::sync::Arc;

use bytes::Bytes;
use codecrafters_redis::{
    _type, Db, RedisDb, echo, get, incr, info, lists, ping, replication, set, set_ex, streams,
};
use codecrafters_redis::{
    interpreter::RedisCommand,
    parser::{RedisValueRef, RespParser},
};
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Decoder;

async fn process(stream: TcpStream, db: Db) {
    tokio::spawn(async move {
        let mut transport = RespParser.framed(stream);
        let mut in_transaction = false;
        let mut queued_commands: Vec<RedisCommand> = Vec::new();
        while let Some(redis_value) = transport.next().await {
            match redis_value {
                Ok(value) => match value.try_into() {
                    Ok(command) => match command {
                        RedisCommand::Multi => {
                            if in_transaction {
                                let resp = RedisValueRef::Error(Bytes::from(
                                    "ERR MULTI calls can not be nested",
                                ));
                                transport.send(resp).await.unwrap();
                            } else {
                                in_transaction = true;
                                queued_commands.clear();
                                let result = RedisValueRef::SimpleString(Bytes::from("OK"));
                                transport.send(result).await.unwrap();
                            }
                        }
                        RedisCommand::Exec => {
                            if in_transaction {
                                in_transaction = false;
                                let mut results = Vec::new();
                                for cmd in queued_commands.drain(..) {
                                    let result = handle_command(&db, cmd).await;
                                    results.push(result);
                                }

                                transport.send(RedisValueRef::Array(results)).await.unwrap();
                            } else {
                                let resp =
                                    RedisValueRef::Error(Bytes::from("ERR EXEC without MULTI"));
                                transport.send(resp).await.unwrap();
                            }
                        }
                        RedisCommand::Discard => {
                            if in_transaction {
                                in_transaction = false;
                                queued_commands.clear();
                                transport
                                    .send(RedisValueRef::SimpleString(Bytes::from("OK")))
                                    .await
                                    .unwrap();
                            } else {
                                transport
                                    .send(RedisValueRef::Error(Bytes::from(
                                        "ERR DISCARD without MULTI",
                                    )))
                                    .await
                                    .unwrap();
                            }
                        }
                        _ => {
                            if in_transaction {
                                queued_commands.push(command);
                                transport
                                    .send(RedisValueRef::SimpleString(Bytes::from("QUEUED")))
                                    .await
                                    .unwrap();
                            } else {
                                let result = handle_command(&db, command).await;
                                transport.send(result).await.unwrap();
                            }
                        }
                    },
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
        RedisCommand::Rpush(key, value) => lists::rpush(db, key, value).await,
        RedisCommand::Lpush(key, value) => lists::lpush(db, key, value).await,
        RedisCommand::Lrange(key, start, stop) => lists::lrange(db, key, start, stop).await,
        RedisCommand::LLen(key) => lists::llen(db, key).await,
        RedisCommand::LPop(key, num_elements) => lists::lpop(db, key, num_elements).await,
        RedisCommand::BLPop(key, timeout) => lists::blpop(db, key, timeout).await,
        RedisCommand::Type(key) => _type(db, key).await,
        RedisCommand::XAdd(key, id_tuple, fields) => streams::xadd(db, key, id_tuple, fields).await,
        RedisCommand::XRange(key, start, stop) => streams::xrange(db, key, start, stop).await,
        RedisCommand::XRead(streams, timeout) => match timeout {
            Some(timeout) => streams::xread_block(db, streams, timeout).await,
            None => streams::xread(db, streams).await,
        },
        RedisCommand::Incr(key) => incr(db, key).await,
        RedisCommand::Multi => unreachable!(),
        RedisCommand::Exec => unreachable!(),
        RedisCommand::Discard => unreachable!(),
        RedisCommand::Info(section) => info(db, section).await,
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let port = if let Some(ports_pos) = args.iter().position(|arg| arg == "--port") {
        args[ports_pos + 1].parse().unwrap_or(6379)
    } else {
        6379
    };
    let replica_of = if let Some(replica_of_pos) = args.iter().position(|arg| arg == "--replicaof")
    {
        let arg = args[replica_of_pos + 1].clone();
        let split: Vec<&str> = arg.split(" ").collect();
        if split.len() == 2 {
            Some((split[0].to_string(), split[1].parse().unwrap_or(6379)))
        } else {
            None
        }
    } else {
        None
    };
    println!("Starting server on port {}", port);
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let db = Arc::new(RedisDb::new(replica_of));

    if let Some((master_addr, master_port)) = db.replica_of.clone() {
        tokio::spawn(async move {
            if let Err(e) = replication::handshake(master_addr, master_port, port).await {
                eprintln!("Replication handshake failed: {}", e);
                std::process::exit(1);
            }
        });
    }

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
