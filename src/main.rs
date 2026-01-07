use std::sync::Arc;

use codecrafters_redis::parser::{RInt, RedisValueRef};
use codecrafters_redis::replication::psync_preamble;
use codecrafters_redis::{Db, RedisDb, Replica, handle_command, replication};
use codecrafters_redis::{
    interpreter::RedisCommand,
    parser::{RArray, RError, RSimpleString, RespParser},
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
                                let resp = RError("ERR MULTI calls can not be nested");
                                transport.send(resp).await.unwrap();
                            } else {
                                in_transaction = true;
                                queued_commands.clear();
                                let result = RSimpleString("OK");
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

                                transport.send(RArray(results)).await.unwrap();
                            } else {
                                let resp = RError("ERR EXEC without MULTI");
                                transport.send(resp).await.unwrap();
                            }
                        }
                        RedisCommand::Discard => {
                            if in_transaction {
                                in_transaction = false;
                                queued_commands.clear();
                                transport.send(RSimpleString("OK")).await.unwrap();
                            } else {
                                transport
                                    .send(RError("ERR DISCARD without MULTI"))
                                    .await
                                    .unwrap();
                            }
                        }
                        RedisCommand::ReplConf(_key, _value) => {
                            let command = RSimpleString("OK");
                            transport.send(command).await.unwrap();
                        }
                        RedisCommand::Psync(id_in, offset_in) => {
                            println!("Master - Got replication request");
                            let response = psync_preamble(&db, id_in, offset_in).await;
                            transport.send(response).await.unwrap();
                            let (tx, mut rx) = tokio::sync::mpsc::channel::<RedisCommand>(1024);
                            let replica = Replica {
                                id: uuid::Uuid::new_v4().to_string(),
                                offset: 0,
                                tx,
                            };
                            {
                                let mut replicas = db.replicating_to.lock().unwrap();
                                replicas.push(replica);
                            } // MutexGuard dropped here
                            loop {
                                if let Some(command) = rx.recv().await {
                                    println!("Master - Replicating command: {:?}", command.clone());
                                    if command.can_replicate() {
                                        let r_value: RedisValueRef = match command.try_into() {
                                            Ok(val) => val,
                                            Err(_) => {
                                                eprintln!("Command serialization not implemented");
                                                continue;
                                            }
                                        };
                                        transport.send(r_value).await.unwrap();
                                    } else {
                                        println!(
                                            "Master - Skipping non-replicable command: {:?}",
                                            command
                                        );
                                    }
                                }
                            }
                        }
                        RedisCommand::Wait(_replicas, _timeout) => {
                            transport
                                .send(RInt(db.connected_replicas() as i64))
                                .await
                                .unwrap();
                        }
                        _ => {
                            if in_transaction {
                                queued_commands.push(command);
                                transport.send(RSimpleString("QUEUED")).await.unwrap();
                            } else {
                                println!("Master - Received command: {:?}", command);
                                let result = handle_command(&db, command.clone()).await;
                                transport.send(result).await.unwrap();
                                {
                                    let replicas: Vec<_> = db
                                        .replicating_to
                                        .lock()
                                        .unwrap()
                                        .iter()
                                        .map(|r| r.tx.clone())
                                        .collect();

                                    for tx in replicas {
                                        tx.send(command.clone()).await.unwrap();
                                    }
                                }
                            }
                        }
                    },
                    Err(err) => {
                        eprintln!("Command interpretation error: {}", err);
                        let resp = RError(format!("ERR {}", err));
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

    // Replication
    if let Some((master_addr, master_port)) = db.replica_of.clone() {
        let db = db.clone();
        tokio::spawn(async move {
            let stream = match TcpStream::connect((master_addr, master_port)).await {
                Ok(stream) => stream,
                Err(_) => {
                    eprintln!("Failed to connect to master");
                    std::process::exit(1);
                }
            };
            let mut transport = RespParser.framed(stream);
            if let Err(e) = replication::handshake(&mut transport, port).await {
                eprintln!("Replication handshake failed: {}", e);
                std::process::exit(1);
            }
            let mut recieved_offset: usize = 0;
            while let Some(redis_value) = transport.next().await {
                match redis_value {
                    Ok(value) => {
                        let result: Result<RedisCommand, _> = value.try_into();
                        match result {
                            Ok(command) => {
                                println!("Replica - Received command: {:?}", command);
                                let cmd_for_bytes = command.clone();
                                match command {
                                    RedisCommand::ReplConf(key, value) => {
                                        let command = if key == "GETACK" {
                                            // Value is bytes offset
                                            RedisCommand::ReplConf(
                                                "ACK".to_string(),
                                                recieved_offset.to_string(),
                                            )
                                            .try_into()
                                            .unwrap()
                                        } else {
                                            RSimpleString("OK")
                                        };

                                        transport.send(command).await.unwrap();
                                    }
                                    _ => {
                                        handle_command(&db, command).await;
                                    }
                                }
                                recieved_offset += replication::command_bytes(cmd_for_bytes);
                            }
                            Err(e) => eprintln!("Failed to parse command: {}", e),
                        }
                    }
                    Err(e) => eprintln!("Failed to read command: {:?}", e),
                }
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
