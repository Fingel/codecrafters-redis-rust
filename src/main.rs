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
                            let replica_id = uuid::Uuid::new_v4().to_string();
                            let replica = Replica {
                                id: replica_id.clone(),
                                offset: 0,
                                tx,
                            };
                            {
                                let mut replicas = db.replicating_to.lock().unwrap();
                                replicas.push(replica);
                            }
                            loop {
                                tokio::select! {
                                    Some(command) = rx.recv() => {
                                        println!("Master - Replicating command: {:?}", command.clone());
                                        match command {
                                            RedisCommand::ReplConf(key, value) if key == "GETACK" => {
                                                // Send GETACK to replica
                                                let r_value: RedisValueRef = RedisCommand::ReplConf(key, value)
                                                    .try_into()
                                                    .unwrap();
                                                transport.send(r_value).await.unwrap();
                                            }
                                            _ => {
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
                                                    println!("Master - Skipping non-replicable command: {:?}", command);
                                                }
                                            }
                                        }
                                    }
                                    Some(result) = transport.next() => {
                                        match result {
                                            Ok(value) => {
                                                let result: Result<RedisCommand, _> = value.try_into();
                                                match result {
                                                    Ok(RedisCommand::ReplConf(key, value)) if key == "ACK" => {
                                                        println!("Master - Received ACK from replica: offset {}", value);
                                                        // Handle the ACK here - update replica offset, etc.
                                                        let mut replicas = db.replicating_to.lock().unwrap();
                                                        for replica in replicas.iter_mut() {
                                                            if replica.id == replica_id {
                                                                println!("Master - setting replica with id {} to offset {}", replica.id, value);
                                                                replica.offset = value.parse().unwrap();
                                                            }
                                                        }
                                                    }
                                                    Ok(cmd) => {
                                                        println!("Master - Received unexpected command from replica: {:?}", cmd);
                                                    }
                                                    Err(e) => eprintln!("Failed to parse replica response: {}", e),
                                                }
                                            }
                                            Err(e) => {
                                                eprintln!("Error reading from replica: {:?}", e);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        RedisCommand::Wait(replicas, timeout) => {
                            let last_wait_offset = db
                                .replication_offset
                                .load(std::sync::atomic::Ordering::Relaxed);
                            let command =
                                RedisCommand::ReplConf("GETACK".to_string(), "*".to_string());
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
                            let mut timeout_limit = 0;
                            loop {
                                let cnt = db
                                    .replicating_to
                                    .lock()
                                    .unwrap()
                                    .iter()
                                    .filter(|r| !r.tx.is_closed() && r.offset >= last_wait_offset)
                                    .count();
                                if cnt as u64 >= replicas || timeout_limit >= timeout {
                                    transport.send(RInt(cnt as i64)).await.unwrap();
                                    break;
                                }
                                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                                timeout_limit += 100;
                            }
                        }
                        _ => {
                            if in_transaction {
                                queued_commands.push(command);
                                transport.send(RSimpleString("QUEUED")).await.unwrap();
                            } else {
                                println!("Master - Received command: {:?}", command);
                                if command.can_replicate() {
                                    let command_bytes =
                                        replication::command_bytes(command.clone()) as i64;
                                    db.replication_offset.fetch_add(
                                        command_bytes,
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                }
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
                                    RedisCommand::ReplConf(key, _value) => {
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
