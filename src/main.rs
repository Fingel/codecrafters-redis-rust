use crate::interpreter::{RedisCommand, RedisInterpreter};
use crate::parser::{RedisValueRef, RespParser};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Decoder;

pub mod interpreter;
pub mod parser;

async fn process(stream: TcpStream) {
    tokio::spawn(async move {
        let mut transport = RespParser.framed(stream);
        let interpreter = RedisInterpreter::new();
        while let Some(redis_value) = transport.next().await {
            match redis_value {
                Ok(value) => match interpreter.interpret(value) {
                    Ok(RedisCommand::Ping) => {
                        let resp = RedisValueRef::String(Bytes::from("PONG"));
                        transport.send(resp).await.unwrap();
                    }
                    Ok(RedisCommand::Echo(arg)) => {
                        transport.send(arg).await.unwrap();
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

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((stream, _)) => {
                println!("Accepted new connection");
                process(stream).await;
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
                break;
            }
        }
    }
}
