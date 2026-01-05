use crate::{
    interpreter::RedisCommand,
    parser::{RedisValueRef, RespParser},
};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Framed};

#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error("Handshake failed: {0}")]
    HandshakeFailed(String),
    #[error("Network error: {0}")]
    IoError(#[from] std::io::Error),
}

async fn get_next_response(
    transport: &mut Framed<TcpStream, RespParser>,
) -> Result<RedisValueRef, ReplicationError> {
    transport
        .next()
        .await
        .ok_or_else(|| ReplicationError::HandshakeFailed("Connection closed by master".into()))?
        .map_err(|_| {
            ReplicationError::HandshakeFailed("Failed to parse response from master".into())
        })
}

pub async fn handshake(
    master_addr: String,
    master_port: u16,
    listen_port: u16,
) -> Result<(), ReplicationError> {
    let conn = TcpStream::connect((master_addr, master_port))
        .await
        .map_err(|_| ReplicationError::HandshakeFailed("Failed to connect to master".into()))?;
    let mut transport = RespParser.framed(conn);
    transport
        .send(RedisCommand::Ping.try_into().unwrap())
        .await?;
    let resp = get_next_response(&mut transport).await?;

    if &resp.as_string().unwrap_or_default() != "PONG" {
        return Err(ReplicationError::HandshakeFailed(
            "Didn't get a pong response".into(),
        ));
    }

    let replconf = RedisValueRef::Array(vec![
        RedisValueRef::String(Bytes::from("REPLCONF")),
        RedisValueRef::String(Bytes::from("listening-port")),
        RedisValueRef::String(Bytes::from(listen_port.to_string())),
    ]);
    transport.send(replconf).await?;
    Ok(())
}
