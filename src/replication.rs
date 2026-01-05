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
    // Setup connection to master
    let conn = TcpStream::connect((master_addr, master_port))
        .await
        .map_err(|_| ReplicationError::HandshakeFailed("Failed to connect to master".into()))?;
    let mut transport = RespParser.framed(conn);

    // Start handshake - send PING and expect PONG
    transport
        .send(RedisCommand::Ping.try_into().unwrap())
        .await?;
    let resp = get_next_response(&mut transport).await?;

    if &resp.as_string().unwrap_or_default() != "PONG" {
        return Err(ReplicationError::HandshakeFailed(
            "Didn't get a pong response".into(),
        ));
    }

    // Send REPLCONF listening-port and expect OK
    transport
        .send(
            RedisCommand::ReplConf(
                Bytes::from("listening-port"),
                Bytes::from(listen_port.to_string()),
            )
            .try_into()
            .unwrap(),
        )
        .await?;

    let resp = get_next_response(&mut transport).await?;

    if &resp.as_string().unwrap_or_default() != "OK" {
        return Err(ReplicationError::HandshakeFailed(
            "Didn't get a OK response for replconf port".into(),
        ));
    }

    // Send REPLCONF capa and expect OK
    transport
        .send(
            RedisCommand::ReplConf(Bytes::from("capa"), Bytes::from("psync2"))
                .try_into()
                .unwrap(),
        )
        .await?;

    let resp = get_next_response(&mut transport).await?;

    if &resp.as_string().unwrap_or_default() != "OK" {
        return Err(ReplicationError::HandshakeFailed(
            "Didn't get a OK response for replconf capa".into(),
        ));
    }

    Ok(())
}

pub async fn replconf_resp(_key: Bytes, _value: Bytes) -> RedisValueRef {
    RedisValueRef::SimpleString("OK".into())
}
