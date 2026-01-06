use crate::{
    Db,
    interpreter::RedisCommand,
    parser::{RInt, RSimpleString, RedisValueRef, RespParser},
};
use base64::prelude::*;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

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
    transport: &mut Framed<TcpStream, RespParser>,
    listen_port: u16,
) -> Result<(), ReplicationError> {
    // Start handshake - send PING and expect PONG
    transport
        .send(RedisCommand::Ping.try_into().unwrap())
        .await?;
    let resp = get_next_response(transport).await?;

    if &resp.as_string().unwrap_or_default() != "PONG" {
        return Err(ReplicationError::HandshakeFailed(
            "Didn't get a pong response".into(),
        ));
    }

    // Send REPLCONF listening-port and expect OK
    transport
        .send(
            RedisCommand::ReplConf("listening-port".to_string(), listen_port.to_string())
                .try_into()
                .unwrap(),
        )
        .await?;

    let resp = get_next_response(transport).await?;

    if &resp.as_string().unwrap_or_default() != "OK" {
        return Err(ReplicationError::HandshakeFailed(
            "Didn't get a OK response for replconf port".into(),
        ));
    }

    // Send REPLCONF capa and expect OK
    transport
        .send(
            RedisCommand::ReplConf("capa".to_string(), "psync2".to_string())
                .try_into()
                .unwrap(),
        )
        .await?;

    let resp = get_next_response(transport).await?;

    if &resp.as_string().unwrap_or_default() != "OK" {
        return Err(ReplicationError::HandshakeFailed(
            "Didn't get a OK response for replconf capa".into(),
        ));
    }

    // Send PSYNC command
    transport
        .send(RedisCommand::Psync("?".to_string(), -1).try_into().unwrap())
        .await?;

    let resp = get_next_response(transport).await?;

    if !String::from_utf8_lossy(&resp.as_string().unwrap_or_default()).contains("FULLRESYNC") {
        return Err(ReplicationError::HandshakeFailed(
            "Didn't get a FULLRESYNC response for replconf capa".into(),
        ));
    }

    Ok(())
}

pub async fn replconf_resp(key: String, value: String) -> RedisValueRef {
    if key == "GETACK" {
        // Value is bytes offset
        RedisCommand::ReplConf("ACK".to_string(), value)
            .try_into()
            .unwrap()
    } else {
        RSimpleString("OK")
    }
}

pub async fn psync_resp(db: &Db, _id: String, _offset: i64) -> RedisValueRef {
    // On handshake, id will be ? and offset will be -1
    let repl_id = db.replication_id.clone();
    let repl_offset = db.replication_offset;
    let empty_file_bytes = b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
    let empty_file = BASE64_STANDARD.decode(empty_file_bytes).unwrap();
    RedisValueRef::MultiValue(vec![
        RSimpleString(format!("FULLRESYNC {} {}", repl_id, repl_offset)),
        RedisValueRef::RDBFile(Bytes::from(empty_file)),
    ])
}

pub async fn set_rdb_payload(_db: &Db, payload: Bytes) -> RedisValueRef {
    // Todo - actually parse this
    println!("Got request to set RDB payload with len {}", payload.len());
    RSimpleString("OK")
}

pub fn command_bytes(command: RedisCommand) -> usize {
    // TODO: this is highly inefficient, we are writing to a buffer just to count how large it is.
    // We should be able to compute length from the command itself.
    let r_ref: RedisValueRef = match command.try_into() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error Converting to RedisValueRef, counting 0 bytes: {}", e);
            return 0;
        }
    };
    compute_redis_value_size(&r_ref)
}

fn compute_redis_value_size(item: &RedisValueRef) -> usize {
    match item {
        RedisValueRef::Error(e) => {
            1 + e.len() + 2 // "-" + error + "\r\n"
        }
        RedisValueRef::String(s) => {
            let len_str = s.len().to_string();
            1 + len_str.len() + 2 + s.len() + 2 // "$" + len + "\r\n" + data + "\r\n"
        }
        RedisValueRef::SimpleString(s) => {
            1 + s.len() + 2 // "+" + string + "\r\n"
        }
        RedisValueRef::Array(array) => {
            let len_str = array.len().to_string();
            let header_size = 1 + len_str.len() + 2; // "*" + len + "\r\n"
            let elements_size: usize = array.iter().map(compute_redis_value_size).sum();
            header_size + elements_size
        }
        RedisValueRef::Int(i) => {
            let int_str = i.to_string();
            1 + int_str.len() + 2 // ":" + number + "\r\n"
        }
        RedisValueRef::NullArray => crate::parser::NULL_ARRAY.len(),
        RedisValueRef::NullBulkString => crate::parser::NULL_BULK_STRING.len(),
        RedisValueRef::RDBFile(file) => {
            let len_str = file.len().to_string();
            1 + len_str.len() + 2 + file.len() // "$" + len + "\r\n" + data
        }
        RedisValueRef::MultiValue(values) => values.iter().map(compute_redis_value_size).sum(),
        RedisValueRef::ErrorMsg(_) => 0,
    }
}

pub async fn wait(_db: &Db, _replicas: u64, _timeout: u64) -> RedisValueRef {
    RInt(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_redis_value_size() {
        assert_eq!(
            compute_redis_value_size(&RedisValueRef::String(Bytes::from("hello"))),
            11
        );
        assert_eq!(
            compute_redis_value_size(&RedisValueRef::SimpleString(Bytes::from("hello"))),
            8
        );
        assert_eq!(compute_redis_value_size(&RedisValueRef::Int(42)), 5);
        assert_eq!(
            compute_redis_value_size(&RedisValueRef::Array(vec![
                // 1 + 1 + 2
                RedisValueRef::String(Bytes::from("hello")), // 11
                RedisValueRef::Int(42)                       // 5
            ])),
            20
        );
        assert_eq!(compute_redis_value_size(&RedisValueRef::NullArray), 5);
        assert_eq!(compute_redis_value_size(&RedisValueRef::NullBulkString), 5);
    }
}
