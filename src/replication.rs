use crate::parser::{RedisValueRef, RespParser};
use bytes::Bytes;
use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_util::codec::Decoder;

pub async fn handshake(master_addr: String, master_port: u16) {
    let conn = TcpStream::connect((master_addr, master_port))
        .await
        .unwrap();
    let mut transport = RespParser.framed(conn);
    let ping = RedisValueRef::Array(vec![RedisValueRef::String(Bytes::from("PING"))]);
    transport.send(ping).await.unwrap();
}
