use std::collections::HashMap;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast::Receiver;
use tokio_util::codec::Framed;

use crate::{
    Db,
    interpreter::RedisCommand,
    parser::{RArray, RError, RInt, RString, RedisValueRef, RespParser},
};

type Subscriptions = HashMap<String, Receiver<RedisValueRef>>;

pub async fn subscription_loop(
    db: &Db,
    transport: &mut Framed<TcpStream, RespParser>,
    channel: String,
) {
    let mut subscriptions: Subscriptions = HashMap::new();
    let resp = subscribe(db, channel, &mut subscriptions).await;
    transport.send(resp).await.unwrap();
    loop {
        let result = transport.next().await.unwrap();
        match result {
            Ok(value) => {
                let command: Result<RedisCommand, _> = value.try_into();
                let resp = match command {
                    Ok(RedisCommand::Subscribe(channel)) => {
                        subscribe(db, channel, &mut subscriptions).await
                    }
                    Ok(RedisCommand::Unsubscribe(channel)) => {
                        unsubscribe(db, channel, &mut subscriptions).await
                    }
                    Ok(RedisCommand::PSubscribe(pattern)) => {
                        punsubscribe(db, pattern, &mut subscriptions).await
                    }
                    Ok(RedisCommand::PUnsubscribe(pattern)) => {
                        punsubscribe(db, pattern, &mut subscriptions).await
                    }
                    Ok(other_command) => RError(format!(
                        "ERR Can't execute {} in subscribed mode",
                        other_command
                    )),
                    Err(e) => RError(format!(
                        "ERR Can't execute command in subscribe mode: {:?}",
                        e
                    )),
                };
                transport.send(resp).await.unwrap();
            }
            Err(e) => {
                eprintln!("Error reading from transport: {:?}", e);
                break;
            }
        };
    }
}

pub async fn subscribe(
    db: &Db,
    channel: String,
    subscriptions: &mut Subscriptions,
) -> RedisValueRef {
    let sub_cnt = {
        let mut pubsub = db.pubsub.lock().unwrap();
        if let Some(tx) = pubsub.get(&channel) {
            subscriptions.insert(channel.clone(), tx.subscribe());
        } else {
            let (tx, rx) = tokio::sync::broadcast::channel::<RedisValueRef>(1024);
            pubsub.insert(channel.clone(), tx);
            subscriptions.insert(channel.clone(), rx);
        }
        subscriptions.len()
    };
    RArray(vec![
        RString("subscribe"),
        RString(channel),
        RInt(sub_cnt as i64),
    ])
}

pub async fn unsubscribe(
    _db: &Db,
    _channel: String,
    _subscriptions: &mut Subscriptions,
) -> RedisValueRef {
    RString("OK")
}

pub async fn psubscribe(
    _db: &Db,
    _pattern: String,
    _subscriptions: &mut Subscriptions,
) -> RedisValueRef {
    RString("OK")
}

pub async fn punsubscribe(
    _db: &Db,
    _pattern: String,
    _subscriptions: &mut Subscriptions,
) -> RedisValueRef {
    RString("OK")
}
