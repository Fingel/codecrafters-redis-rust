use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_stream::StreamMap;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::codec::Framed;

use crate::{
    Db,
    interpreter::RedisCommand,
    parser::{RArray, RError, RInt, RString, RedisValueRef, RespParser},
};

type Subscriptions = StreamMap<String, BroadcastStream<RedisValueRef>>;

pub async fn subscription_loop(
    db: &Db,
    transport: &mut Framed<TcpStream, RespParser>,
    channel: String,
) {
    let mut subscriptions: Subscriptions = StreamMap::new();
    let resp = subscribe(db, channel, &mut subscriptions).await;
    transport.send(resp).await.unwrap();

    loop {
        tokio::select! {
            Some((channel, result)) = subscriptions.next() => {
                match result {
                    Ok(message) => {
                        transport.send(RArray(vec![
                            RString("message"),
                            RString(channel),
                            message
                        ])).await.unwrap();
                    }
                    Err(err) => {
                        transport.send(RString(format!("Error: {}", err))).await.unwrap();
                    }
                }
            }
            // gather all subscriptions and wait for the next message from any of them
            Some(result) = transport.next() => {
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
                            Ok(RedisCommand::Ping) => ping(),
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
    }
}

pub async fn subscribe(
    db: &Db,
    channel: String,
    subscriptions: &mut Subscriptions,
) -> RedisValueRef {
    if subscriptions.contains_key(&channel) {
        return RArray(vec![
            RString("subscribe"),
            RString(&channel),
            RInt(subscriptions.len() as i64),
        ]);
    }

    // TODO: make a db.subscribe(channel) method
    let receiver = {
        let mut pubsub = db.pubsub.lock().unwrap();
        if let Some(tx) = pubsub.get(&channel) {
            tx.subscribe()
        } else {
            let (tx, rx) = tokio::sync::broadcast::channel::<RedisValueRef>(1024);
            pubsub.insert(channel.clone(), tx);
            rx
        }
    };
    let stream = BroadcastStream::new(receiver);
    subscriptions.insert(channel.clone(), stream);

    RArray(vec![
        RString("subscribe"),
        RString(channel),
        RInt(subscriptions.len() as i64),
    ])
}

pub async fn unsubscribe(
    _db: &Db,
    channel: String,
    subscriptions: &mut Subscriptions,
) -> RedisValueRef {
    if subscriptions.contains_key(&channel) {
        subscriptions.remove(&channel);
    }
    RArray(vec![
        RString("unsubscribe"),
        RString(channel),
        RInt(subscriptions.len() as i64),
    ])
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

pub fn ping() -> RedisValueRef {
    RArray(vec![RString("pong"), RString("")])
}

pub async fn publish(db: &Db, channel: String, message: String) -> RedisValueRef {
    let guard = db.pubsub.lock().unwrap();
    if let Some(sender) = guard.get(&channel) {
        let _ = sender.send(RString(message));
        let cnt = sender.receiver_count() as i64;
        RInt(cnt)
    } else {
        RInt(0)
    }
}
