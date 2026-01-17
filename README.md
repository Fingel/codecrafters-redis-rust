# RRRedis: Really Rusty Redis 

This is an implementation of the Redis server written in Rust
with a few notable properties. This is purely an intellectual
endeavor, not for production use. This implementation passes the Codecrafters.io
Redis challenge integration test suite.

## Architecture
Unlike the official Redis server this implementation is multi-threaded.
Each client command runs on it's own Tokio task. In theory this means
better multi-core utilization in the case of many concurrent commands.
In reality it probably means worse performance due to synchronization
overhead.

To lessen the impact of resource coordination, the main data-store
uses [dashmap](https://github.com/xacrimon/dashmap) which means
that in some cases, multiple commands can read/write to the db
with true parallelism.

## Bookmarks and Patterns
This project required a lot of neat patterns and solutions, some of
which I'll undoubtedly want to reference in the future.

### Using tokio_util::codec to decode a TCP stream into frames.

```rust
let mut transport = RespParser.framed(stream);
while let Some(redis_value) = transport.next().await {
    // do something based on redis_value and write/read to the transport.
}
```

The decoder is defined in [parser.rs](src/parser.rs)

### Taking over a connection for a replicaiton loop
https://github.com/Fingel/rrredis/blob/f5147d57b99b93905e1d08c9d20e4ed402a98eb9/src/main.rs#L75-L80

This tricky bit involves using the `tokio::select!` macro to listen on both the main command connection and the 
replication connection.

### Pubsub pattern using `BroadcastStream` from `tokio_stream`

Implementing pubsub meant waiting on messages from multiple recievers at the same time. Another win
for the `select!` macro. However I had trouble with doing this while at the same time holding a mutable 
reference to the data structure containing the recievers. This was necessary so pubsub channels could be
added and removed at runtime.

One solution I found was to use seperate tokio tasks per channel and use a single `mpsc::channel` to 
coordinate them all for a single connection. This seemed like a lot of boilerplate.

Luckily, there is the [BroadcastStream](https://docs.rs/tokio-stream/latest/tokio_stream/wrappers/struct.BroadcastStream.html) type from tokio_stream that is designed for the very purpose. It made this
pattern feel pretty natural.

https://github.com/Fingel/rrredis/blob/f5147d57b99b93905e1d08c9d20e4ed402a98eb9/src/pubsub.rs#L13

https://github.com/Fingel/rrredis/blob/f5147d57b99b93905e1d08c9d20e4ed402a98eb9/src/pubsub.rs#L24-L39

### Using a DashMap for lock-free concurrent access
Traditional Redis uses a single thread for it's command queue. This RRREdis uses a tokio task per 
connection and all that connection's commands. So in order to share data structures between tasks,
some kind of locking mechanism is required: Mutex, RWLock, etc.

Fighting over locks for the main data store in Redis seems less than ideal. So I decided to try 
out [DashMap](https://github.com/xacrimon/dashmap) (Distributed Hashmap). It still uses locking mechanisms 
internally, but it also distributes it's keyspace so read/write operations on different keys in most
cases can happen concurrently. Very cool. 

This meant I didn't have to reach for Mutex or RWlock for the main GET, SET, etc commands. But the API is a little awkward.

https://github.com/Fingel/rrredis/blob/f5147d57b99b93905e1d08c9d20e4ed402a98eb9/src/lib.rs#L73

https://github.com/Fingel/rrredis/blob/f5147d57b99b93905e1d08c9d20e4ed402a98eb9/src/lib.rs#L276-L281

## Using Criterion to benchmark changes
Especially early on in the project, I wanted to be able to measure the real difference between different
architectural decisions. The two main ones being using DashMap vs a RWLock<HashMap> and using a 
VecDeque vs a Vec for some other internal structures.

Turns out when people say benchmarking is hard, they aren't lying. It took me a while to get tests
that actually isolated what I wanted to measure. Once I did, and saw the timings change dramatically 
between implementations, it was all worth it.

https://github.com/Fingel/rrredis/blob/f5147d57b99b93905e1d08c9d20e4ed402a98eb9/benches/list_operations.rs#L34-L43

## Commands
Ping
Echo
Set
SetEx
Get
Rpush
Lpush
Lrange
LLen
LPop
BLPop
Type
XAdd
XRange
XRead
Incr
Multi
Exec
Discard
Info
ReplConf
Psync
RdbPayload
Wait
Config
Keys
Subscribe
Unsubscribe
PSubscribe
PUnsubscribe
Publish
ZAdd
ZRank
ZRange
ZCard
ZScore
ZRem
GeoAdd
GeoPos
GeoDist
GeoSearch
AclWhoami
AclGetUser
AclSetUser
Auth
