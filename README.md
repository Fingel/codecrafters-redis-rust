# RRRedis: Really Rusty Redis 

This is an implementation of the Redis server written in Rust
with a few notable properties. This is purely a learning
endeavor, not for production use. This implementation passes the Codecrafters.io
Redis challenge integration test suite.

* [Architecture](#architecture)
* [Bookmarks and Patterns](#bookmarks-and-patterns)
  1. [Using tokio_util::codec to decode a TCP stream into frames.](https://github.com/Fingel/rrredis/tree/master#using-tokio_utilcodec-to-decode-a-tcp-stream-into-frames)
  2. [Taking over a connection for a replicaiton loop](https://github.com/Fingel/rrredis/tree/master#taking-over-a-connection-for-a-replicaiton-loop)
  3. [Pubsub pattern using BroadcastStream from tokio_stream](https://github.com/Fingel/rrredis/tree/master#pubsub-pattern-using-broadcaststream-from-tokio_stream)
  4. [Using a DashMap for lock-free concurrent access](https://github.com/Fingel/rrredis/tree/master#using-a-dashmap-for-lock-free-concurrent-access)
  5. [Using Criterion to benchmark changes](https://github.com/Fingel/rrredis/tree/master#using-criterion-to-benchmark-changes)
  6. 

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
This would seem to be generally useful anytime you have a stream of async data, like a tcp connection,
and want to pull "chunks" off the stream to operate on. This is probably the moment when I realized
that Tokio project is much, much more than simply an async runtime. 
```rust
let mut transport = RespParser.framed(stream);
while let Some(redis_value) = transport.next().await {
    // do something based on redis_value and write/read to the transport.
}
```

The decoder is defined in [parser.rs](src/parser.rs)

### Taking over a connection for a replicaiton loop
This was one of the hardest parts to get right - it involved using the `tokio::select!` macro to listen 
on both the main command connection and the replication connection. What I found challenging was 
writing code that is a client to itself and keeping in my mind the context of where the code I was
looking at was running: are we in the replica or the main context? The code is shared between
the two, minus a few critical branching points which made it all the more difficult to reason about.

https://github.com/Fingel/rrredis/blob/f5147d57b99b93905e1d08c9d20e4ed402a98eb9/src/main.rs#L75-L80


### Pubsub pattern using `BroadcastStream` from `tokio_stream`

Implementing pubsub meant waiting on messages from multiple recievers at the same time. Another win
for the `select!` macro. However I had trouble with doing this while at the same time holding a mutable 
reference to the data structure containing the Recievers. This was necessary so pubsub channels could be
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

### Using Criterion to benchmark changes
Especially early on in the project, I wanted to be able to measure the real difference between different
architectural decisions. The two main ones being using DashMap vs a RWLock\<HashMap\> and using a 
VecDeque vs a Vec for some other internal structures.

Turns out when people say benchmarking is hard, they aren't lying. It took me a while to get tests
that actually isolated what I wanted to measure. Once I did, and saw the timings change dramatically 
between implementations, it was all worth it.
https://github.com/Fingel/rrredis/blob/f5147d57b99b93905e1d08c9d20e4ed402a98eb9/benches/list_operations.rs#L34-L43

### Writing a parser for the RDB binary format
This was a good opportunity to try out the [nom](https://github.com/rust-bakery/nom) 
"parser combinator framework". I don't have a lot of experience writing parsers but the ones 
I have written have all been much more procedural than what I ended up with using Nom.

https://github.com/Fingel/rrredis/blob/d5cde6daa70c022be118367cb0006dddcd46b405/src/rdb.rs#L190-L196

### Using a SkipList to implement a scored set (zset)
Supposedly real Redis also uses a skiplist to implement zsets, so I tried that here. I did consider implementing
the skipist myself but decided the more interesting part woukd be the higher order ZSet datastructure and opted
for a library. I might go back and implement it by scratch.
https://github.com/Fingel/rrredis/blob/d5cde6daa70c022be118367cb0006dddcd46b405/src/zset.rs#L25-L30

### Encoding spherical lat/long pairs as scores to store in a scored set
Redis takes a very interesting approach to storing geographical coordinates: it normalizes lat/longs as
large itegers, then interleaves the _binary_ forms of those integers into one large integer, then stores
that in a scored set such that closer objects are scored similarly. I've normally just used something like
PostGIS for this kind of thing without looking under the hood so I don't know if this is actually a novel
approach but I thought it was really cool.

The [geo.rs](src/geo.rs) file contains the implementation of various geo commands.

## Areas for further improvement
If I were interested in making this project production grade, here are the areas I'd want to clean up:

1. [interpreter.rs](src/interpreter.rs) helps with conversion between the Redis wire format `RedisValueRef` and
the friendlier `RedisCommand` enum. This module is full of repetitive boilerplate. I would probably invest
some time in cleaning this up, perhaps by leveraging macros or just re-organizing it completely.
2. The RDB parser in [rdb.rs](src/rdb.rs) still uses some good-ol `match` and `if` branching where
sub-parsers would probably be more idiomatic.
3. The [zset](src/zset.rs) implentation duplicates values between the skiplist and the hashset. A proper
shared datastructure here would probably save some memory, but that's a rabbit hole I had no intention of
going down at the time - especially in Rust.
4. Replication seems to work, but it still feels... fragile? This feels like one of those inherently
hard to get right problems - this is an area where additional rigour would need to be applied.

## Running
If for some reason you want to run this project all you should need is cargo and a rust compiler. This 
was built using stable Rust, no Nightly required.

    cargo run

You can then interact with the server using `redis-cli`: 

    redis-cli PING
    PONG

The following section lists the available commands.

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
