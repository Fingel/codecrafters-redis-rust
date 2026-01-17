# RRRedis: Really Rusty Redis 

This is an implementation of the Redis server written in Rust
with a few notable properties. This is purely an intellectual
endeavor, not for production use.

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

https://github.com/Fingel/rrredis/blob/7108da4a86cb76c3d6ba311b841a31c8e9fd09e9/src/main.rs#L17
```rust
let mut transport = RespParser.framed(stream);
while let Some(redis_value) = transport.next().await {
    // do something based on redis_value and write/read to the transport.
}
```

The decoder is defined in [parser.rs](src/parser.rs)


It would be interesting to see under which workloads the
multi-threaded implementation actually comes out ahead. For
now I'm writing it this way to get better at writing async
Rust.

## Commands
So far there is support for:
* Ping
* Echo
* Set
* SetEx
* Get
* RPush
* LPush
* LRange
* LLen
* LPop
* BLPop
* Type
* XAdd
* XRange
* XRead
* Incr
* Multi
* Exec
* Discard
