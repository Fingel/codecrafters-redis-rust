use std::{collections::BTreeMap, time::SystemTime};

use bytes::Bytes;

use crate::{Db, RedisValue, parser::RedisValueRef};

#[derive(Debug, Clone, PartialEq)]
pub struct RedisStream(BTreeMap<StreamId, StreamData>);

impl RedisStream {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn insert(&mut self, id: StreamId, data: StreamData) {
        self.0.insert(id, data);
    }

    pub fn get(&self, key: &StreamId) -> Option<&StreamData> {
        self.0.get(key)
    }
}

impl Default for RedisStream {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamId {
    ms: u64,
    seq: u64,
}
impl PartialOrd for StreamId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for StreamId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ms.cmp(&other.ms).then(self.seq.cmp(&other.seq))
    }
}

impl StreamId {
    pub fn new(ms: Option<u64>, seq: Option<u64>) -> Self {
        Self {
            ms: ms.unwrap_or(
                SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            ),
            seq: seq.unwrap_or(1).max(1), // cannot be 0 TODO enforce this at type level
        }
    }
    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(format!("{}-{}", self.ms, self.seq))
    }
}

impl Default for StreamId {
    fn default() -> Self {
        Self::new(None, None)
    }
}

type StreamData = Vec<(Bytes, Bytes)>;

fn compute_stream_id(
    ms: Option<u64>,
    seq: Option<u64>,
    last_stream: Option<(&StreamId, &StreamData)>,
) -> Result<StreamId, String> {
    match last_stream {
        Some((k, _)) => {
            let new_stream_id = match ms {
                None => StreamId::new(None, None), // *
                Some(ms) => {
                    // 1234546-
                    if ms == k.ms {
                        StreamId::new(Some(ms), Some(seq.unwrap_or(k.seq + 1))) // Add one to the seq of the same ms
                    } else {
                        StreamId::new(Some(ms), seq) // new timestamp, use supplied seq or 1
                    }
                }
            };
            if &new_stream_id <= k {
                return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string());
            }
            Ok(new_stream_id)
        }
        None => Ok(StreamId::new(ms, seq)),
    }
}

pub async fn xadd(
    db: &Db,
    key: Bytes,
    ms: Option<u64>,
    seq: Option<u64>,
    fields: Vec<(Bytes, Bytes)>,
) -> RedisValueRef {
    if ms == Some(0) && seq == Some(0) {
        return RedisValueRef::Error(Bytes::from(
            "ERR The ID specified in XADD must be greater than 0-0".to_string(),
        ));
    }
    let key_string = String::from_utf8_lossy(&key).to_string();
    match db.get_mut_if_valid(&key_string) {
        Some(mut entry) => match &mut *entry {
            RedisValue::Stream(existing_stream) => {
                let last_stream_entry = existing_stream.0.last_key_value();
                let stream_id = match compute_stream_id(ms, seq, last_stream_entry) {
                    Ok(id) => id,
                    Err(err) => return RedisValueRef::Error(Bytes::from(err)),
                };
                existing_stream.insert(stream_id.clone(), fields);

                RedisValueRef::String(stream_id.to_bytes())
            }
            _ => RedisValueRef::Error(Bytes::from("Attempted add to non-stream value")),
        },
        None => {
            let mut new_map = RedisStream::new();
            let new_id = StreamId::new(ms, seq);
            new_map.insert(new_id.clone(), fields);
            db.dict.insert(key_string, RedisValue::Stream(new_map));
            RedisValueRef::String(new_id.to_bytes())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::RedisDb;

    use super::*;

    fn setup() -> Arc<RedisDb> {
        Arc::new(RedisDb::new())
    }

    #[test]
    fn test_stream_id_ordering() {
        let id1 = StreamId { ms: 1, seq: 2 };
        let id2 = StreamId { ms: 2, seq: 1 };
        assert!(id1 < id2);

        let id1 = StreamId { ms: 1, seq: 2 };
        let id2 = StreamId { ms: 1, seq: 1 };
        assert!(id1 > id2);

        let id1 = StreamId { ms: 1, seq: 1 };
        let id2 = StreamId { ms: 1, seq: 1 };
        assert!(id1 == id2);
    }

    #[test]
    fn test_compute_stream_full_auto() {
        let last_id = StreamId { ms: 0, seq: 1 };
        let last_stream = Some((&last_id, &vec![]));
        let computed_id = compute_stream_id(None, None, last_stream).unwrap();
        assert!(computed_id.ms > 1000); // jank, this is a new timestamp
        assert_eq!(computed_id.seq, 1);
    }

    #[test]
    fn test_compute_stream_auto_seq() {
        let last_id = StreamId { ms: 0, seq: 1 };
        let last_stream = Some((&last_id, &vec![]));
        let computed_id = compute_stream_id(Some(0), None, last_stream).unwrap();
        assert_eq!(computed_id.ms, 0);
        assert_eq!(computed_id.seq, 2);
    }

    #[test]
    fn test_compute_stream_no_auto() {
        let last_id = StreamId { ms: 0, seq: 1 };
        let last_stream = Some((&last_id, &vec![]));
        let computed_id = compute_stream_id(Some(5), Some(5), last_stream).unwrap();
        assert_eq!(computed_id.ms, 5);
        assert_eq!(computed_id.seq, 5);
    }

    #[test]
    fn test_compute_stream_less_ms() {
        let last_id = StreamId { ms: 1, seq: 1 };
        let last_stream = Some((&last_id, &vec![]));
        assert!(compute_stream_id(Some(0), None, last_stream).is_err());
    }

    #[test]
    fn test_compute_stream_less_seq() {
        let last_id = StreamId { ms: 1, seq: 1 };
        let last_stream = Some((&last_id, &vec![]));
        assert!(compute_stream_id(Some(1), Some(0), last_stream).is_err());
    }

    #[tokio::test]
    async fn test_xadd() {
        let db = setup();
        let key = Bytes::from("test_stream");
        let time = Some(1);
        let seq = Some(1);
        let fields = vec![
            (Bytes::from("field1"), Bytes::from("value1")),
            (Bytes::from("field2"), Bytes::from("value2")),
        ];

        let result = xadd(&db, key.clone(), time, seq, fields.clone()).await;
        assert_eq!(result, RedisValueRef::String("1-1".into()));

        let redis_val = db
            .get_if_valid(&String::from_utf8_lossy(&key))
            .unwrap()
            .clone();
        let stream_id = StreamId { ms: 1, seq: 1 };
        match redis_val {
            RedisValue::Stream(stream) => {
                assert_eq!(stream.get(&stream_id), Some(&fields))
            }
            _ => {
                panic!("Expected RedisValue::Stream");
            }
        }
    }

    #[tokio::test]
    async fn test_xadd_auto_seq() {
        let db = setup();
        let key = Bytes::from("test_stream");
        let time = Some(1);
        let seq = Some(1);
        let fields = vec![];

        let result = xadd(&db, key.clone(), time, seq, fields.clone()).await;
        assert_eq!(result, RedisValueRef::String("1-1".into()));

        let result = xadd(&db, key.clone(), Some(1), None, fields.clone()).await;
        assert_eq!(result, RedisValueRef::String("1-2".into()));
    }

    #[tokio::test]
    async fn test_xadd_same() {
        let db = setup();
        let key = Bytes::from("test_stream");
        let time = Some(1);
        let seq = Some(1);
        let fields = vec![];

        let result = xadd(&db, key.clone(), time, seq, fields.clone()).await;
        assert_eq!(result, RedisValueRef::String("1-1".into()));

        let result = xadd(&db, key.clone(), time, seq, fields.clone()).await;
        assert_eq!(
            result,
            RedisValueRef::Error(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .into()
            )
        );
    }
}
