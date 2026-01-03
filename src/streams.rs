use std::{collections::BTreeMap, time::SystemTime};

use bytes::Bytes;

use crate::{Db, RedisValue, parser::RedisValueRef, ref_error};

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
        let ms = ms.unwrap_or(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        );
        // Shit special case where seq 0 is only invalid if ms is also 0
        let seq = match seq {
            None => {
                if ms == 0 {
                    1
                } else {
                    0
                }
            }
            Some(seq) => seq,
        };
        Self { ms, seq }
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

fn compute_stream_id(ms: Option<u64>, seq: Option<u64>, last_stream: &StreamId) -> StreamId {
    match ms {
        None => StreamId::new(None, None), // *
        Some(ms) => {
            // 1234546-
            if ms == last_stream.ms {
                StreamId::new(Some(ms), Some(seq.unwrap_or(last_stream.seq + 1))) // Add one to the seq of the same ms
            } else {
                StreamId::new(Some(ms), seq) // new timestamp, use supplied seq or 1
            }
        }
    }
}

pub async fn xadd(
    db: &Db,
    key: Bytes,
    id_tuple: (Option<u64>, Option<u64>),
    fields: Vec<(Bytes, Bytes)>,
) -> RedisValueRef {
    let (ms, seq) = id_tuple;
    if ms == Some(0) && seq == Some(0) {
        return ref_error("ERR The ID specified in XADD must be greater than 0-0");
    }
    let key_string = String::from_utf8_lossy(&key).to_string();
    match db.get_mut_if_valid(&key_string) {
        Some(mut entry) => match &mut *entry {
            RedisValue::Stream(existing_stream) => {
                let stream_id = match existing_stream.0.last_key_value() {
                    Some((last_id, _)) => {
                        let new_id = compute_stream_id(ms, seq, last_id);
                        if &new_id <= last_id {
                            return ref_error(
                                "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                            );
                        } else {
                            new_id
                        }
                    }
                    None => StreamId::new(ms, seq),
                };
                existing_stream.insert(stream_id.clone(), fields);

                RedisValueRef::String(stream_id.to_bytes())
            }
            _ => ref_error("Attempted add to non-stream value"),
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

pub async fn xrange(
    db: &Db,
    key: Bytes,
    start: (u64, Option<u64>),
    stop: (u64, Option<u64>),
) -> RedisValueRef {
    let key_string = String::from_utf8_lossy(&key).to_string();
    let (start_ms, start_seq) = start;
    let (stop_ms, stop_seq) = stop;
    match db.get_if_valid(&key_string) {
        Some(entry) => match &*entry {
            RedisValue::Stream(stream) => {
                let start = StreamId {
                    ms: start_ms,
                    seq: start_seq.unwrap_or(0),
                };
                let stop = StreamId {
                    ms: stop_ms,
                    seq: stop_seq.unwrap_or(u64::MAX),
                };

                let mut result = Vec::new();
                for (id, fields) in stream.0.range(start..=stop) {
                    result.push(RedisValueRef::Array(vec![
                        RedisValueRef::String(id.to_bytes()),
                        RedisValueRef::Array(
                            fields
                                .iter()
                                .flat_map(|(k, v)| {
                                    vec![
                                        RedisValueRef::String(k.clone()),
                                        RedisValueRef::String(v.clone()),
                                    ]
                                })
                                .collect(),
                        ),
                    ]));
                }

                RedisValueRef::Array(result)
            }
            _ => ref_error("Attempted range on non-stream value"),
        },
        None => ref_error("Key does not exist"),
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
    fn test_stream_id_new() {
        let id1 = StreamId::new(None, None);
        assert!(id1.ms > 1000); // jank, this is a new timestamp
        assert_eq!(id1.seq, 0);

        let id2 = StreamId::new(None, Some(1));
        assert!(id2.ms > 1000); // jank, this is a new timestamp
        assert_eq!(id2.seq, 1);

        let id3 = StreamId::new(Some(1), None);
        assert_eq!(id3.ms, 1);
        assert_eq!(id3.seq, 0);

        let id4 = StreamId::new(Some(1), Some(2));
        assert_eq!(id4.ms, 1);
        assert_eq!(id4.seq, 2);

        let id4 = StreamId::new(Some(0), None);
        assert_eq!(id4.ms, 0);
        assert_eq!(id4.seq, 1);
    }

    #[test]
    fn test_compute_stream_full_auto() {
        let last_id = StreamId { ms: 0, seq: 1 };
        let computed_id = compute_stream_id(None, None, &last_id);
        assert!(computed_id.ms > 1000); // jank, this is a new timestamp
        assert_eq!(computed_id.seq, 0);
    }

    #[test]
    fn test_compute_stream_auto_seq() {
        let last_id = StreamId { ms: 0, seq: 1 };
        let computed_id = compute_stream_id(Some(0), None, &last_id);
        assert_eq!(computed_id.ms, 0);
        assert_eq!(computed_id.seq, 2);
    }

    #[test]
    fn test_compute_stream_no_auto() {
        let last_id = StreamId { ms: 0, seq: 1 };
        let computed_id = compute_stream_id(Some(5), Some(5), &last_id);
        assert_eq!(computed_id.ms, 5);
        assert_eq!(computed_id.seq, 5);
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

        let result = xadd(&db, key.clone(), (time, seq), fields.clone()).await;
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

        let result = xadd(&db, key.clone(), (time, seq), fields.clone()).await;
        assert_eq!(result, RedisValueRef::String("1-1".into()));

        let result = xadd(&db, key.clone(), (Some(1), None), fields.clone()).await;
        assert_eq!(result, RedisValueRef::String("1-2".into()));
    }

    #[tokio::test]
    async fn test_xadd_same() {
        let db = setup();
        let key = Bytes::from("test_stream");
        let time = Some(1);
        let seq = Some(1);
        let fields = vec![];

        let result = xadd(&db, key.clone(), (time, seq), fields.clone()).await;
        assert_eq!(result, RedisValueRef::String("1-1".into()));

        let result = xadd(&db, key.clone(), (time, seq), fields.clone()).await;
        assert_eq!(
            result,
            ref_error(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            )
        );
    }

    #[tokio::test]
    async fn test_xadd_less() {
        let db = setup();
        let key = Bytes::from("test_stream");
        let time = Some(2);
        let seq = Some(2);
        let fields = vec![];

        let result = xadd(&db, key.clone(), (time, seq), fields.clone()).await;
        assert_eq!(result, RedisValueRef::String("2-2".into()));

        // less ms
        let result = xadd(&db, key.clone(), (Some(1), Some(3)), fields.clone()).await;
        assert_eq!(
            result,
            ref_error(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            )
        );

        // less seq
        let result = xadd(&db, key.clone(), (Some(2), Some(1)), fields.clone()).await;
        assert_eq!(
            result,
            ref_error(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            )
        );
    }

    #[tokio::test]
    async fn test_xrange() {
        let db = setup();
        let key = Bytes::from("test_stream");
        let entries = vec![
            (
                1,
                Some(0),
                vec![
                    (Bytes::from("1-0-field1"), Bytes::from("1-0value1")),
                    (Bytes::from("1-0-field2"), Bytes::from("1-0value2")),
                ],
            ),
            (
                2,
                Some(0),
                vec![
                    (Bytes::from("2-0-field1"), Bytes::from("2-0value1")),
                    (Bytes::from("2-0-field2"), Bytes::from("2-0value2")),
                ],
            ),
            (
                2,
                Some(2),
                vec![
                    (Bytes::from("2-2-field1"), Bytes::from("2-2value1")),
                    (Bytes::from("2-2-field2"), Bytes::from("2-2value2")),
                ],
            ),
        ];
        for entry in entries {
            xadd(&db, key.clone(), (Some(entry.0), entry.1), entry.2).await;
        }

        let result = xrange(&db, key.clone(), (0, Some(0)), (2, Some(2))).await;
        assert_eq!(
            result,
            RedisValueRef::Array(vec![
                RedisValueRef::Array(vec![
                    RedisValueRef::String("1-0".into()),
                    RedisValueRef::Array(vec![
                        RedisValueRef::String("1-0-field1".into()),
                        RedisValueRef::String("1-0value1".into()),
                        RedisValueRef::String("1-0-field2".into()),
                        RedisValueRef::String("1-0value2".into()),
                    ]),
                ]),
                RedisValueRef::Array(vec![
                    RedisValueRef::String("2-0".into()),
                    RedisValueRef::Array(vec![
                        RedisValueRef::String("2-0-field1".into()),
                        RedisValueRef::String("2-0value1".into()),
                        RedisValueRef::String("2-0-field2".into()),
                        RedisValueRef::String("2-0value2".into()),
                    ]),
                ]),
                RedisValueRef::Array(vec![
                    RedisValueRef::String("2-2".into()),
                    RedisValueRef::Array(vec![
                        RedisValueRef::String("2-2-field1".into()),
                        RedisValueRef::String("2-2value1".into()),
                        RedisValueRef::String("2-2-field2".into()),
                        RedisValueRef::String("2-2value2".into()),
                    ]),
                ]),
            ])
        );

        let result = xrange(&db, key.clone(), (0, Some(0)), (2, Some(1))).await;
        assert_eq!(
            result,
            RedisValueRef::Array(vec![
                RedisValueRef::Array(vec![
                    RedisValueRef::String("1-0".into()),
                    RedisValueRef::Array(vec![
                        RedisValueRef::String("1-0-field1".into()),
                        RedisValueRef::String("1-0value1".into()),
                        RedisValueRef::String("1-0-field2".into()),
                        RedisValueRef::String("1-0value2".into()),
                    ]),
                ]),
                RedisValueRef::Array(vec![
                    RedisValueRef::String("2-0".into()),
                    RedisValueRef::Array(vec![
                        RedisValueRef::String("2-0-field1".into()),
                        RedisValueRef::String("2-0value1".into()),
                        RedisValueRef::String("2-0-field2".into()),
                        RedisValueRef::String("2-0value2".into()),
                    ]),
                ]),
            ])
        );

        let result = xrange(&db, key.clone(), (0, None), (2, None)).await;
        assert_eq!(
            result,
            RedisValueRef::Array(vec![
                RedisValueRef::Array(vec![
                    RedisValueRef::String("1-0".into()),
                    RedisValueRef::Array(vec![
                        RedisValueRef::String("1-0-field1".into()),
                        RedisValueRef::String("1-0value1".into()),
                        RedisValueRef::String("1-0-field2".into()),
                        RedisValueRef::String("1-0value2".into()),
                    ]),
                ]),
                RedisValueRef::Array(vec![
                    RedisValueRef::String("2-0".into()),
                    RedisValueRef::Array(vec![
                        RedisValueRef::String("2-0-field1".into()),
                        RedisValueRef::String("2-0value1".into()),
                        RedisValueRef::String("2-0-field2".into()),
                        RedisValueRef::String("2-0value2".into()),
                    ]),
                ]),
                RedisValueRef::Array(vec![
                    RedisValueRef::String("2-2".into()),
                    RedisValueRef::Array(vec![
                        RedisValueRef::String("2-2-field1".into()),
                        RedisValueRef::String("2-2value1".into()),
                        RedisValueRef::String("2-2-field2".into()),
                        RedisValueRef::String("2-2value2".into()),
                    ]),
                ]),
            ])
        );
    }
}
