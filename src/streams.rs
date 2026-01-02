use bytes::Bytes;

use crate::{Db, RedisValue, Stream, parser::RedisValueRef};

pub async fn xadd(db: &Db, key: Bytes, id: Bytes, fields: Vec<(Bytes, Bytes)>) -> RedisValueRef {
    let key_string = String::from_utf8_lossy(&key).to_string();
    let stream = Stream {
        id: id.clone(),
        fields,
    };
    match db.get_mut_if_valid(&key_string) {
        Some(mut entry) => match &mut *entry {
            RedisValue::Stream(existing_stream) => {
                existing_stream.push(stream);
            }
            _ => {
                return RedisValueRef::Error(Bytes::from(
                    "Attempted to push to an array of the wrong type",
                ));
            }
        },
        None => {
            db.dict
                .insert(key_string.clone(), RedisValue::Stream(vec![stream]));
        }
    };

    RedisValueRef::String(id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::RedisDb;

    use super::*;

    fn setup() -> Arc<RedisDb> {
        Arc::new(RedisDb::new())
    }

    #[tokio::test]
    async fn test_xadd() {
        let db = setup();
        let key = Bytes::from("test_stream");
        let id = Bytes::from("1");
        let fields = vec![
            (Bytes::from("field1"), Bytes::from("value1")),
            (Bytes::from("field2"), Bytes::from("value2")),
        ];

        let result = xadd(&db, key.clone(), id.clone(), fields.clone()).await;
        assert_eq!(result, RedisValueRef::String(id.clone()));

        let stream = db.get_if_valid(&String::from_utf8_lossy(&key)).unwrap();
        assert_eq!(*stream, RedisValue::Stream(vec![Stream { id, fields }]));
    }
}
