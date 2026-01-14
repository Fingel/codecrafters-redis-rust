use std::collections::HashMap;

use ordered_float::NotNan;
use skiplist::SkipMap;

use crate::{
    Db,
    parser::{RInt, RedisValueRef},
};

type Score = NotNan<f64>;

#[derive(Debug)]
pub struct ZSet {
    map: HashMap<String, f64>,
    list: SkipMap<Score, String>,
}

impl ZSet {
    fn new() -> Self {
        ZSet {
            map: HashMap::new(),
            list: SkipMap::new(),
        }
    }

    fn add(&mut self, member: String, score: f64) {
        self.map.insert(member.clone(), score);
        self.list.insert(Score::new(score).unwrap(), member);
    }
}

pub async fn zadd(db: &Db, set: String, score: f64, member: String) -> RedisValueRef {
    let mut set_guard = db.zsets.lock().unwrap();
    match set_guard.get_mut(&set) {
        Some(zset) => {
            zset.add(member, score);
        }
        None => {
            let mut zset = ZSet::new();
            zset.add(member, score);
            set_guard.insert(set.clone(), zset);
        }
    }
    RInt(1)
}
