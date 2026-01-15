use std::collections::HashMap;

use ordered_float::NotNan;
use skiplist::OrderedSkipList;

use crate::{
    Db,
    parser::{RArray, RInt, RNull, RString, RedisValueRef},
};

type Score = NotNan<f64>;

#[derive(Debug, PartialEq, Clone)]
struct ListNode(Score, String);

impl PartialOrd for ListNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.0.partial_cmp(&other.0) {
            Some(std::cmp::Ordering::Equal) => self.1.partial_cmp(&other.1),
            other => other,
        }
    }
}

#[derive(Debug)]
pub struct ZSet {
    map: HashMap<String, Score>,
    list: OrderedSkipList<ListNode>,
}

impl ZSet {
    fn new() -> Self {
        ZSet {
            map: HashMap::new(),
            list: OrderedSkipList::new(),
        }
    }

    /// Add a member to the zset, returning the number of elements added
    fn add(&mut self, member: String, score: f64) -> usize {
        let score = Score::new(score).unwrap();
        match self.map.get_mut(&member) {
            Some(existing) => {
                // find and remove item from the skiplist
                let old_member = ListNode(*existing, member.clone());
                self.list.remove(&old_member);
                // update the score in the map
                *existing = score;
                // insert a new value into the skiplist
                let new_member = ListNode(score, member);
                self.list.insert(new_member);
                0
            }
            None => {
                self.map.insert(member.clone(), score);
                self.list.insert(ListNode(score, member));
                1
            }
        }
    }
}

pub fn zadd(db: &Db, set: String, score: f64, member: String) -> RedisValueRef {
    let mut set_guard = db.zsets.lock().unwrap();
    let cnt = match set_guard.get_mut(&set) {
        Some(zset) => zset.add(member, score),
        None => {
            let mut zset = ZSet::new();
            let cnt = zset.add(member, score);
            set_guard.insert(set, zset);
            cnt
        }
    };
    RInt(cnt as i64)
}

pub fn zrank(db: &Db, set: String, member: String) -> RedisValueRef {
    let set_guard = db.zsets.lock().unwrap();
    if let Some(zset) = set_guard.get(&set)
        && let Some(score) = zset.map.get(&member)
        && let Some(rank) = zset.list.index_of(&ListNode(*score, member))
    {
        RInt(rank as i64)
    } else {
        RNull()
    }
}

fn normalize_index(index: i64, len: usize) -> usize {
    if index < 0 {
        (len as i64 + index).max(0) as usize
    } else {
        index as usize
    }
}

pub fn zrange(db: &Db, set: String, start: i64, stop: i64) -> RedisValueRef {
    let set_guard = db.zsets.lock().unwrap();
    match set_guard.get(&set) {
        Some(zset) => {
            let len = zset.list.len();
            let start = normalize_index(start, len);
            let stop = normalize_index(stop, len);
            let start = start.max(0);
            let stop = stop.min(len - 1);
            let range = zset
                .list
                .index_range(start..stop + 1)
                .map(|node| RString(node.1.clone()))
                .collect();
            RArray(range)
        }
        None => RArray(Vec::new()),
    }
}

pub fn zcard(db: &Db, set: String) -> RedisValueRef {
    let set_guard = db.zsets.lock().unwrap();
    match set_guard.get(&set) {
        Some(zset) => RInt(zset.list.len() as i64),
        None => RInt(0),
    }
}

pub fn zscore(db: &Db, set: String, member: String) -> RedisValueRef {
    let set_guard = db.zsets.lock().unwrap();
    if let Some(zset) = set_guard.get(&set)
        && let Some(entry) = zset.map.get(&member)
    {
        RString(entry.to_string())
    } else {
        RNull()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::RedisDb;

    use super::*;

    fn setup() -> Arc<RedisDb> {
        Arc::new(RedisDb::new(None, "/tmp/redis-files", "dump.rdb"))
    }

    #[test]
    fn test_zadd() {
        let db = setup();
        let cnt = zadd(&db, "test_set".to_string(), 1.0, "member1".to_string());
        assert_eq!(cnt, RInt(1));
        // Same
        let cnt = zadd(&db, "test_set".to_string(), 1.0, "member1".to_string());
        assert_eq!(cnt, RInt(0));
    }

    #[test]
    fn test_zadd_update() {
        let db = setup();
        let _ = zadd(&db, "test_set".to_string(), 1.0, "member1".to_string());
        let node = db.zsets.lock().unwrap().get("test_set").unwrap().list[0].clone();
        assert_eq!(node.0, 1.0);
        assert_eq!(node.1, "member1");
        let _ = zadd(&db, "test_set".to_string(), 2.0, "member1".to_string());
        let node = db.zsets.lock().unwrap().get("test_set").unwrap().list[0].clone();
        assert_eq!(node.0, 2.0);
        assert_eq!(node.1, "member1");
        // Same
    }

    #[test]
    fn test_zrank() {
        let db = setup();
        let _ = zadd(&db, "test_set".to_string(), 1.0, "member1".to_string());
        let _ = zadd(&db, "test_set".to_string(), 2.0, "member3".to_string());
        // out of lexigraphical order
        let _ = zadd(&db, "test_set".to_string(), 2.0, "member2".to_string());

        let rank = zrank(&db, "test_set".to_string(), "member1".to_string());
        assert_eq!(rank, RInt(0));
        let rank = zrank(&db, "test_set".to_string(), "member2".to_string());
        assert_eq!(rank, RInt(1));
        let rank = zrank(&db, "test_set".to_string(), "member3".to_string());
        assert_eq!(rank, RInt(2));
    }

    #[test]
    fn test_zrange() {
        let db = setup();
        let _ = zadd(&db, "test_set".to_string(), 1.0, "member1".to_string());
        let _ = zadd(&db, "test_set".to_string(), 2.0, "member2".to_string());
        let _ = zadd(&db, "test_set".to_string(), 3.0, "member3".to_string());
        let _ = zadd(&db, "test_set".to_string(), 4.0, "member4".to_string());

        let range = zrange(&db, "test_set".to_string(), 0, 2);
        assert_eq!(
            range,
            RArray(vec![
                RString("member1"),
                RString("member2"),
                RString("member3")
            ])
        );

        let range = zrange(&db, "test_set".to_string(), 0, 20);
        assert_eq!(
            range,
            RArray(vec![
                RString("member1"),
                RString("member2"),
                RString("member3"),
                RString("member4")
            ])
        );

        let range = zrange(&db, "test_set".to_string(), 0, 3);
        assert_eq!(
            range,
            RArray(vec![
                RString("member1"),
                RString("member2"),
                RString("member3"),
                RString("member4")
            ])
        );

        let range = zrange(&db, "test_set".to_string(), 0, 4);
        assert_eq!(
            range,
            RArray(vec![
                RString("member1"),
                RString("member2"),
                RString("member3"),
                RString("member4")
            ])
        );

        let range = zrange(&db, "test_set".to_string(), 4, 0);
        assert_eq!(range, RArray(vec![]));

        let range = zrange(&db, "test_set".to_string(), 40, 50);
        assert_eq!(range, RArray(vec![]));
    }

    #[test]
    fn test_zrange_negative() {
        let db = setup();
        let _ = zadd(&db, "test_set".to_string(), 1.0, "member1".to_string());
        let _ = zadd(&db, "test_set".to_string(), 2.0, "member2".to_string());
        let _ = zadd(&db, "test_set".to_string(), 3.0, "member3".to_string());
        let _ = zadd(&db, "test_set".to_string(), 4.0, "member4".to_string());

        let range = zrange(&db, "test_set".to_string(), 2, -1);
        assert_eq!(range, RArray(vec![RString("member3"), RString("member4")]));

        let range = zrange(&db, "test_set".to_string(), -1, -1);
        assert_eq!(range, RArray(vec![RString("member4")]));

        let range = zrange(&db, "test_set".to_string(), -20, -1);
        assert_eq!(
            range,
            RArray(vec![
                RString("member1"),
                RString("member2"),
                RString("member3"),
                RString("member4")
            ])
        );
    }

    #[test]
    fn test_zcard() {
        let db = setup();
        let _ = zadd(&db, "test_set".to_string(), 1.0, "member1".to_string());
        let _ = zadd(&db, "test_set".to_string(), 2.0, "member2".to_string());

        let card = zcard(&db, "test_set".to_string());
        assert_eq!(card, RInt(2));
    }

    #[test]
    fn test_zscore() {
        let db = setup();
        let score = zscore(&db, "test_set".to_string(), "member1".to_string());
        assert_eq!(score, RNull());

        let _ = zadd(&db, "test_set".to_string(), 1.0, "member1".to_string());
        let _ = zadd(&db, "test_set".to_string(), 2.0, "member2".to_string());

        let score = zscore(&db, "test_set".to_string(), "member1".to_string());
        assert_eq!(score, RString("1"));

        let score = zscore(&db, "test_set".to_string(), "member3".to_string());
        assert_eq!(score, RNull());
    }
}
