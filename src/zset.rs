use std::collections::HashMap;

use ordered_float::NotNan;
use skiplist::OrderedSkipList;

use crate::{
    Db,
    parser::{RInt, RedisValueRef},
};

type Score = NotNan<f64>;

#[derive(Debug, PartialEq)]
struct ListNode {
    score: Score,
    key: String,
}

impl ListNode {
    fn new(score: f64, key: String) -> Self {
        let score = Score::new(score).unwrap();
        ListNode { score, key }
    }
}

impl PartialOrd for ListNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.score.partial_cmp(&other.score)
    }
}

#[derive(Debug)]
pub struct ZSet {
    map: HashMap<String, f64>,
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
        match self.map.get_mut(&member) {
            Some(existing) => {
                // find and remove item from the skiplist
                let old_member = ListNode::new(*existing, member.clone());
                self.list.remove(&old_member);
                // update the score in the map
                *existing = score;
                // insert a new value into the skiplist
                let new_member = ListNode::new(score, member);
                self.list.insert(new_member);
                0
            }
            None => {
                self.map.insert(member.clone(), score);
                self.list.insert(ListNode::new(score, member));
                1
            }
        }
    }
}

pub async fn zadd(db: &Db, set: String, score: f64, member: String) -> RedisValueRef {
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
