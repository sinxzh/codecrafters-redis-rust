use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct KvItem {
    pub val: String,
    expire_at: Option<Instant>,
}

impl KvItem {
    pub fn new(val: String, expire_mills: Option<u64>) -> KvItem {
        let expire_at = expire_mills.map(|mills| Instant::now() + Duration::from_millis(mills));
        KvItem { val, expire_at }
    }

    pub fn expire_after(&mut self, mills: u64) {
        self.expire_at = Some(Instant::now() + Duration::from_millis(mills));
    }
}

pub struct KvStore {
    items: Arc<RwLock<HashMap<String, KvItem>>>,
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            items: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn clone(kv_store: &KvStore) -> KvStore {
        KvStore {
            items: Arc::clone(&kv_store.items),
        }
    }

    pub fn insert(&mut self, key: String, val: KvItem) {
        println!("set key: {}, val: {:?}", key, val);
        self.items.write().unwrap().insert(key, val);
    }

    pub fn get_clone(&self, key: &str) -> Option<KvItem> {
        let items_guard = self.items.read().unwrap();
        if let Some(val) = items_guard.get(key) {
            println!("get key: {}, val: {:?}", key, val);
            if let Some(exp) = val.expire_at {
                if exp > Instant::now() {
                    return Some(val.clone());
                }
            } else {
                return Some(val.clone());
            }
        }
        None
    }

    pub fn do_action<F>(&mut self, key: &str, action_cb: F) -> bool
    where
        F: FnOnce(&str, Option<&mut KvItem>),
    {
        let mut items_guard = self.items.write().unwrap();
        if let Some(val) = items_guard.get_mut(key) {
            if let Some(exp) = val.expire_at {
                if exp > Instant::now() {
                    action_cb(key, Some(val));
                    return true;
                }
            }
        } else {
            action_cb(key, None);
            return true;
        }

        return false;
    }
}
