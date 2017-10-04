extern crate byteorder;
extern crate core;
extern crate rocksdb;

pub trait DurableQueue {
    fn list(&self);
    fn push(&mut self, value: String);
    fn peek(&self) -> Option<(u64, String)>;
    fn delete(&self, key: u64);
}

pub mod rocks_dq;
