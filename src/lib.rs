extern crate byteorder;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use std::io::Cursor;
use std::path::Path;

extern crate core;
use core::ops::Range;

extern crate rocksdb;
use rocksdb::{DB, IteratorMode, Direction};

pub struct DurableQueue {
    db: DB,
    bounds: Range<u64>,
}

impl DurableQueue {
    pub fn new<P: AsRef<Path>>(path: P) -> DurableQueue {
        let db = DB::open_default(path).expect("open rocksdb");
        let bounds = {
            db.iterator(IteratorMode::Start).next().map(|(k, _)| {
                let lo = DurableQueue::deserialize_key(k.into_vec());
                let hi = DurableQueue::deserialize_key(
                    db.iterator(IteratorMode::End).next().unwrap().0.into_vec(),
                );
                lo..hi + 1
            })
        }.unwrap_or(0..0);
        DurableQueue { db, bounds }
    }

    fn serialize_key(key: u64) -> Vec<u8> {
        let mut k = Vec::new();
        k.write_u64::<BigEndian>(key).expect(
            "write u64 to big-endian bytes",
        );
        k
    }

    fn deserialize_key(k: Vec<u8>) -> u64 {
        Cursor::new(k).read_u64::<BigEndian>().expect(
            "parse u64 from rocksdb key",
        )
    }

    pub fn list(&self) {
        for (key, value) in self.db.iterator(IteratorMode::Start) {
            let k = Cursor::new(key).read_u64::<BigEndian>().expect(
                "parse u64 from rocksdb key",
            );
            let v =
                String::from_utf8(value.into_vec()).expect("parse utf8 string from rocksdb value");
            println!("{}: {}", k, v);
        }
    }
    pub fn push(&mut self, value: String) {
        let k = DurableQueue::serialize_key(self.bounds.end);
        let v = value.into_bytes();
        self.db.put(&k, &v).expect("writing data to rocksdb");
        self.bounds.end += 1;
    }

    pub fn peek(&self) -> Option<(u64, String)> {
        if self.bounds.start >= self.bounds.end {
            None
        } else {
            let key = self.bounds.start;
            let value = {
                let k = DurableQueue::serialize_key(self.bounds.start);
                let v = self.db
                    .iterator(IteratorMode::From(&k, Direction::Forward))
                    .next()
                    .unwrap()
                    .1
                    .into_vec();
                String::from_utf8(v).expect("parse utf8 string from rocksdb value")
            };
            Some((key, value))
        }
    }

    pub fn delete(&self, key: u64) {
        self.db.delete(&DurableQueue::serialize_key(key)).expect(
            "delete data from rocksdb",
        );
    }
}
