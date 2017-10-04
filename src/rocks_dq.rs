use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use core::ops::Range;
use rocksdb::{DB, IteratorMode, Direction};
use std::io::Cursor;
use std::path::Path;

use DurableQueue;

pub struct RocksDQ {
    db: DB,
    bounds: Range<u64>,
}

impl RocksDQ {
    pub fn new<P: AsRef<Path>>(path: P) -> RocksDQ {
        let db = DB::open_default(path).expect("open rocksdb");
        let bounds = {
            db.iterator(IteratorMode::Start).next().map(|(k, _)| {
                let lo = RocksDQ::deserialize_key(k.into_vec());
                let hi = RocksDQ::deserialize_key(
                    db.iterator(IteratorMode::End).next().unwrap().0.into_vec(),
                );
                lo..hi + 1
            })
        }.unwrap_or(0..0);
        RocksDQ { db, bounds }
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
}

impl DurableQueue for RocksDQ {
    fn list(&self) {
        for (key, value) in self.db.iterator(IteratorMode::Start) {
            let k = Cursor::new(key).read_u64::<BigEndian>().expect(
                "parse u64 from rocksdb key",
            );
            let v =
                String::from_utf8(value.into_vec()).expect("parse utf8 string from rocksdb value");
            println!("{}: {}", k, v);
        }
    }
    fn push(&mut self, value: String) {
        let k = RocksDQ::serialize_key(self.bounds.end);
        let v = value.into_bytes();
        self.db.put(&k, &v).expect("writing data to rocksdb");
        self.bounds.end += 1;
    }

    fn peek(&self) -> Option<(u64, String)> {
        if self.bounds.start >= self.bounds.end {
            None
        } else {
            let key = self.bounds.start;
            let value = {
                let k = RocksDQ::serialize_key(self.bounds.start);
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

    fn delete(&self, key: u64) {
        self.db.delete(&RocksDQ::serialize_key(key)).expect(
            "delete data from rocksdb",
        );
    }
}
