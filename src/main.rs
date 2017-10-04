extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use structopt::StructOpt;

extern crate rocksdb;
use rocksdb::{DB, IteratorMode};

extern crate byteorder;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use std::io::Cursor;

#[derive(StructOpt, Debug)]
#[structopt(name = "durable_queue", about = "Interact with a durable queue.")]
struct Command {
    #[structopt(long = "db-path")]
    db_path: String,
    #[structopt(subcommand)]
    op: Operation,
}

#[derive(StructOpt, Debug)]
enum Operation {
    #[structopt(name = "list")]
    List,
    #[structopt(name = "push")]
    Push { strings: Vec<String> },
    #[structopt(name = "pop")]
    Pop,
}

fn process(v: String) {
    println!("[PROCESSING] {}", v);
    println!(".");
    println!(".");
    println!(".");
    println!("DONE!");
}

fn main() {
    let cmd: Command = Command::from_args();

    let db = DB::open_default(cmd.db_path).expect("open rocksdb");

    match cmd.op {
        Operation::List => {
            let iter = db.iterator(IteratorMode::Start);
            for (key, value) in iter {
                let k = Cursor::new(key).read_u64::<BigEndian>().expect(
                    "parse u64 from rocksdb key",
                );
                let v = String::from_utf8(value.into_vec()).expect(
                    "parse utf8 string from rocksdb value",
                );
                println!("{}: {}", k, v);
            }
        }
        Operation::Push { strings } => {
            let mut idx = {
                let mut iter = db.iterator(IteratorMode::End);
                iter.next().map(|(key, _)| {
                    1 +
                        Cursor::new(key).read_u64::<BigEndian>().expect(
                            "parse u64 from rocksdb key",
                        )
                })
            }.unwrap_or(0);
            for v in strings {
                let mut key = Vec::new();
                key.write_u64::<BigEndian>(idx).expect(
                    "write u64 to big-endian bytes",
                );
                db.put(&key, v.as_bytes()).expect("writing data to rocksdb");
                idx += 1;
            }
        }
        Operation::Pop => {
            let item = {
                db.iterator(IteratorMode::Start).next()
            };
            for (key, value) in item {
                let v = String::from_utf8(value.into_vec()).expect(
                    "parse utf8 string from rocksdb value",
                );
                process(v);
                db.delete(key.as_ref()).expect("deleting data from rocksdb");
            }
        }
    };
}
