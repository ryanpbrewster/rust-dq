extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use structopt::StructOpt;

extern crate durable_queue;
use durable_queue::DurableQueue;
use durable_queue::rocks_dq::RocksDQ;

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

    let mut dq = RocksDQ::new(cmd.db_path);

    match cmd.op {
        Operation::List => {
            dq.list();
        }
        Operation::Push { strings } => {
            for v in strings {
                dq.push(v);
            }
        }
        Operation::Pop => {
            for (k, v) in dq.peek() {
                process(v);
                dq.delete(k)
            }
        }
    };
}
