extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "durable_queue", about = "Interact with a durable queue.")]
enum Command {
    #[structopt(name = "list")]
    List,
    #[structopt(name = "push")]
    Push { strings: Vec<String> },
    #[structopt(name = "pop")]
    Pop {
        #[structopt(long = "dry-run")]
        dry_run: bool,
    },
}

fn main() {
    let cmd = Command::from_args();


    match cmd {
        Command::List => println!("coming soon!"),
        _ => unimplemented!(),
    };
}
