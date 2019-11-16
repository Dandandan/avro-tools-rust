use avro_rs::Reader;
use std::fs::File;
use std::io::BufReader;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "Avro Tools", author = "Daniël Heres", about = "An example of StructOpt usage.")]
struct Opt {
    #[structopt(name = "command")]
    command: String,

    #[structopt(name = "FILE")]
    file: String,

}

fn get_schema(file_name: &str) {
    let file = File::open(&file_name);

    match file {
        Ok(f) => {
            let buffered_reader = BufReader::new(f);

            let r = Reader::new(buffered_reader);
            for x in r {
                println!("{}", x.writer_schema().canonical_form());
            }
        }
        _ => println!("File {} not found", file_name)
    }
}

fn main() {
    let opt = Opt::from_args();

    match opt.command.as_ref() {
        "getschema" => get_schema(&opt.file),
        _ => println!("command nut supported")
    }
}
