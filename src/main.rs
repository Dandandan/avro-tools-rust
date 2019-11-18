use avro_rs::Reader;
use avro_rs::types::Value;
use std::fs::File;
use std::io::BufReader;
use structopt::StructOpt;
use serde_json;

#[derive(Debug, StructOpt)]
#[structopt(name = "Avro Tools", author = "Daniël Heres", about = "An example of StructOpt usage.")]
struct Opt {
    #[structopt(name = "command")]
    command: String,

    #[structopt(name = "FILE")]
    file: String,

}

fn avro_to_json(x: Value) -> serde_json::Value {
    match x {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(b),
        Value::Long(n) => serde_json::json!(n),
        Value::Double(n) => serde_json::json!(n),
        Value::Int(n) => serde_json::json!(n),
        Value::Float(n) => serde_json::json!(n),
        Value::Fixed(n, items) => serde_json::json!(vec![serde_json::json!(n), serde_json::Value::Array(items.into_iter().map(|item| serde_json::json!(item)).collect())]),

        Value::Bytes(items) => serde_json::Value::Array(items.into_iter().map(|item| serde_json::json!(item)).collect()),
        Value::String(s) => serde_json::Value::String(s),
        Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(|item| avro_to_json(item)).collect())
        }
        Value::Map(items) => serde_json::Value::Object(
            items
                .into_iter()
                .map(|(key, value)| (key, avro_to_json(value)))
                .collect::<_>(),
        ),
        Value::Record(items) => serde_json::Value::Object(
            items
                .into_iter()
                .map(|(key, value)| (key, avro_to_json(value)))
                .collect::<_>(),
        ),
        Value::Union(v) => avro_to_json(*v),
        Value::Enum(_v, s) => serde_json::json!(s),
    }
}


fn get_schema(file: File) {
    let buffered_reader = BufReader::new(file);

    let r = Reader::new(buffered_reader);
    for x in r {
        let json: serde_json::Value = serde_json::from_str(&x.writer_schema().canonical_form()).expect("");
        let pretty = serde_json::to_string_pretty(&json).expect("");
        print!("{}", pretty);
    }
}

fn tojson(file: File) {
    let buffered_reader = BufReader::new(file);

    let r = Reader::new(buffered_reader);
    for x in r.unwrap() {
        let json = avro_to_json(x.unwrap());

        print!("{}", json);
    }
}

fn parse_command(file: File, command: &str) {
    match command.as_ref() {
        "getschema" => get_schema(file),
        "tojson" => tojson(file),
        _ => println!("Command `{}` not supported", command)
    }
}

fn main() {
    let opt = Opt::from_args();

    let file = File::open(&opt.file);

    match file {
        Ok(f) => parse_command(f, &opt.command),
        _ => println!("File `{}` not found", opt.file)
    }
}
