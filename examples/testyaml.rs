use std::collections::HashMap;
use std::fs;

use serde::{Deserialize, Serialize};

fn main() {
    let contents =
        fs::read_to_string("examples/df.yaml").expect("Should have been able to read the file");

    let rule_file: DFConfig = match serde_yaml::from_str::<DFConfig>(&contents) {
        Ok(v) => v,
        Err(e) => {
            println!("{e}");
            return;
        }
    };

    println!("{:#?}", rule_file)
}

#[derive(Debug, Serialize, Deserialize)]
struct DFConfig {
    source: Vec<Source>,
    sink: Sink,
    query: Vec<Query>,
}
#[derive(Debug, Serialize, Deserialize)]
struct Source {
    name: String,
    format: String,
    path: Vec<String>,
}
#[derive(Debug, Serialize, Deserialize)]
struct Sink {
    name: String,
    format: String,
    path: String,
    parameters: Vec<HashMap<String, String>>,
    columns: Vec<Column>,
}
#[derive(Debug, Serialize, Deserialize)]
struct Query {
    name: String,
    sql: String,
}
#[derive(Debug, Serialize, Deserialize)]
struct Column {
    name: String,
    compression: Option<String>,
    encoding: Option<String>,
    sort: Option<String>,
}
