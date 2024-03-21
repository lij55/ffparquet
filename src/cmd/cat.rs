use clap::Parser;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde_json::{Map, Value};

use crate::cmd::utils::*;

#[derive(clap::ValueEnum, Clone, Debug)]
enum OutputFormat {
    CSV,
    JSON,
}

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long)]
    group: Option<usize>,

    #[arg(short, long, default_value_t = 0)]
    limit: u64,

    #[arg(short, long, default_value = "csv")]
    output: OutputFormat,

    #[arg(long, default_value_t = 0)]
    offset: u64,

    #[arg(short, long)]
    column: Vec<i32>,

    file: String,
}

pub fn cat_main(mut args: Args) -> eyre::Result<()> {
    let file = open_file(args.file)?;
    let reader = SerializedFileReader::new(file)?;

    let rg = reader.get_row_group(args.group.unwrap_or(0))?;

    let col_sets = hashset(args.column);

    if args.offset > 0 {
        args.limit += args.offset;
    }

    for (idx, i) in rg.get_row_iter(None)?.enumerate() {
        if (args.offset > 0) && idx < args.offset as usize {
            continue;
        }
        if (args.limit > 0) && (idx >= args.limit as usize) {
            break;
        }

        let row = i?;
        let row = row
            .get_column_iter()
            .enumerate()
            .filter_map(|(idx, x)| {
                if col_sets.is_empty() || col_sets.contains(&(idx as i32)) {
                    Some(x)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        match &args.output {
            OutputFormat::CSV => {
                let output = row
                    .into_iter()
                    .map(|x| format!("{}", x.1))
                    .collect::<Vec<_>>();

                println!("{}", output.join(";"));
            }
            OutputFormat::JSON => {
                let r = Value::Object(
                    row.into_iter()
                        .map(|(key, field)| (key.to_owned(), field.to_json_value()))
                        .collect::<Map<String, Value>>(),
                );
                println!("{}", r);
            }
        }
    }
    Ok(())
}
