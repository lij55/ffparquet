use std::fmt::Display;
use std::fs::File;
use std::path::Path;

use arrow_array::RecordBatchReader;
use clap::Parser;
use env_logger;
use eyre::Result;
use parquet::file::reader::{FileReader, SerializedFileReader};

#[derive(Parser)]
struct Args {
    /// input parquet file path
    file: String,
}

fn open_file<P: AsRef<Path>>(file_name: P) -> std::io::Result<File> {
    let file_name = file_name.as_ref();
    let path = Path::new(file_name);
    File::open(path)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let file = open_file(args.file)?;
    let reader = SerializedFileReader::new(file)?;

    let rg = reader.get_row_group(0)?;
    for i in rg.get_row_iter(None)? {
        // for (idx, (name, field)) in i?.get_column_iter().enumerate() {
        //     println!(
        //         "column index: {}, column name: {}, column value: {}",
        //         idx, name, field
        //     );
        // }

        let output = i?
            .get_column_iter()
            .map(|x| format!("{}", x.1))
            .collect::<Vec<_>>();

        println!("{}", output.join(";"));
    }

    Ok(())
}
