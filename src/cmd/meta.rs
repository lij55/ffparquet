use clap::Parser;
#[allow(unused_imports)]
use log::{debug, error, info, warn};
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::cmd::utils::*;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long)]
    group: Option<usize>,

    files: String,

    #[arg(short, long, default_value_t = false)]
    statics: bool,
}

pub fn meta_main(args: Args) -> eyre::Result<()> {
    let file = args.files;
    let file = open_file(file)?;
    let parquet_reader = SerializedFileReader::new(file)?;

    let c = parquet_reader.num_row_groups();
    let file_meta = parquet_reader.metadata().file_metadata();

    println!("total rows: {}", file_meta.num_rows());
    println!("total columns: {}", file_meta.schema_descr().num_columns());
    println!("total groups: {}", c);

    let columns = file_meta.schema_descr().columns();
    println!("column types:");
    for i in 0..columns.len() {
        println!(
            "\t{i} {}: {} {:?}",
            columns[i].name(),
            columns[i].converted_type(),
            columns[i].logical_type()
        );
    }

    println!("row group summaries");
    for i in 0..c {
        let reader = parquet_reader.get_row_group(i).unwrap();
        let rg_metadata = reader.metadata();
        println!(
            "\tRow group {i} has {} rows, {} bytes , {} columns, sorting columns is {:?}",
            rg_metadata.num_rows(),
            rg_metadata.total_byte_size() + rg_metadata.compressed_size(),
            rg_metadata.num_columns(),
            rg_metadata.sorting_columns()
        );
        // for i in 0..rg_metadata.columns().len() {
        //     println!(
        //         "\t\tcolumn {i}: {} => {} by {}, encoding: {:?}, statics: {:?}",
        //         rg_metadata.column(i).uncompressed_size(),
        //         rg_metadata.column(i).compressed_size(),
        //         rg_metadata.column(i).compression(),
        //         rg_metadata.column(i).encodings(),
        //         rg_metadata.column(i).statistics()
        //     );
        // }
    }

    Ok(())
}
