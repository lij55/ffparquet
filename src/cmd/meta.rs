use clap::Parser;
#[allow(unused_imports)]
use log::{debug, error, info, warn};
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::cmd::utils::*;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long)]
    group: Vec<i32>,

    file: String,

    #[arg(short, long, default_value_t = false)]
    schema: bool,

    #[arg(short, long)]
    column: Vec<i32>,
}

pub fn meta_main(args: Args) -> eyre::Result<()> {
    let file = args.file;
    let file = open_file(file)?;
    let parquet_reader = SerializedFileReader::new(file)?;

    let c = parquet_reader.num_row_groups();
    let file_meta = parquet_reader.metadata().file_metadata();

    println!("total rows: {}", file_meta.num_rows());
    println!("total columns: {}", file_meta.schema_descr().num_columns());
    println!("total groups: {}", c);

    if args.schema {
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
    }

    let col_sets = hashset(args.column.clone());
    let row_sets = hashset(args.group.clone());

    println!("row group information:");
    for i in 0..c {
        let reader = parquet_reader.get_row_group(i).unwrap();
        let rg_metadata = reader.metadata();
        match args.group.len() + args.column.len(){
            0 => {
                println!(
                    "\tRow group {i} has {} rows, {} bytes , {} columns, sorting columns is {:?}",
                    rg_metadata.num_rows(),
                    rg_metadata.compressed_size(),
                    rg_metadata.num_columns(),
                    rg_metadata.sorting_columns()
                );
            }
            _ => {
                if row_sets.is_empty() || row_sets.contains(&(i as i32)) {
                    //println!("\tDump Row group {i}");
                    println!(
                        "\tRow group {i} has {} rows, {} bytes , {} columns, sorting columns is {:?}",
                        rg_metadata.num_rows(),
                        rg_metadata.compressed_size(),
                        rg_metadata.num_columns(),
                        rg_metadata.sorting_columns()
                    );
                    for j in 0..rg_metadata.columns().len() {
                        if col_sets.is_empty() || col_sets.contains(&(j as i32)) {
                            println!(
                                "\t\tcolumn {j}: {} => {} by {}, encoding: {:?}, statics: {:?}",
                                rg_metadata.column(j).uncompressed_size(),
                                rg_metadata.column(j).compressed_size(),
                                rg_metadata.column(j).compression(),
                                rg_metadata.column(j).encodings(),
                                rg_metadata.column(j).statistics()
                            );
                        }

                    }
                }
            }
        }
    }

    Ok(())
}
