use arrow::datatypes::SchemaRef;
use arrow_array::{RecordBatch, RecordBatchReader};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::format::SortingColumn;
use parquet::schema::types::{SchemaDescriptor, Type};
use std::fs::{read, File};
use std::io;
use std::path::Path;
use std::time::Duration;

use clap::Parser;
use std::fmt::Display;

use std::cmp::min;
use std::ops::{Deref, Index};
use std::{sync::mpsc, sync::Arc, sync::Mutex, thread};

use env_logger;
use eyre::{Result, WrapErr};
use log::{debug, error, info, warn};
use std::env;

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
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.with_batch_size(10240).build()?;

    // for i in reader.into_iter() {
    //     println!("{}", i?);
    // }

    let writer_builder = arrow::csv::WriterBuilder::new().has_headers(false);
    let mut writer = writer_builder.build(std::io::stdout());

    let mut left = Some(10000);

    for maybe_batch in reader {
        if left == Some(0) {
            break;
        }

        let mut batch = maybe_batch?;
        if let Some(l) = left {
            if batch.num_rows() <= l {
                left = Some(l - batch.num_rows());
            } else {
                let n = min(batch.num_rows(), l);
                batch = batch.slice(0, n);
                left = Some(0);
            }
        };

        writer.write(&batch)?;
    }
    Ok(())
}
