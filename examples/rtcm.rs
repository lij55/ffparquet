use std::fs::File;
use std::path::Path;

use arrow_schema::SchemaRef;
use clap::Parser;
use eyre::Result;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

#[derive(Parser, Debug)]
pub struct Args {
    /// Intput file
    input: String,
    /// Output file
    output: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    // read csv from file
    let file = open_file(args.input.as_str())?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    println!("schema: {:?}", schema);
    let mut reader = builder.build()?;

    let mut w = build_parquet_file_writer2(args.output.as_str(), schema).unwrap();
    loop {
        match reader.next() {
            Some(r) => match r {
                Ok(r) => {
                    w.write(&r)?;
                }
                Err(e) => {
                    println!("{e:?}");
                }
            },
            None => {
                // done
                break;
            }
        }
    }
    w.flush()?;
    w.close()?;
    Ok(())
}

fn build_parquet_file_writer2(path_str: &str, schema: SchemaRef) -> Option<ArrowWriter<File>> {
    let file = File::create(path_str).ok()?;
    let props = WriterProperties::builder()
        .set_max_row_group_size(1000000)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_created_by("op".into())
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_encoding(Encoding::DELTA_BINARY_PACKED)
        .set_write_batch_size(16 * 1024 * 1024)
        .set_dictionary_enabled(false)
        .build();
    let writer = ArrowWriter::try_new(file, schema, Some(props)).ok()?;
    Some(writer)
}


fn open_file<P: AsRef<Path>>(file_name: P) -> std::io::Result<File> {
    let file_name = file_name.as_ref();
    let path = Path::new(file_name);
    File::open(path)
}
