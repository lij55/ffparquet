use arrow::datatypes::SchemaRef;
use arrow_array::{RecordBatch, RecordBatchReader};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::arrow::parquet_to_arrow_schema;
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

use std::ops::{Deref, Index};
use std::{sync::mpsc, sync::Arc, sync::Mutex, thread};

use env_logger;
use log::{debug, error, info, warn};
use std::env;
const ERR_LOCK: &str = "cant acquire resource";

fn build_parquet_file_writer2(path_str: &str, schema: SchemaRef, max_rows_per_group: usize) -> Option<ArrowWriter<File>> {
    let file = File::create(path_str).ok()?;
    let props = WriterProperties::builder()
        .set_max_row_group_size(max_rows_per_group)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_created_by("ffparquet".into())
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();
    let writer = ArrowWriter::try_new(file, schema, Some(props)).ok()?;
    Some(writer)
}


#[derive(clap::ValueEnum, PartialEq, Default, Clone, Debug)]
enum CompressionType {
    Uncompressed,
    #[default]
    Zstd,
    Snapy,
    LZ4,
    Gzip,
}

impl Display for CompressionType {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::result::Result<(), ::std::fmt::Error> {
        match *self {
            CompressionType::Uncompressed => write!(f, "uncompressed"),
            CompressionType::Zstd => write!(f, "zstd"),
            CompressionType::Snapy => write!(f, "snapy"),
            CompressionType::Gzip => write!(f, "gzip"),
            CompressionType::LZ4 => write!(f, "lz4"),
        }
    }
}

#[derive(Parser)]
struct Args {
    /// Max row groups to create
    #[arg(short = 'g', long = "groups", default_value_t = 0)]
    max_row_group_count: u16,

    /// Size of each row group
    #[arg(short = 's', long = "size", default_value_t = 1000000)]
    max_row_group_size: u32,

    /// jobs in parallel
    #[arg(short = 'j', long = "jobs", default_value_t = 4)]
    jobs: u8,

    /// output directoy, default is PWD
    #[arg(short = 'd', long = "dir")]
    output_dir: Option<String>,

    /// prefix
    prefix: Option<String>,

    /// compression method
    #[arg(short='c', long="compression", default_value_t = CompressionType::Zstd)]
    compression: CompressionType,

    /// compression level
    #[arg(short='l', long="level")]
    level: Option<u8>,

    /// input parquet file path
    #[arg(short = 'i', long = "input")]
    file: String,
}

enum Message {
    DONE,
    CONTENT(Box<RecordBatch>),
}

fn main() {
    env_logger::init(); // controlled by env var RUST_LOG
    let args = Args::parse();

    // no output, just dump information
    if args.prefix.is_none() {
        dump_parquet_info(args.file.as_str());
        return;
    } else {
        summary_info(args.file.clone().as_str())
    }

    if args.max_row_group_size == 0 {
        error!("please set a value bigger than 0 for max_row_group_size");
        return
    }

    let batch_size = 10000;

    let target_prefix = args.prefix.unwrap();

    let file = File::open(args.file.clone()).unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let total_rows = builder.metadata().file_metadata().num_rows() as u64;
    let schema_desc = builder.schema().clone();

    let mut reader = builder.with_batch_size(batch_size).build().unwrap();


    let row_limit_per_file: u64 =
        (args.max_row_group_count as u32 * args.max_row_group_size) as u64;
    let total_tasks: u64 = match row_limit_per_file {
        0 => {
            if total_rows < batch_size as u64 {
                1
            } else {
                args.jobs as u64
            }
        }
        _ => total_rows / row_limit_per_file + 1,
    };

    //let schema_desc = builder.schema().clone();
    let (tx, rx) = mpsc::sync_channel::<Message>(16);

    let (thread_tx, thread_rx) = mpsc::channel::<i8>();

    for _i in 0..args.jobs {
        let _ = thread_tx.send(0);
    }

    let mut threads = vec![];

    let rxtf = Arc::new(Mutex::new(rx));

    thread::spawn(move || {
        while let Some(record_batch) = reader.next() {
            if record_batch.is_err() {
                break;
            }

            let data = Box::new(record_batch.unwrap());
            tx.send(Message::CONTENT(data)).unwrap();
            // w1.write(&data).expect("Writing batch");
            // w2.write(&data).expect("Writing batch");
        }
        for _i in 0..total_tasks {
            tx.send(Message::DONE).unwrap();
        }

        //println!("send data done");
    });

    println!("output files: {}_*.parquet", args.file.clone());
    println!("\tmax row group size is {}", args.max_row_group_size);
    println!("\ttotal {total_tasks} files will be created");

    for i in 0..total_tasks {
        let _ = thread_rx.recv();
        let rx = rxtf.clone();
        let schema = schema_desc.clone();
        let local_thread_tx = thread_tx.clone();
        let outfile_name = format!("{target_prefix}_{i}.parquet");

        let t = thread::spawn(move || {
            let mut w1 = build_parquet_file_writer2(outfile_name.as_str(), schema, args.max_row_group_size as usize).unwrap();

            loop {
                let msg = rx
                    .lock()
                    .expect(ERR_LOCK)
                    .recv_timeout(Duration::from_secs(10));

                match msg {
                    Ok(msg_data) => match msg_data {
                        Message::CONTENT(data) => {
                            //println!("get data from {i}");
                            let _ = w1.write(data.as_ref());
                        }
                        Message::DONE => {
                            info!("thread for part {i} done");
                            break;
                        }
                    },
                    Err(_) => break,
                }
            }
            let _ = w1.flush();

            // writer must be closed to write footer
            w1.close().unwrap();
            let _ = local_thread_tx.send(0);
        });
        threads.push(t);
    }

    while let Some(cur_thread) = threads.pop() {
        cur_thread.join().unwrap();
    }
}

fn dump_parquet_info(file: &str) {
    let file = File::open(file).unwrap();
    let parquet_reader = SerializedFileReader::new(file).unwrap();
    let c = parquet_reader.num_row_groups();
    let file_meta = parquet_reader.metadata().file_metadata();

    println!(
        "total number of columns: {}",
        file_meta.schema_descr().num_columns()
    );

    let columns = file_meta.schema_descr().columns();
    for i in 0..columns.len() {
        println!(
            "\t{}: {:?}",
            columns[i].name(),
            //columns[i].converted_type()
            columns[i].logical_type()
        );
    }

    println!("number of row groups: {}", c);
    for i in 0..c {
        let reader = parquet_reader.get_row_group(i).unwrap();
        let rg_metadata = reader.metadata();
        println!("\tRow group {i} has {} rows", rg_metadata.num_rows());
        for i in 0..rg_metadata.columns().len() {
            println!(
                "\t\tcolumn {i}: {} => {} by {}, encoding: {:?}, statics: {:?}",
                rg_metadata.column(i).uncompressed_size(),
                rg_metadata.column(i).compressed_size(),
                rg_metadata.column(i).compression(),
                rg_metadata.column(i).encodings(),
                rg_metadata.column(i).statistics()
            );
        }
    }

    println!("total number of rows: {}", file_meta.num_rows());

    //
}

fn summary_info(src: &str) {
    let file = File::open(src).unwrap();
    let parquet_reader = SerializedFileReader::new(file).unwrap();
    let c = parquet_reader.num_row_groups();
    let file_meta = parquet_reader.metadata().file_metadata();

    println!(
        "source file:\t{src}"
    );
    println!(
        "\ttotal number of columns: {}",
        file_meta.schema_descr().num_columns()
    );

    println!("\tnumber of row groups: {}", c);


    println!("\ttotal number of rows: {}", file_meta.num_rows());

}